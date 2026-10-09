package dispatcher

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/go-kit/kit/log/level"
	spool "github.com/kubesphere/notification-manager/pkg/notification_spool"
	"github.com/kubesphere/notification-manager/pkg/notify"
	"github.com/kubesphere/notification-manager/pkg/utils"
)

func (d *Dispatcher) runDurable() error {
	var wg sync.WaitGroup
	maintenanceCtx, stopMaintenance := context.WithCancel(context.Background())
	defer stopMaintenance()
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-maintenanceCtx.Done():
				return
			case now := <-ticker.C:
				if _, err := d.alerts.Durable.CleanupCompleted(48*time.Hour, 200, now); err != nil {
					_ = level.Error(d.l).Log("msg", "Completed notification maintenance failed")
				}
			}
		}
	}()
	workers := cap(d.semCh)
	if workers < 1 {
		workers = 1
	}
	if workers > 32 {
		workers = 32
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); d.durableWorker() }()
	}
	wg.Wait()
	return nil
}

func (d *Dispatcher) durableWorker() {
	for {
		closing, mode, deadline := d.alerts.ShutdownState()
		if closing && (mode == "handoff" || !time.Now().Before(deadline)) {
			return
		}
		claim, err := d.alerts.Durable.Claim(time.Now().UTC())
		if errors.Is(err, spool.ErrEmpty) {
			if closing {
				status, statusErr := d.alerts.Durable.Status()
				if statusErr == nil {
					counts := status["counts"].(map[string]int)
					if counts[spool.Pending]+counts[spool.Retryable]+counts[spool.Sending] == 0 {
						return
					}
				}
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if err != nil {
			_ = level.Error(d.l).Log("msg", "Original notification claim failed")
			time.Sleep(time.Second)
			continue
		}
		d.sendClaim(claim)
	}
}

func (d *Dispatcher) sendClaim(claim spool.Claim) {
	timeout := d.wkrTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	err := d.waitSendBudget(ctx)
	if err != nil {
		err = &utils.PreSendError{Err: err}
	} else {
		err = notify.SendFrozen(ctx, d.l, d.notifierCtl, claim.Target)
	}
	state, outcome := utils.DeliveryOutcome(err)
	evidence := "attempt:" + claim.AttemptID
	if err := d.alerts.Durable.Finish(claim, state, outcome, evidence, time.Now().Add(5*time.Second)); err != nil {
		// A successful external send without a durable Ack must never be retried
		// here. The persisted sending record is reconciled as unknown on restart.
		_ = level.Error(d.l).Log("msg", "Original notification completion could not be persisted", "target_id", claim.Target.ID, "attempt_id", claim.AttemptID)
		return
	}
	_ = level.Info(d.l).Log("msg", "Original notification target completed", "target_id", claim.Target.ID, "attempt_id", claim.AttemptID, "state", state)
}

// One reservation clock bounds automatic sends across all workers and retries.
// The legacy memory pipeline retains its existing notification behavior.
func (d *Dispatcher) waitSendBudget(ctx context.Context) error {
	profiles := d.notifierCtl.DeliveryProfiles
	if profiles == nil || profiles.SendInterval <= 0 {
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		d.sendBudgetMu.Lock()
		wait := time.Until(d.nextSend)
		if wait <= 0 {
			d.nextSend = time.Now().Add(profiles.SendInterval)
			d.sendBudgetMu.Unlock()
			return nil
		}
		d.sendBudgetMu.Unlock()
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
