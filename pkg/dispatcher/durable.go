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
	if profiles := d.notifierCtl.DeliveryProfiles; profiles != nil {
		// Independent workers never claim another lane's historical backlog.
		// Keep original throughput by default; one bounded worker serves test.
		formalWorkers := profiles.FormalWorkers
		for _, lane := range []struct {
			id      string
			workers int
		}{{"formal", formalWorkers}, {"test", profiles.TestWorkers}} {
			for i := 0; i < lane.workers; i++ {
				id := lane.id
				wg.Add(1)
				go func() { defer wg.Done(); d.durableWorkerLane(id) }()
			}
		}
	} else {
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); d.durableWorker() }()
		}
	}
	wg.Wait()
	return nil
}

func (d *Dispatcher) durableWorker() { d.durableWorkerLane("*") }
func (d *Dispatcher) durableWorkerLane(lane string) {
	for {
		closing, mode, deadline := d.alerts.ShutdownState()
		if closing && (mode == "handoff" || !time.Now().Before(deadline)) {
			return
		}
		var claim spool.Claim
		var err error
		if lane == "*" {
			claim, err = d.alerts.Durable.Claim(time.Now().UTC())
		} else {
			claim, err = d.alerts.Durable.ClaimProfile(lane, time.Now().UTC())
		}
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
	interval := time.Duration(0)
	if profiles := d.notifierCtl.DeliveryProfiles; profiles != nil {
		interval = profiles.IntervalFor(claim.Target.ProfileID)
	}
	started := false
	err := d.alerts.Durable.WithSendPermit(claim, interval, time.Now().UTC(), func() error {
		started = true
		return notify.SendFrozen(ctx, d.l, d.notifierCtl, claim.Target)
	})
	next := time.Now().Add(5 * time.Second)
	var deferred *spool.SendBudgetDeferred
	if errors.As(err, &deferred) {
		next = deferred.AvailableAt
	}
	if err != nil && !started {
		err = &utils.PreSendError{Err: err}
	}
	state, outcome := utils.DeliveryOutcome(err)
	evidence := "attempt:" + claim.AttemptID
	if err := d.alerts.Durable.Finish(claim, state, outcome, evidence, next); err != nil {
		// A successful external send without a durable Ack must never be retried
		// here. The persisted sending record is reconciled as unknown on restart.
		_ = level.Error(d.l).Log("msg", "Original notification completion could not be persisted", "target_id", claim.Target.ID, "attempt_id", claim.AttemptID)
		return
	}
	_ = level.Info(d.l).Log("msg", "Original notification target completed", "target_id", claim.Target.ID, "attempt_id", claim.AttemptID, "state", state)
}
