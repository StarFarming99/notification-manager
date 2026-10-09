package dispatcher

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/kubesphere/notification-manager/pkg/controller"
	"github.com/kubesphere/notification-manager/pkg/deliveryprofiles"
)

func TestProfileSharedBudgetBoundsConcurrentWorkersAndRetries(t *testing.T) {
	d := &Dispatcher{notifierCtl: &controller.Controller{DeliveryProfiles: &deliveryprofiles.Manager{SendInterval: 30 * time.Millisecond}}}
	var mu sync.Mutex
	var sent []time.Time
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := d.waitSendBudget(context.Background()); err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			sent = append(sent, time.Now())
			mu.Unlock()
		}()
	}
	wg.Wait()
	sort.Slice(sent, func(i, j int) bool { return sent[i].Before(sent[j]) })
	if len(sent) != 6 {
		t.Fatal(sent)
	}
	for i := 1; i < len(sent); i++ {
		if sent[i].Sub(sent[i-1]) < 25*time.Millisecond {
			t.Fatal("parallel workers bypassed shared budget", sent)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := d.waitSendBudget(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("pre-send cancellation ignored", err)
	}
	start := time.Now()
	if err := d.waitSendBudget(context.Background()); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("cancelled reservation accumulated future delay")
	}
}

func TestLegacyHasNoAdditionalSendBudget(t *testing.T) {
	d := &Dispatcher{notifierCtl: &controller.Controller{}, nextSend: time.Now().Add(time.Hour)}
	if err := d.waitSendBudget(context.Background()); err != nil {
		t.Fatal(err)
	}
}
