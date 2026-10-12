package notifier

import (
	"context"
	"github.com/go-kit/kit/log"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTokenDeadlineNoDetachedWriterAndConcurrentInvalidation(t *testing.T) {
	cache := &AccessTokenService{tokens: make(map[string]*token)}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	finished := make(chan struct{})
	_, err := cache.GetToken(ctx, "synthetic", func(ctx context.Context) (string, time.Duration, error) {
		defer close(finished)
		<-ctx.Done()
		return "", 0, ctx.Err()
	})
	if err == nil {
		t.Fatal("deadline not preserved")
	}
	select {
	case <-finished:
	default:
		t.Fatal("detached token acquisition")
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := cache.GetToken(context.Background(), "synthetic", func(context.Context) (string, time.Duration, error) { return "synthetic-token", time.Hour, nil })
			if err != nil {
				t.Error(err)
			}
			cache.InvalidToken(context.Background(), "synthetic", log.NewNopLogger())
		}()
	}
	wg.Wait()
}

func TestExpiredContextCannotPopulateTokenCache(t *testing.T) {
	cache := &AccessTokenService{tokens: make(map[string]*token)}
	ctx, cancel := context.WithCancel(context.Background())
	_, err := cache.GetToken(ctx, "synthetic", func(context.Context) (string, time.Duration, error) { cancel(); return "expired-token", time.Hour, nil })
	if err == nil || len(cache.tokens) != 0 {
		t.Fatal("expired acquisition populated cache", err)
	}
}

func TestTokenConcurrentAcquisitionUsesOnePreflightRequest(t *testing.T) {
	cache := &AccessTokenService{tokens: make(map[string]*token)}
	var calls atomic.Int32
	acquire := func(ctx context.Context) (string, time.Duration, error) {
		calls.Add(1)
		select {
		case <-ctx.Done():
			return "", 0, ctx.Err()
		case <-time.After(20 * time.Millisecond):
			return "synthetic-token", time.Hour, nil
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			value, err := cache.GetToken(context.Background(), "synthetic", acquire)
			if err != nil || value != "synthetic-token" {
				t.Error("token fetch failed", err)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatal("platform token preflight amplified", calls.Load())
	}
}

func TestTokenWaitingCallerRetainsOwnDeadline(t *testing.T) {
	cache := &AccessTokenService{tokens: make(map[string]*token)}
	started, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(finished)
		_, _ = cache.GetToken(context.Background(), "synthetic", func(context.Context) (string, time.Duration, error) {
			close(started)
			<-release
			return "synthetic-token", time.Hour, nil
		})
	}()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := cache.GetToken(ctx, "synthetic", func(context.Context) (string, time.Duration, error) {
		t.Error("second token request")
		return "", 0, nil
	})
	close(release)
	<-finished
	if err == nil {
		t.Fatal("waiting token acquisition ignored its deadline")
	}
}
