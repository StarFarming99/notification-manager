package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/kubesphere/notification-manager/pkg/jevshadow"
	"github.com/kubesphere/notification-manager/pkg/notify/notifier/feishu"
)

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
	logger := log.With(log.NewJSONLogger(os.Stdout), "ts", log.DefaultTimestampUTC)
	config, err := jevshadow.ConfigFromEnv()
	if err != nil {
		_ = level.Error(logger).Log("msg", "invalid executor configuration")
		return err
	}
	release, err := jevshadow.AcquireExecutorLease(config)
	if err != nil {
		return err
	}
	defer release()
	service, err := jevshadow.New(logger, config, nil)
	if err != nil {
		return err
	}
	defer service.Close()
	if !service.Enabled() {
		return errors.New("executor durable stores are unavailable or disabled")
	}
	appID, appSecret := os.Getenv("JEV_FEISHU_APP_ID"), os.Getenv("JEV_FEISHU_APP_SECRET")
	if appID != config.SenderApp {
		return errors.New("executor app differs from registered owner")
	}
	patcher, err := feishu.StaticCardPatcher(logger, appID, appSecret)
	if err != nil {
		return err
	}
	service.SetCardPatcherResolver(func(_ context.Context, receiver, destination string) (jevshadow.CardPatcher, error) {
		if !service.ShouldCapture(receiver, destination) {
			return nil, jevshadow.ErrCardNotFound
		}
		return patcher, nil
	})
	relayToken := os.Getenv("JEV_EXECUTOR_TOKEN")
	if !service.IndependentExecutorCredential(relayToken) {
		return errors.New("executor relay credential must be independent")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/jev/successful-deliveries", service.HandleSuccessfulDelivery(relayToken))
	if service.FeedbackEnabled() {
		callbackToken := os.Getenv("JEV_CALLBACK_RELAY_TOKEN")
		if callbackToken == relayToken || !service.IndependentExecutorCredential(callbackToken) {
			return errors.New("existing callback relay requires an independent credential")
		}
		mux.HandleFunc("/internal/jev/card-feedback", service.HandleCardFeedback(callbackToken))
	}
	mux.HandleFunc("/internal/jev/annotations/", func(w http.ResponseWriter, r *http.Request) {
		// Legacy original card callbacks can write the same message. Until their
		// writer coordination is verified, leave production annotation closed.
		if os.Getenv("JEV_CARD_STATE_COORDINATED") != "true" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if r.Method != http.MethodPut {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		service.HandleAnnotation(w, r, strings.TrimPrefix(r.URL.Path, "/internal/jev/annotations/"))
	})
	mux.HandleFunc("/health/ready", func(w http.ResponseWriter, r *http.Request) {
		if !service.Enabled() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/health/live", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	slots := make(chan struct{}, 8)
	bounded := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
			mux.ServeHTTP(w, r)
		default:
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
		}
	})
	server := &http.Server{Addr: ":19095", Handler: bounded, ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 * 1024}
	if service.FeedbackEnabled() && config.Environment != "production" && config.Environment != "prod" {
		go func() {
			if err := feishu.StartJevFeedbackListener(ctx, logger, service, appID, appSecret); err != nil {
				// Feedback failure affects only the executor, never NM readiness.
				_ = level.Error(logger).Log("msg", "executor feedback listener exited")
			}
		}()
	}
	go func() {
		<-ctx.Done()
		stop, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		_ = server.Shutdown(stop)
	}()
	err = server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
