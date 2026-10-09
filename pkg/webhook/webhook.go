package webhook

import (
	"context"
	"net/http"
	"time"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/kubesphere/notification-manager/pkg/controller"
	"github.com/kubesphere/notification-manager/pkg/store"
	v1 "github.com/kubesphere/notification-manager/pkg/webhook/v1"
)

type Options struct {
	ListenAddress             string
	WebhookTimeout            time.Duration
	WorkerTimeout             time.Duration
	FormalCompatListenAddress string
	FormalCompatSourceCIDRs   []string
}

type Webhook struct {
	router       chi.Router
	compatRouter chi.Router
	compatError  error
	*Options
	logger  log.Logger
	handler *v1.HttpHandler
}

func New(logger log.Logger, notifierCtl *controller.Controller, alerts *store.AlertStore, o *Options) *Webhook {

	h := &Webhook{
		Options: o,
		logger:  logger,
	}

	h.handler = v1.New(logger, h.WorkerTimeout, notifierCtl, alerts)
	h.router = chi.NewRouter()

	h.router.Use(middleware.RequestID)
	// h.router.Use(middleware.Logger)
	h.router.Use(middleware.Recoverer)
	h.router.Use(middleware.Timeout(2 * h.WebhookTimeout))
	h.router.Get("/receivers", h.handler.ListReceivers)
	h.router.Get("/configs", h.handler.ListConfigs)
	h.router.Get("/receiverWithConfig", h.handler.ListReceiverWithConfig)
	formalAlert, formalVerify, formalNotification := h.handler.Alert, h.handler.Verify, h.handler.Notification
	if profiles := notifierCtl.DeliveryProfiles; profiles != nil {
		formalAlert = profiles.Guard("formal", formalAlert)
		formalVerify = profiles.Guard("formal", formalVerify)
		formalNotification = profiles.Guard("formal", formalNotification)
		h.router.Post("/api/v2/test/alerts", profiles.Guard("test", h.handler.TestAlert))
		deny := func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "test lane only admits scoped Alertmanager alerts", http.StatusForbidden)
		}
		h.router.Post("/api/v2/test/verify", profiles.Guard("test", deny))
		h.router.Post("/api/v2/test/notifications", profiles.Guard("test", deny))
		h.router.Get("/internal/delivery-profiles", profiles.ControlGuard(h.handler.ProfileStatus))
		h.router.Post("/internal/delivery-profiles/{profile}/{action}", profiles.ControlGuard(func(w http.ResponseWriter, r *http.Request) {
			h.handler.ProfileControl(w, r, chi.URLParam(r, "profile"), chi.URLParam(r, "action"))
		}))
	}
	h.router.Post("/api/v2/alerts", formalAlert)
	if shadow := notifierCtl.GetJevShadow(); shadow != nil && shadow.Enabled() && !shadow.IsRelay() {
		h.router.Put("/internal/jev/annotations/{messageID}", func(w http.ResponseWriter, r *http.Request) {
			shadow.HandleAnnotation(w, r, chi.URLParam(r, "messageID"))
		})
	}
	h.router.Post("/api/v2/verify", formalVerify)
	h.router.Post("/api/v2/notifications", formalNotification)
	h.router.Get("/metrics", h.handler.ServeMetrics)
	h.router.Get("/-/reload", h.handler.ServeReload)
	h.router.Get("/-/ready", h.handler.ServeReadinessCheck)
	h.router.Get("/-/formal-ready", func(w http.ResponseWriter, r *http.Request) {
		if h.FormalCompatListenAddress == "" || h.compatError != nil || h.compatRouter == nil {
			http.Error(w, "formal compatibility disabled", 503)
			return
		}
		h.handler.ServeFormalReadiness(w, r)
	})
	h.router.Get("/-/live", h.handler.ServeHealthCheck)
	h.router.Get("/status", h.handler.ServeStatus)

	if h.FormalCompatListenAddress != "" {
		h.compatRouter, h.compatError = h.compatibilityRouter(notifierCtl.DeliveryProfiles)
	}
	return h
}

func (h *Webhook) PrepareInitialProfiles(ctx context.Context) error {
	if h.compatError != nil {
		return h.compatError
	}
	return h.handler.PrepareInitialProfiles(ctx)
}

func (h *Webhook) Run(ctx context.Context) error {
	if h.compatError != nil {
		return h.compatError
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	count := 1
	results := make(chan error, 2)
	go func() { results <- h.runServer(ctx, h.ListenAddress, h.router) }()
	if h.compatRouter != nil {
		count++
		go func() { results <- h.runServer(ctx, h.FormalCompatListenAddress, h.compatRouter) }()
	}
	var result error
	for i := 0; i < count; i++ {
		err := <-results
		if err != nil && result == nil {
			result = err
		}
		cancel()
	}
	return result
}

func (h *Webhook) runServer(ctx context.Context, address string, handler http.Handler) error {
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	var err error
	httpSrv := &http.Server{
		Addr:              address,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      35 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	srvClosed := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			// Shutdown needs a fresh bounded context; ctx is already cancelled.
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := httpSrv.Shutdown(shutdownCtx); err != nil {
				// Error from closing listeners, or context timeout:
				_ = level.Error(h.logger).Log("msg", "Shutdown HTTP server", "err", err)
			}
			_ = level.Info(h.logger).Log("msg", "Shutdown HTTP server")
			close(srvClosed)
		}
	}()

	if err = httpSrv.ListenAndServe(); err != http.ErrServerClosed {
		// Error starting or closing listener:
		_ = level.Error(h.logger).Log("msg", "HTTP server ListenAndServe", "err", err)
	}

	_ = level.Error(h.logger).Log("msg", "HTTP server exit", "err", err)
	stop()
	<-srvClosed
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}
