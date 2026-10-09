package webhook

import (
	"errors"
	"net"
	"net/http"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/kubesphere/notification-manager/pkg/deliveryprofiles"
)

// compatibilityRouter is a separate, default-disabled listener. It exposes
// only original API paths, binds every request to prepared formal delivery and
// trusts the direct socket source, never X-Forwarded-For. Deployment must also
// enforce the declared AM/notification-api identities through NetworkPolicy.
func (h *Webhook) compatibilityRouter(profiles *deliveryprofiles.Manager) (chi.Router, error) {
	if profiles == nil || h.FormalCompatListenAddress == h.ListenAddress {
		return nil, errors.New("formal compatibility requires profiles and a separate listener")
	}
	var networks []*net.IPNet
	for _, raw := range h.FormalCompatSourceCIDRs {
		ip, network, err := net.ParseCIDR(raw)
		if err != nil || !ip.Equal(network.IP) {
			return nil, errors.New("formal compatibility requires canonical source CIDRs")
		}
		ones, _ := network.Mask.Size()
		if ones == 0 {
			return nil, errors.New("formal compatibility cannot allow all source addresses")
		}
		networks = append(networks, network)
	}
	if len(networks) == 0 {
		return nil, errors.New("formal compatibility requires explicit source CIDRs")
	}
	wrap := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			host, _, err := net.SplitHostPort(r.RemoteAddr)
			if err != nil {
				http.Error(w, "unauthorized compatibility source", 403)
				return
			}
			ip := net.ParseIP(host)
			allowed := false
			for _, network := range networks {
				if network.Contains(ip) {
					allowed = true
				}
			}
			if !allowed {
				http.Error(w, "unauthorized compatibility source", 403)
				return
			}
			profiles.FormalCompatibility(next)(w, r)
		}
	}
	router := chi.NewRouter()
	router.Use(middleware.Recoverer)
	router.Use(middleware.Timeout(2 * h.WebhookTimeout))
	router.Post("/api/v2/alerts", wrap(h.handler.Alert))
	router.Post("/api/v2/notifications", wrap(h.handler.Notification))
	router.Post("/api/v2/verify", wrap(h.handler.Verify))
	router.Get("/receivers", wrap(h.handler.ListReceivers))
	router.Get("/configs", wrap(h.handler.ListConfigs))
	router.Get("/receiverWithConfig", wrap(h.handler.ListReceiverWithConfig))
	router.Get("/-/ready", wrap(h.handler.ServeReadinessCheck))
	router.Get("/-/live", wrap(h.handler.ServeHealthCheck))
	return router, nil
}
