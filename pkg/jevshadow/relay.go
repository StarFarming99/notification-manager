package jevshadow

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/kubesphere/notification-manager/pkg/template"
)

const relayCapacity = 128
const maxSuccessBytes = 2 * 1024 * 1024

// SuccessfulDelivery owns read-only notifier data after the original platform
// acknowledged the send. Neither the notifier nor relay may mutate these maps.
// Serialization, cloning, disk access and retries belong to the relay worker.
type SuccessfulDelivery struct {
	Data        *template.Data         `json:"data"`
	Receiver    string                 `json:"receiver"`
	Destination string                 `json:"destination"`
	MessageID   string                 `json:"message_id"`
	BaseCard    map[string]interface{} `json:"base_card"`
	SenderApp   string                 `json:"sender_app"`
}

type successRelay struct {
	queue              chan SuccessfulDelivery
	cancel             context.CancelFunc
	client             *http.Client
	endpoint           string
	token              string
	accepted           uint64
	dropped            uint64
	failed             uint64
	sequence           uint64
	gapOverflow        uint64
	journalErrors      uint64
	gaps               chan RelayGap
	gapMu              sync.Mutex
	recent             []RelayGap
	instance           string
	journalPath        string
	logger             log.Logger
	done               chan struct{}
	gapDone            chan struct{}
	closing            uint32
	offers             int64
	shutdownIncomplete uint32
	gapPending         int64
	processing         uint32
}

// NewNMFromEnv never initializes a disk store or a feedback listener. The legacy
// in-process mode is explicitly limited to isolated UAT for migration purposes.
func NewNMFromEnv(logger log.Logger) (*Service, error) {
	enabled, err := strconv.ParseBool(valueOrDefault(envEnabled, "false"))
	if err != nil {
		return nil, errors.New("invalid extension enable switch")
	}
	if !enabled {
		return New(logger, Config{Enabled: false}, nil)
	}
	if os.Getenv("JEV_SHADOW_MODE") == "inline-uat" {
		config, err := ConfigFromEnv()
		if err != nil {
			return nil, err
		}
		if config.Environment != "uat" && config.Environment != "test" {
			return nil, errors.New("inline Jev mode is restricted to isolated UAT")
		}
		return New(logger, config, nil)
	}
	config := Config{Enabled: true, SenderApp: strings.TrimSpace(os.Getenv(envSenderApp)),
		ReceiverAllowlist:    csvSet(os.Getenv(envReceiverAllowlist)),
		DestinationAllowlist: csvSet(os.Getenv(envDestinationAllowlist))}
	if config.SenderApp == "" || len(config.ReceiverAllowlist) == 0 || len(config.DestinationAllowlist) == 0 {
		return nil, errors.New("relay requires explicit sender, receiver and destination bindings")
	}
	endpoint := strings.TrimSpace(os.Getenv("JEV_EXECUTOR_SUCCESS_URL"))
	parsed, err := url.Parse(endpoint)
	token := strings.TrimSpace(os.Getenv("JEV_EXECUTOR_TOKEN"))
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || len(token) < 32 {
		return nil, errors.New("a valid executor endpoint and independent credential are required")
	}
	return newRelay(logger, config, endpoint, token, &http.Client{
		Timeout:       3 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}), nil
}

func newRelay(logger log.Logger, config Config, endpoint, token string, client *http.Client) *Service {
	ctx, cancel := context.WithCancel(context.Background())
	relay := &successRelay{queue: make(chan SuccessfulDelivery, relayCapacity), cancel: cancel,
		endpoint: endpoint, token: token, client: client}
	relay.gaps = make(chan RelayGap, 1024)
	relay.done = make(chan struct{})
	relay.gapDone = make(chan struct{})
	relay.instance = strconv.FormatInt(time.Now().UnixNano(), 36)
	relay.journalPath = os.Getenv("JEV_RELAY_GAP_PATH")
	if relay.journalPath == "" && filepath.IsAbs(os.Getenv("NM_NOTIFICATION_SPOOL_PATH")) {
		relay.journalPath = filepath.Join(filepath.Dir(os.Getenv("NM_NOTIFICATION_SPOOL_PATH")), "jev-relay-gaps.jsonl")
	}
	relay.logger = logger
	service := &Service{config: config, logger: logger, relay: relay}
	go relay.run(ctx)
	go relay.runGaps(ctx)
	return service
}

func (s *Service) IsRelay() bool { return s != nil && s.relay != nil }

// offer is strictly bounded, allocation-free and non-blocking. Do not add logs,
// goroutines, hashing, marshaling, mutexes, network or persistence to this hook.
func (r *successRelay) offer(delivery SuccessfulDelivery) {
	atomic.AddInt64(&r.offers, 1)
	defer atomic.AddInt64(&r.offers, -1)
	if atomic.LoadUint32(&r.closing) != 0 {
		return
	}
	sequence := atomic.AddUint64(&r.sequence, 1)
	select {
	case r.queue <- delivery:
		atomic.AddUint64(&r.accepted, 1)
	default:
		atomic.AddUint64(&r.dropped, 1)
		r.gap(delivery, "queue_full", sequence)
	}
}

// shutdown bounds extension cleanup independently of original notification
// shutdown. Stop admission before cancelling, then join the fsynced gap writer.
func (r *successRelay) shutdown(budget time.Duration) {
	atomic.StoreUint32(&r.closing, 1)
	deadline := time.NewTimer(budget)
	defer deadline.Stop()
	for atomic.LoadInt64(&r.offers) != 0 {
		select {
		case <-deadline.C:
			atomic.StoreUint32(&r.shutdownIncomplete, 1)
			r.cancel()
			_ = r.logger.Log("msg", "Jev relay shutdown incomplete", "instance", r.instance)
			return
		case <-time.After(time.Millisecond):
		}
	}
	r.cancel()
	select {
	case <-r.gapDone:
	case <-deadline.C:
		atomic.StoreUint32(&r.shutdownIncomplete, 1)
		_ = r.logger.Log("msg", "Jev relay shutdown incomplete", "instance", r.instance)
	}
}

func (r *successRelay) run(ctx context.Context) {
	defer func() {
		atomic.StoreUint32(&r.processing, 0)
		for {
			select {
			case delivery := <-r.queue:
				atomic.AddUint64(&r.failed, 1)
				r.gap(delivery, "shutdown_before_relay", atomic.AddUint64(&r.sequence, 1))
			default:
				close(r.done)
				return
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case delivery := <-r.queue:
			atomic.StoreUint32(&r.processing, 1)
			body, err := json.Marshal(delivery)
			if err != nil || len(body) > maxSuccessBytes {
				atomic.AddUint64(&r.failed, 1)
				r.gap(delivery, "serialization_failed", atomic.AddUint64(&r.sequence, 1))
				atomic.StoreUint32(&r.processing, 0)
				continue
			}
			// Relay loss is observable, never a reason to resend an original card.
			// The independent AM observer supplies coverage reconciliation.
			callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			request, err := http.NewRequestWithContext(callCtx, http.MethodPost, r.endpoint, bytes.NewReader(body))
			if err == nil {
				request.Header.Set("Authorization", "Bearer "+r.token)
				request.Header.Set("Content-Type", "application/json")
				response, sendErr := r.client.Do(request)
				if sendErr != nil {
					err = sendErr
				} else {
					_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
					_ = response.Body.Close()
					if response.StatusCode != http.StatusAccepted {
						err = errors.New("executor rejected success relay")
					}
				}
			}
			cancel()
			if err != nil {
				atomic.AddUint64(&r.failed, 1)
				r.gap(delivery, "executor_unavailable_or_rejected", atomic.AddUint64(&r.sequence, 1))
			}
			atomic.StoreUint32(&r.processing, 0)
		}
	}
}

func (s *Service) RelayStatus() map[string]uint64 {
	if !s.IsRelay() {
		return map[string]uint64{}
	}
	return map[string]uint64{"accepted": atomic.LoadUint64(&s.relay.accepted),
		"dropped": atomic.LoadUint64(&s.relay.dropped), "failed": atomic.LoadUint64(&s.relay.failed),
		"queued": uint64(len(s.relay.queue)), "gap_overflow": atomic.LoadUint64(&s.relay.gapOverflow),
		"journal_errors": atomic.LoadUint64(&s.relay.journalErrors), "sequence": atomic.LoadUint64(&s.relay.sequence),
		"shutdown_incomplete": uint64(atomic.LoadUint32(&s.relay.shutdownIncomplete))}
}

// HandleSuccessfulDelivery acknowledges only durable card binding plus receipt
// outbox acceptance. It runs exclusively in the independent executor process.
func (s *Service) HandleSuccessfulDelivery(token string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if len(token) < 32 || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var delivery SuccessfulDelivery
		reader := http.MaxBytesReader(w, r.Body, maxSuccessBytes)
		decoder := json.NewDecoder(reader)
		decoder.DisallowUnknownFields()
		if decoder.Decode(&delivery) != nil || decoder.Decode(new(interface{})) != io.EOF ||
			delivery.Data == nil || len(delivery.Data.Alerts) == 0 || delivery.MessageID == "" ||
			delivery.SenderApp != s.config.SenderApp || !s.ShouldCapture(delivery.Receiver, delivery.Destination) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		patcher, err := s.resolveCardPatcher(r.Context(), delivery.Receiver, delivery.Destination, nil)
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if err := s.captureDurable(delivery.Data, delivery.Receiver, delivery.Destination,
			delivery.MessageID, delivery.BaseCard, patcher); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}
}
