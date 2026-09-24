package jevshadow

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/kubesphere/notification-manager/pkg/template"
)

const maxAnnotationBodyBytes = 64 * 1024

var (
	ErrDisabled       = errors.New("Jev shadow card host is disabled")
	ErrExpired        = errors.New("Jev shadow experiment expired")
	ErrCardNotFound   = errors.New("card binding not found")
	ErrRevision       = errors.New("card revision conflict")
	ErrInvalidPayload = errors.New("invalid annotation payload")
)

type CardPatcher func(context.Context, string, map[string]interface{}) error

type Service struct {
	config Config
	logger log.Logger
	client *http.Client

	mu       sync.RWMutex
	cards    map[string]*cardBinding
	receipts chan deliveryReceipt
	stop     chan struct{}
	done     chan struct{}
	close    sync.Once
}

type cardBinding struct {
	mu                 sync.Mutex
	baseCard           map[string]interface{}
	baseRevision       int
	annotationRevision int
	annotations        map[string]AnnotationComponent
	appliedKeys        map[string]appliedRequest
	patcher            CardPatcher
	createdAt          time.Time
}

type appliedRequest struct {
	payloadHash string
	requestID   string
}

func New(logger log.Logger, config Config, client *http.Client) (*Service, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	if client == nil {
		client = &http.Client{Timeout: 3 * time.Second}
	}
	service := &Service{
		config: config,
		logger: logger,
		client: client,
		cards:  make(map[string]*cardBinding),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	if config.Enabled {
		service.receipts = make(chan deliveryReceipt, config.ReceiptQueueSize)
		go service.runReceiptWorker()
	} else {
		close(service.done)
	}
	return service, nil
}

func NewFromEnv(logger log.Logger) (*Service, error) {
	config, err := ConfigFromEnv()
	if err != nil {
		return nil, err
	}
	return New(logger, config, nil)
}

func (s *Service) Enabled() bool {
	return s != nil && s.config.Enabled && !s.expired()
}

func (s *Service) ShouldCapture(receiver, destination string) bool {
	if !s.Enabled() {
		return false
	}
	if _, ok := s.config.ReceiverAllowlist[receiver]; !ok {
		return false
	}
	_, ok := s.config.DestinationAllowlist[destination]
	return ok
}

func (s *Service) CaptureSuccessfulDelivery(
	data *template.Data,
	receiver string,
	destination string,
	messageID string,
	baseCard map[string]interface{},
	patcher CardPatcher,
) {
	if !s.ShouldCapture(receiver, destination) {
		return
	}
	if messageID == "" || patcher == nil || len(data.Alerts) == 0 {
		_ = level.Error(s.logger).Log("msg", "Jev shadow skipped incomplete delivery receipt", "receiver", receiver, "destination", destination)
		return
	}
	cardCopy, err := cloneMap(baseCard)
	if err != nil {
		_ = level.Error(s.logger).Log("msg", "Jev shadow failed to copy base card", "error", err)
		return
	}
	cardHash, err := hashCanonical(cardCopy)
	if err != nil {
		_ = level.Error(s.logger).Log("msg", "Jev shadow failed to hash base card", "error", err)
		return
	}

	s.mu.Lock()
	if len(s.cards) >= s.config.MaxCards {
		s.evictOldestCardLocked()
	}
	s.cards[messageID] = &cardBinding{
		baseCard:     cardCopy,
		baseRevision: 1,
		annotations:  make(map[string]AnnotationComponent),
		appliedKeys:  make(map[string]appliedRequest),
		patcher:      patcher,
		createdAt:    time.Now().UTC(),
	}
	s.mu.Unlock()

	receipt, err := s.buildReceipt(data, receiver, destination, messageID, cardHash)
	if err != nil {
		_ = level.Error(s.logger).Log("msg", "Jev shadow failed to build delivery receipt", "error", err)
		return
	}
	select {
	case s.receipts <- receipt:
	default:
		_ = level.Error(s.logger).Log("msg", "Jev shadow delivery receipt queue is full", "receiver", receiver, "destination", destination)
	}
}

func (s *Service) Close() {
	if s == nil {
		return
	}
	s.close.Do(func() {
		if s.config.Enabled {
			close(s.stop)
		}
		<-s.done
	})
}

func (s *Service) HandleAnnotation(w http.ResponseWriter, r *http.Request, messageID string) {
	if s == nil || !s.config.Enabled {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": ErrDisabled.Error()})
		return
	}
	if s.expired() {
		writeJSON(w, http.StatusGone, map[string]string{"error": ErrExpired.Error()})
		return
	}
	if !s.authorized(r.Header.Get("Authorization")) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	if strings.TrimSpace(messageID) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "message_id is required"})
		return
	}

	body := http.MaxBytesReader(w, r.Body, maxAnnotationBodyBytes)
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	var component AnnotationComponent
	if err := decoder.Decode(&component); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": ErrInvalidPayload.Error()})
		return
	}
	if err := ensureEOF(decoder); err != nil || component.validate() != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": ErrInvalidPayload.Error()})
		return
	}
	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if len(idempotencyKey) < 8 || len(idempotencyKey) > 256 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "valid Idempotency-Key is required"})
		return
	}

	requestID, err := s.applyAnnotation(r.Context(), messageID, idempotencyKey, component)
	if err != nil {
		switch {
		case errors.Is(err, ErrCardNotFound):
			writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		case errors.Is(err, ErrRevision):
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		case errors.Is(err, ErrInvalidPayload):
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		default:
			_ = level.Error(s.logger).Log("msg", "Jev shadow Feishu card update failed", "messageID", messageID, "error", err)
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": "card update failed"})
		}
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"request_id": requestID})
}

func (s *Service) applyAnnotation(ctx context.Context, messageID, idempotencyKey string, component AnnotationComponent) (string, error) {
	s.mu.RLock()
	binding := s.cards[messageID]
	s.mu.RUnlock()
	if binding == nil {
		return "", ErrCardNotFound
	}

	binding.mu.Lock()
	defer binding.mu.Unlock()
	payloadHash, err := hashCanonical(component)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidPayload, err)
	}
	if applied, ok := binding.appliedKeys[idempotencyKey]; ok {
		if applied.payloadHash != payloadHash {
			return "", ErrRevision
		}
		return applied.requestID, nil
	}
	if component.ExpectedBaseRevision != binding.baseRevision {
		return "", ErrRevision
	}
	if component.AnnotationRevision <= binding.annotationRevision {
		return "", ErrRevision
	}
	members := append([]string(nil), component.MemberEventIDs...)
	sort.Strings(members)
	componentKey := strings.Join(members, ",")
	annotations := make(map[string]AnnotationComponent, len(binding.annotations)+1)
	for key, value := range binding.annotations {
		annotations[key] = value
	}
	annotations[componentKey] = component
	rendered, err := renderCard(binding.baseCard, annotations)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidPayload, err)
	}
	if err := binding.patcher(ctx, messageID, rendered); err != nil {
		return "", err
	}
	binding.annotations = annotations
	binding.annotationRevision = component.AnnotationRevision
	requestID := requestID(messageID, idempotencyKey)
	binding.appliedKeys[idempotencyKey] = appliedRequest{payloadHash: payloadHash, requestID: requestID}
	return requestID, nil
}

func (s *Service) authorized(header string) bool {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return false
	}
	provided := []byte(strings.TrimSpace(strings.TrimPrefix(header, prefix)))
	expected := []byte(s.config.Token)
	return len(provided) == len(expected) && subtle.ConstantTimeCompare(provided, expected) == 1
}

func (s *Service) expired() bool {
	return !s.config.ExpiresAt.IsZero() && !time.Now().UTC().Before(s.config.ExpiresAt)
}

func (s *Service) evictOldestCardLocked() {
	var oldestID string
	var oldestTime time.Time
	for messageID, binding := range s.cards {
		if oldestID == "" || binding.createdAt.Before(oldestTime) {
			oldestID = messageID
			oldestTime = binding.createdAt
		}
	}
	if oldestID != "" {
		delete(s.cards, oldestID)
	}
}

func cloneMap(value map[string]interface{}) (map[string]interface{}, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var result map[string]interface{}
	if err := json.Unmarshal(encoded, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func hashCanonical(value interface{}) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func requestID(messageID, idempotencyKey string) string {
	digest := sha256.Sum256([]byte(messageID + "\x00" + idempotencyKey))
	return "jev-card-" + hex.EncodeToString(digest[:8])
}

func ensureEOF(decoder *json.Decoder) error {
	var extra interface{}
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func renderCard(base map[string]interface{}, annotations map[string]AnnotationComponent) (map[string]interface{}, error) {
	card, err := cloneMap(base)
	if err != nil {
		return nil, err
	}
	elements, ok := card["elements"].([]interface{})
	if !ok {
		return nil, errors.New("base card has no elements array")
	}
	keys := make([]string, 0, len(annotations))
	for key := range annotations {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		component := annotations[key]
		elements = append(elements,
			map[string]interface{}{"tag": "hr"},
			map[string]interface{}{
				"tag": "div",
				"text": map[string]interface{}{
					"tag":     "lark_md",
					"content": component.markdown(),
				},
			},
		)
		if component.ActionReference != nil && *component.ActionReference != "" {
			elements = append(elements, component.feedbackActions())
		}
		if component.DetailURL != nil && *component.DetailURL != "" {
			elements = append(elements, component.detailAction())
		}
	}
	card["elements"] = elements
	return card, nil
}

type AnnotationComponent struct {
	SchemaVersion        string   `json:"schema_version"`
	AnnotationRevision   int      `json:"annotation_revision"`
	ExpectedBaseRevision int      `json:"expected_base_revision"`
	JudgmentRefs         []string `json:"judgment_refs"`
	MemberEventIDs       []string `json:"member_event_ids"`
	Title                string   `json:"title"`
	Category             string   `json:"category"`
	Route                string   `json:"route"`
	RepeatCount          int      `json:"repeat_count"`
	SameKindCount        int      `json:"same_kind_count"`
	Recommendation       string   `json:"recommendation"`
	EvidenceLines        []string `json:"evidence_lines"`
	RecurrenceLine       *string  `json:"recurrence_line"`
	RelationLine         *string  `json:"relation_line"`
	Footer               string   `json:"footer"`
	ActionReference      *string  `json:"action_reference"`
	DetailURL            *string  `json:"detail_url"`
}

func (c AnnotationComponent) validate() error {
	if c.SchemaVersion != "1" || c.AnnotationRevision < 1 || c.ExpectedBaseRevision < 1 {
		return ErrInvalidPayload
	}
	if len(c.MemberEventIDs) == 0 || len(c.MemberEventIDs) > 100 || len(c.EvidenceLines) > 20 {
		return ErrInvalidPayload
	}
	if len(c.Title) > 200 || len(c.Recommendation) > 2000 || len(c.Footer) > 1000 {
		return ErrInvalidPayload
	}
	if c.DetailURL != nil &&
		(len(*c.DetailURL) > 2000 || !strings.HasPrefix(*c.DetailURL, "https://")) {
		return ErrInvalidPayload
	}
	if c.Category != "" {
		if c.RepeatCount < 1 || c.SameKindCount < 1 {
			return ErrInvalidPayload
		}
		validCategory := c.Category == "urgent" || c.Category == "non_urgent" ||
			c.Category == "repeat" || c.Category == "same_kind" || c.Category == "protected"
		validRoute := c.Route == "oncall" || c.Route == "pool" || c.Route == "original"
		if !validCategory || !validRoute {
			return ErrInvalidPayload
		}
	}
	for _, value := range append(append([]string{}, c.MemberEventIDs...), c.EvidenceLines...) {
		if value == "" || len(value) > 4000 {
			return ErrInvalidPayload
		}
	}
	return nil
}

func (c AnnotationComponent) markdown() string {
	if c.Category != "" {
		lines := []string{"**" + c.Title + "**", c.Recommendation}
		for _, evidence := range c.EvidenceLines {
			lines = append(lines, evidence)
		}
		if c.Footer != "" {
			lines = append(lines, c.Footer)
		}
		return strings.Join(lines, "\n")
	}
	lines := []string{"**" + c.Title + "**", "建议：" + c.Recommendation}
	for _, evidence := range c.EvidenceLines {
		lines = append(lines, "- "+evidence)
	}
	if c.RecurrenceLine != nil && *c.RecurrenceLine != "" {
		lines = append(lines, "反复："+*c.RecurrenceLine)
	}
	if c.RelationLine != nil && *c.RelationLine != "" {
		lines = append(lines, "关联："+*c.RelationLine)
	}
	if c.Footer != "" {
		lines = append(lines, c.Footer)
	}
	return strings.Join(lines, "\n")
}

func (c AnnotationComponent) feedbackActions() map[string]interface{} {
	button := func(text, label, buttonType string) map[string]interface{} {
		return map[string]interface{}{
			"tag":  "button",
			"type": buttonType,
			"text": map[string]interface{}{
				"tag":     "plain_text",
				"content": text,
			},
			"value": map[string]interface{}{
				"action":           "jev_feedback",
				"dimension":        "overall",
				"correct_label":    label,
				"action_reference": *c.ActionReference,
			},
		}
	}
	return map[string]interface{}{
		"tag":    "action",
		"layout": "bisected",
		"actions": []interface{}{
			button("✅ 准确", "accurate", "primary"),
			button("❌ 不准确", "inaccurate", "default"),
		},
	}
}

func (c AnnotationComponent) detailAction() map[string]interface{} {
	return map[string]interface{}{
		"tag":    "action",
		"layout": "flow",
		"actions": []interface{}{
			map[string]interface{}{
				"tag":  "button",
				"type": "default",
				"text": map[string]interface{}{
					"tag":     "plain_text",
					"content": "查看 Jev 分析与规则",
				},
				"url": *c.DetailURL,
			},
		},
	}
}

func newRequestBody(value interface{}) (*bytes.Reader, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(encoded), nil
}
