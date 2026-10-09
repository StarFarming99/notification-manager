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
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/kubesphere/notification-manager/pkg/template"
)

const (
	maxAnnotationBodyBytes       = 64 * 1024
	maxV2JudgmentIDCharacters    = 64
	maxV2MemberEventIDCharacters = 64
	maxV2PolicyReasonCharacters  = 160
	maxV2TraceURLCharacters      = 2000
	maxV2ActionRefCharacters     = 4096
)

var (
	ErrDisabled       = errors.New("Jev shadow card host is disabled")
	ErrExpired        = errors.New("Jev shadow experiment expired")
	ErrCardNotFound   = errors.New("card binding not found")
	ErrRevision       = errors.New("card revision conflict")
	ErrInvalidPayload = errors.New("invalid annotation payload")
)

type retryableAnnotationRevisionError struct {
	expected int
	received int
}

func (e *retryableAnnotationRevisionError) Error() string {
	return fmt.Sprintf(
		"%s: expected annotation revision %d, received %d",
		ErrRevision,
		e.expected,
		e.received,
	)
}

func (e *retryableAnnotationRevisionError) Unwrap() error {
	return ErrRevision
}

type CardPatcher func(context.Context, string, map[string]interface{}) error
type CardPatcherResolver func(context.Context, string, string) (CardPatcher, error)

type Service struct {
	relay  *successRelay
	config Config
	logger log.Logger
	client *http.Client

	mu              sync.RWMutex
	cards           map[string]*cardBinding
	patchers        map[string]CardPatcher
	patcherResolver CardPatcherResolver
	cardStore       *cardStateStore
	outbox          *receiptOutbox
	close           sync.Once
}

type cardBinding struct {
	mu                 sync.Mutex
	messageID          string
	receiver           string
	destination        string
	senderApp          string
	baseCard           map[string]interface{}
	baseRevision       int
	baseSendState      string
	annotationRevision int
	annotations        map[string]AnnotationComponent
	appliedKeys        map[string]appliedRequest
	patchSendState     string
	pendingPatch       *cardPatchIntent
	lastPatchError     string
	patcher            CardPatcher
	createdAt          time.Time
	updatedAt          time.Time
}

type appliedRequest struct {
	payloadHash string
	requestID   string
}

func New(logger log.Logger, config Config, client *http.Client) (*Service, error) {
	config = config.withReceiptDefaults()
	if err := config.validate(); err != nil {
		return nil, err
	}
	if client == nil {
		client = &http.Client{Timeout: 3 * time.Second}
	}
	service := &Service{
		config:   config,
		logger:   logger,
		client:   client,
		cards:    make(map[string]*cardBinding),
		patchers: make(map[string]CardPatcher),
	}
	if config.Enabled {
		cardStore, err := newCardStateStore(logger, config.CardStateDir)
		if err != nil {
			_ = level.Error(logger).Log(
				"msg", "Jev shadow adapter disabled because its card state store could not be initialized",
				"error", err,
			)
			service.config.Enabled = false
			return service, nil
		}
		service.cardStore = cardStore
		for _, state := range cardStore.LoadRecent(config.MaxCards) {
			service.cards[state.MessageID] = bindingFromCardState(state, nil)
		}
		outbox, err := newReceiptOutbox(logger, config, service.postDeliveryReceipt)
		if err != nil {
			_ = level.Error(logger).Log(
				"msg", "Jev shadow adapter disabled because its receipt outbox could not be initialized",
				"error", err,
			)
			service.config.Enabled = false
			return service, nil
		}
		service.outbox = outbox
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
	if s.IsRelay() {
		if data == nil || len(data.Alerts) == 0 || len(data.Alerts) > 1000 || messageID == "" {
			return
		}
		s.relay.offer(SuccessfulDelivery{Data: data, Receiver: receiver, Destination: destination,
			MessageID: messageID, BaseCard: baseCard, SenderApp: s.config.SenderApp})
		return
	}
	if err := s.captureDurable(data, receiver, destination, messageID, baseCard, patcher); err != nil {
		_ = level.Error(s.logger).Log("msg", "Jev successful delivery capture failed", "message_id", messageID)
	}
}

func (s *Service) captureDurable(data *template.Data, receiver, destination, messageID string,
	baseCard map[string]interface{}, patcher CardPatcher) error {
	if !s.ShouldCapture(receiver, destination) {
		return ErrDisabled
	}
	if data == nil || messageID == "" || patcher == nil || len(data.Alerts) == 0 {
		return ErrInvalidPayload
	}
	cardCopy, err := cloneMap(baseCard)
	if err != nil {
		return err
	}
	cardHash, err := hashCanonical(cardCopy)
	if err != nil {
		return err
	}
	s.registerCardPatcher(receiver, destination, patcher)
	if err := s.captureCardBinding(receiver, destination, messageID, cardCopy, patcher); err != nil {
		return err
	}
	receipt, err := s.buildReceipt(data, receiver, destination, messageID, cardHash)
	if err != nil {
		return err
	}
	if s.outbox == nil {
		return ErrCardWriterUnavailable
	}
	return s.outbox.Enqueue(receipt)
}

func (s *Service) Close() {
	if s == nil {
		return
	}
	s.close.Do(func() {
		if s.relay != nil {
			s.relay.cancel()
		}
		if s.outbox != nil {
			s.outbox.Close()
		}
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
	component, err := decodeAnnotation(body)
	if err != nil {
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
			var retryable *retryableAnnotationRevisionError
			if errors.As(err, &retryable) {
				w.Header().Set("Retry-After", "1")
				writeJSON(w, http.StatusTooManyRequests, map[string]interface{}{
					"error":                        retryable.Error(),
					"retryable":                    true,
					"expected_annotation_revision": retryable.expected,
				})
				return
			}
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		case errors.Is(err, ErrInvalidPayload):
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		case errors.Is(err, ErrCardWriterBusy):
			w.Header().Set("Retry-After", "1")
			writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": err.Error()})
		case errors.Is(err, ErrCardWriterUnavailable):
			w.Header().Set("Retry-After", "5")
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		case errors.Is(err, ErrPatchStateUnknown):
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		default:
			_ = level.Error(s.logger).Log("msg", "Jev shadow Feishu card update failed", "messageID", messageID, "error", err)
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": "card update failed"})
		}
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"request_id": requestID})
}

func (s *Service) applyAnnotation(ctx context.Context, messageID, idempotencyKey string, component AnnotationComponent) (string, error) {
	if s.cardStore != nil {
		return s.applyPersistentAnnotation(ctx, messageID, idempotencyKey, component)
	}
	return s.applyInMemoryAnnotation(ctx, messageID, idempotencyKey, component)
}

func (s *Service) applyInMemoryAnnotation(ctx context.Context, messageID, idempotencyKey string, component AnnotationComponent) (string, error) {
	binding, err := s.getCardBinding(messageID)
	if err != nil {
		return "", err
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
	expectedAnnotationRevision := binding.annotationRevision + 1
	if component.AnnotationRevision > expectedAnnotationRevision {
		return "", &retryableAnnotationRevisionError{
			expected: expectedAnnotationRevision,
			received: component.AnnotationRevision,
		}
	}
	if component.AnnotationRevision != expectedAnnotationRevision {
		return "", ErrRevision
	}
	componentKey := stableComponentKey(component.MemberEventIDs)
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

func stableComponentKey(memberEventIDs []string) string {
	members := append([]string(nil), memberEventIDs...)
	sort.Strings(members)
	encoded, _ := json.Marshal(members)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func (s *Service) authorized(header string) bool {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return false
	}
	provided := []byte(strings.TrimSpace(strings.TrimPrefix(header, prefix)))
	token := s.config.AnnotationToken
	if token == "" {
		token = s.config.Token
	}
	expected := []byte(token)
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

func decodeAnnotation(reader io.Reader) (AnnotationComponent, error) {
	encoded, err := io.ReadAll(reader)
	if err != nil {
		return AnnotationComponent{}, err
	}
	var discriminator struct {
		SchemaVersion json.RawMessage `json:"schema_version"`
	}
	if err := json.Unmarshal(encoded, &discriminator); err != nil || len(discriminator.SchemaVersion) == 0 {
		return AnnotationComponent{}, ErrInvalidPayload
	}
	var schemaVersion string
	if err := json.Unmarshal(discriminator.SchemaVersion, &schemaVersion); err != nil {
		return AnnotationComponent{}, ErrInvalidPayload
	}

	var component AnnotationComponent
	switch schemaVersion {
	case "1":
		if err := requireJSONFields(encoded, []string{
			"schema_version", "annotation_revision", "expected_base_revision", "judgment_refs",
			"member_event_ids", "category", "route", "recommendation", "evidence_lines", "footer",
		}); err != nil {
			return AnnotationComponent{}, err
		}
		var wire annotationComponentV1
		if err := strictDecodeJSON(encoded, &wire); err != nil {
			return AnnotationComponent{}, err
		}
		component = wire.component()
	case "2":
		if err := requireJSONFields(encoded, []string{
			"schema_version", "judgment_id", "member_event_ids", "annotation_revision",
			"expected_base_revision", "routing", "pool_reason", "policy_proposal",
			"policy_reason_code", "execution_mode", "trace_url",
		}); err != nil {
			return AnnotationComponent{}, err
		}
		var wire annotationComponentV2
		if err := strictDecodeJSON(encoded, &wire); err != nil {
			return AnnotationComponent{}, err
		}
		component = wire.component()
	default:
		return AnnotationComponent{}, ErrInvalidPayload
	}
	if err := component.validate(); err != nil {
		return AnnotationComponent{}, err
	}
	return component, nil
}

func requireJSONFields(encoded []byte, fields []string) error {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &object); err != nil {
		return err
	}
	for _, field := range fields {
		if _, ok := object[field]; !ok {
			return ErrInvalidPayload
		}
	}
	return nil
}

func strictDecodeJSON(encoded []byte, value interface{}) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	return ensureEOF(decoder)
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
		showMemberScope := len(annotations) > 1 || len(component.MemberEventIDs) > 1
		elements = append(elements,
			map[string]interface{}{"tag": "hr"},
			map[string]interface{}{
				"tag": "div",
				"text": map[string]interface{}{
					"tag":     "lark_md",
					"content": component.markdownWithMemberScope(showMemberScope),
				},
			},
		)
		if component.ActionReference != nil && *component.ActionReference != "" {
			elements = append(elements, component.feedbackActions())
		}
		if detailURL := component.detailURL(); detailURL != nil && *detailURL != "" {
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
	MemberEventIDs       []string `json:"member_event_ids"`

	// v1 fields remain available until all existing producers have migrated.
	JudgmentRefs   []string `json:"judgment_refs,omitempty"`
	Title          string   `json:"title,omitempty"`
	Category       string   `json:"category,omitempty"`
	Route          string   `json:"route,omitempty"`
	RepeatCount    int      `json:"repeat_count,omitempty"`
	SameKindCount  int      `json:"same_kind_count,omitempty"`
	Recommendation string   `json:"recommendation,omitempty"`
	EvidenceLines  []string `json:"evidence_lines,omitempty"`
	RecurrenceLine *string  `json:"recurrence_line,omitempty"`
	RelationLine   *string  `json:"relation_line,omitempty"`
	Footer         string   `json:"footer,omitempty"`
	DetailURL      *string  `json:"detail_url,omitempty"`

	// v2 fields are deliberately separate from the v1 presentation DTO. The HTTP
	// decoder selects one wire schema first, so a v2 payload cannot smuggle v1
	// presentation fields (or vice versa) through a permissive union struct.
	JudgmentID         string        `json:"judgment_id,omitempty"`
	Routing            *ChoiceAnswer `json:"routing,omitempty"`
	PoolReason         *ChoiceAnswer `json:"pool_reason,omitempty"`
	PolicyProposal     string        `json:"policy_proposal,omitempty"`
	PolicyReasonCode   string        `json:"policy_reason_code,omitempty"`
	PoolMinProbability *float64      `json:"pool_min_probability,omitempty"`
	Capability         string        `json:"capability,omitempty"`
	DecisionSource     string        `json:"decision_source,omitempty"`
	ThresholdApplied   *bool         `json:"threshold_applied,omitempty"`
	ThresholdSource    string        `json:"threshold_source,omitempty"`
	ModelStatus        string        `json:"model_status,omitempty"`
	ModelErrorCode     string        `json:"model_error_code,omitempty"`
	ContextReduced     *bool         `json:"context_reduced,omitempty"`
	ExecutionMode      string        `json:"execution_mode,omitempty"`
	TraceURL           *string       `json:"trace_url,omitempty"`

	ActionReference *string `json:"action_reference,omitempty"`
}

type ChoiceAnswer struct {
	Choice        string             `json:"choice"`
	Probabilities map[string]float64 `json:"probabilities"`
}

type annotationComponentV1 struct {
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

func (c AnnotationComponent) MarshalJSON() ([]byte, error) {
	switch c.SchemaVersion {
	case "1":
		return json.Marshal(annotationComponentV1{
			SchemaVersion:        c.SchemaVersion,
			AnnotationRevision:   c.AnnotationRevision,
			ExpectedBaseRevision: c.ExpectedBaseRevision,
			JudgmentRefs:         c.JudgmentRefs,
			MemberEventIDs:       c.MemberEventIDs,
			Title:                c.Title,
			Category:             c.Category,
			Route:                c.Route,
			RepeatCount:          c.RepeatCount,
			SameKindCount:        c.SameKindCount,
			Recommendation:       c.Recommendation,
			EvidenceLines:        c.EvidenceLines,
			RecurrenceLine:       c.RecurrenceLine,
			RelationLine:         c.RelationLine,
			Footer:               c.Footer,
			ActionReference:      c.ActionReference,
			DetailURL:            c.DetailURL,
		})
	case "2":
		return json.Marshal(annotationComponentV2{
			SchemaVersion:        c.SchemaVersion,
			JudgmentID:           c.JudgmentID,
			MemberEventIDs:       c.MemberEventIDs,
			AnnotationRevision:   c.AnnotationRevision,
			ExpectedBaseRevision: c.ExpectedBaseRevision,
			Routing:              c.Routing,
			PoolReason:           c.PoolReason,
			PolicyProposal:       c.PolicyProposal,
			PolicyReasonCode:     c.PolicyReasonCode,
			PoolMinProbability:   c.PoolMinProbability,
			Capability:           c.Capability,
			DecisionSource:       c.DecisionSource,
			ThresholdApplied:     c.ThresholdApplied,
			ThresholdSource:      c.ThresholdSource,
			ModelStatus:          c.ModelStatus,
			ModelErrorCode:       c.ModelErrorCode,
			ContextReduced:       c.ContextReduced,
			ExecutionMode:        c.ExecutionMode,
			TraceURL:             c.TraceURL,
			ActionReference:      c.ActionReference,
		})
	default:
		type annotationComponentAlias AnnotationComponent
		return json.Marshal(annotationComponentAlias(c))
	}
}

func (wire annotationComponentV1) component() AnnotationComponent {
	return AnnotationComponent{
		SchemaVersion:        wire.SchemaVersion,
		AnnotationRevision:   wire.AnnotationRevision,
		ExpectedBaseRevision: wire.ExpectedBaseRevision,
		JudgmentRefs:         wire.JudgmentRefs,
		MemberEventIDs:       wire.MemberEventIDs,
		Title:                wire.Title,
		Category:             wire.Category,
		Route:                wire.Route,
		RepeatCount:          wire.RepeatCount,
		SameKindCount:        wire.SameKindCount,
		Recommendation:       wire.Recommendation,
		EvidenceLines:        wire.EvidenceLines,
		RecurrenceLine:       wire.RecurrenceLine,
		RelationLine:         wire.RelationLine,
		Footer:               wire.Footer,
		ActionReference:      wire.ActionReference,
		DetailURL:            wire.DetailURL,
	}
}

type annotationComponentV2 struct {
	SchemaVersion        string        `json:"schema_version"`
	JudgmentID           string        `json:"judgment_id"`
	MemberEventIDs       []string      `json:"member_event_ids"`
	AnnotationRevision   int           `json:"annotation_revision"`
	ExpectedBaseRevision int           `json:"expected_base_revision"`
	Routing              *ChoiceAnswer `json:"routing"`
	PoolReason           *ChoiceAnswer `json:"pool_reason"`
	PolicyProposal       string        `json:"policy_proposal"`
	PolicyReasonCode     string        `json:"policy_reason_code"`
	PoolMinProbability   *float64      `json:"pool_min_probability,omitempty"`
	Capability           string        `json:"capability,omitempty"`
	DecisionSource       string        `json:"decision_source,omitempty"`
	ThresholdApplied     *bool         `json:"threshold_applied,omitempty"`
	ThresholdSource      string        `json:"threshold_source,omitempty"`
	ModelStatus          string        `json:"model_status,omitempty"`
	ModelErrorCode       string        `json:"model_error_code,omitempty"`
	ContextReduced       *bool         `json:"context_reduced,omitempty"`
	ExecutionMode        string        `json:"execution_mode"`
	TraceURL             *string       `json:"trace_url"`
	ActionReference      *string       `json:"action_reference,omitempty"`
}

func (wire annotationComponentV2) component() AnnotationComponent {
	return AnnotationComponent{
		SchemaVersion:        wire.SchemaVersion,
		JudgmentID:           wire.JudgmentID,
		MemberEventIDs:       wire.MemberEventIDs,
		AnnotationRevision:   wire.AnnotationRevision,
		ExpectedBaseRevision: wire.ExpectedBaseRevision,
		Routing:              wire.Routing,
		PoolReason:           wire.PoolReason,
		PolicyProposal:       wire.PolicyProposal,
		PolicyReasonCode:     wire.PolicyReasonCode,
		PoolMinProbability:   wire.PoolMinProbability,
		Capability:           wire.Capability,
		DecisionSource:       wire.DecisionSource,
		ThresholdApplied:     wire.ThresholdApplied,
		ThresholdSource:      wire.ThresholdSource,
		ModelStatus:          wire.ModelStatus,
		ModelErrorCode:       wire.ModelErrorCode,
		ContextReduced:       wire.ContextReduced,
		ExecutionMode:        wire.ExecutionMode,
		TraceURL:             wire.TraceURL,
		ActionReference:      wire.ActionReference,
	}
}

func (c AnnotationComponent) validate() error {
	if c.AnnotationRevision < 1 || c.ExpectedBaseRevision < 1 {
		return ErrInvalidPayload
	}
	if err := validateMemberEventIDs(c.MemberEventIDs, c.SchemaVersion); err != nil {
		return ErrInvalidPayload
	}
	switch c.SchemaVersion {
	case "1":
		return c.validateV1()
	case "2":
		return c.validateV2()
	default:
		return ErrInvalidPayload
	}
}

func (c AnnotationComponent) validateV1() error {
	if len(c.EvidenceLines) > 20 {
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

func (c AnnotationComponent) validateV2() error {
	if utf8.RuneCountInString(c.JudgmentID) > maxV2JudgmentIDCharacters || c.ExecutionMode != "shadow" {
		return ErrInvalidPayload
	}
	if c.TraceURL != nil && (*c.TraceURL == "" || utf8.RuneCountInString(*c.TraceURL) > maxV2TraceURLCharacters ||
		!strings.HasPrefix(*c.TraceURL, "https://")) {
		return ErrInvalidPayload
	}
	if c.ActionReference != nil && utf8.RuneCountInString(*c.ActionReference) > maxV2ActionRefCharacters {
		return ErrInvalidPayload
	}
	if c.PolicyProposal != "oncall" && c.PolicyProposal != "pool" && c.PolicyProposal != "original" {
		return ErrInvalidPayload
	}
	if c.PolicyReasonCode == "" || !validReasonCode(c.PolicyReasonCode) {
		return ErrInvalidPayload
	}
	if c.Capability != "" {
		if c.Capability != "policy-facts-v1" {
			return ErrInvalidPayload
		}
		return c.validatePolicyFactsV1()
	}

	if c.Routing == nil {
		if c.PoolReason != nil || c.PolicyProposal != "original" || c.PolicyReasonCode == "" ||
			c.PoolMinProbability != nil {
			return ErrInvalidPayload
		}
		return nil
	}
	if strings.TrimSpace(c.JudgmentID) == "" || c.TraceURL == nil {
		return ErrInvalidPayload
	}
	if c.PoolReason == nil || c.PolicyProposal == "original" {
		return ErrInvalidPayload
	}
	if err := validateChoiceAnswer(c.Routing, []string{"pool", "oncall"}); err != nil {
		return err
	}
	if err := validateChoiceAnswer(
		c.PoolReason,
		[]string{"recurring", "same_incident", "non_urgent", "unclear"},
	); err != nil {
		return err
	}
	if c.PolicyProposal == "pool" && c.Routing.Choice != "pool" {
		return ErrInvalidPayload
	}
	if c.PolicyProposal != c.Routing.Choice && c.PolicyReasonCode == "" {
		return ErrInvalidPayload
	}
	if c.PolicyReasonCode == "pool_probability_below_threshold" {
		if c.PoolMinProbability == nil || !validProbability(*c.PoolMinProbability) ||
			c.PolicyProposal != "oncall" || c.Routing.Probabilities["pool"] >= *c.PoolMinProbability {
			return ErrInvalidPayload
		}
	} else if c.PoolMinProbability != nil {
		return ErrInvalidPayload
	}
	return nil
}

func (c AnnotationComponent) validatePolicyFactsV1() error {
	if c.ThresholdApplied == nil || c.ContextReduced == nil ||
		(c.ThresholdSource != "default" && c.ThresholdSource != "override" &&
			c.ThresholdSource != "not_applicable") ||
		utf8.RuneCountInString(c.ModelErrorCode) > maxV2PolicyReasonCharacters {
		return ErrInvalidPayload
	}
	modelAvailable := c.ModelStatus == "available"
	if !modelAvailable && c.ModelStatus != "unavailable" {
		return ErrInvalidPayload
	}
	if modelAvailable != (c.Routing != nil && c.PoolReason != nil) {
		return ErrInvalidPayload
	}
	if strings.TrimSpace(c.JudgmentID) == "" || c.TraceURL == nil {
		return ErrInvalidPayload
	}
	if modelAvailable {
		if err := validateChoiceAnswer(c.Routing, []string{"pool", "oncall"}); err != nil {
			return err
		}
		if err := validateChoiceAnswer(
			c.PoolReason,
			[]string{"recurring", "same_incident", "non_urgent", "unclear"},
		); err != nil {
			return err
		}
	}
	switch c.DecisionSource {
	case "operator_rule":
		if c.PolicyProposal != "pool" || *c.ThresholdApplied ||
			c.PoolMinProbability != nil || c.ThresholdSource != "not_applicable" {
			return ErrInvalidPayload
		}
	case "model_policy":
		if !modelAvailable || !*c.ThresholdApplied || c.PoolMinProbability == nil ||
			!validProbability(*c.PoolMinProbability) || c.ThresholdSource == "not_applicable" {
			return ErrInvalidPayload
		}
		expected := "oncall"
		if c.Routing.Probabilities["pool"] >= *c.PoolMinProbability {
			expected = "pool"
		}
		if c.PolicyProposal != expected {
			return ErrInvalidPayload
		}
	case "model_unavailable":
		if modelAvailable || c.PolicyProposal != "original" || *c.ThresholdApplied ||
			c.PoolMinProbability != nil || c.ThresholdSource != "not_applicable" {
			return ErrInvalidPayload
		}
	default:
		return ErrInvalidPayload
	}
	return nil
}

func validateMemberEventIDs(values []string, schemaVersion string) error {
	if len(values) == 0 || len(values) > 100 {
		return ErrInvalidPayload
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if schemaVersion == "2" {
			characters := utf8.RuneCountInString(value)
			if characters == 0 || characters > maxV2MemberEventIDCharacters {
				return ErrInvalidPayload
			}
		} else if strings.TrimSpace(value) == "" || len(value) > 4000 {
			return ErrInvalidPayload
		}
		if _, ok := seen[value]; ok {
			return ErrInvalidPayload
		}
		seen[value] = struct{}{}
	}
	return nil
}

func validReasonCode(value string) bool {
	characters := utf8.RuneCountInString(value)
	return characters >= 1 && characters <= maxV2PolicyReasonCharacters
}

func validateChoiceAnswer(answer *ChoiceAnswer, options []string) error {
	if answer == nil || len(answer.Probabilities) != len(options) {
		return ErrInvalidPayload
	}
	choiceProbability, ok := answer.Probabilities[answer.Choice]
	if !ok {
		return ErrInvalidPayload
	}
	sum := 0.0
	lowerBound := 0.0
	upperBound := 0.0
	twoDecimalResponse := true
	for _, option := range options {
		probability, present := answer.Probabilities[option]
		if !present || !validProbability(probability) || choiceProbability < probability {
			return ErrInvalidPayload
		}
		sum += probability
		lowerBound += math.Max(0, probability-0.005)
		upperBound += math.Min(1, probability+0.005)
		if math.Abs(probability*100-math.Round(probability*100)) > 1e-9 {
			twoDecimalResponse = false
		}
	}
	if math.Abs(sum-1) <= 1e-9 {
		return nil
	}
	if !twoDecimalResponse || lowerBound > 1+1e-9 || upperBound < 1-1e-9 {
		return ErrInvalidPayload
	}
	return nil
}

func validProbability(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value >= 0 && value <= 1
}

func (c AnnotationComponent) markdown() string {
	return c.markdownWithMemberScope(false)
}

func (c AnnotationComponent) markdownWithMemberScope(showMemberScope bool) string {
	if c.SchemaVersion == "2" {
		return c.markdownV2(showMemberScope)
	}
	if c.Category != "" {
		lines := []string{"**" + c.Title + "（Shadow）**", "**结论：" + c.Recommendation + "**"}
		for _, evidence := range c.EvidenceLines {
			lines = append(lines, evidence)
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

func (c AnnotationComponent) markdownV2(showMemberScope bool) string {
	if c.Routing == nil {
		status := "Jev unavailable · Original routing retained"
		if protectedReason(c.PolicyReasonCode) {
			status = "Protected · Original routing retained"
		}
		if c.Capability == "policy-facts-v1" && c.ModelErrorCode != "" {
			status = "Jev unavailable · " + modelErrorLabel(c.ModelErrorCode)
		}
		lines := make([]string, 0, 4)
		if showMemberScope {
			lines = append(lines, c.memberScopeLine())
		}
		lines = append(lines, status)
		if policyLine := c.policyLineV2(); policyLine != "" {
			lines = append(lines, policyLine)
		}
		if c.ContextReduced != nil && *c.ContextReduced {
			lines = append(lines, "Context reduced · See Trace for omitted evidence")
		}
		return strings.Join(lines, "\n")
	}

	routing := c.Routing.Probabilities
	lines := make([]string, 0, 6)
	if showMemberScope {
		lines = append(lines, c.memberScopeLine())
	}
	if c.Routing.Choice == "pool" ||
		(c.Capability == "policy-facts-v1" && c.PolicyProposal == "pool") {
		reasons := c.PoolReason.Probabilities
		probabilityLine := "**Pool " + formatProbability(routing["pool"]) + "** · Oncall " + formatProbability(routing["oncall"])
		if c.Routing.Choice == "oncall" {
			probabilityLine = "**Oncall " + formatProbability(routing["oncall"]) + "** · Pool " + formatProbability(routing["pool"])
		}
		lines = append(lines,
			probabilityLine,
			"**"+poolReasonLabel(c.PoolReason.Choice)+"**",
			"Recurring "+formatProbability(reasons["recurring"])+" · Same incident "+formatProbability(reasons["same_incident"]),
			"Non-urgent "+formatProbability(reasons["non_urgent"])+" · Unclear "+formatProbability(reasons["unclear"]),
		)
	} else {
		lines = append(lines,
			"**Oncall "+formatProbability(routing["oncall"])+"** · Pool "+formatProbability(routing["pool"]),
		)
	}
	if policyLine := c.policyLineV2(); policyLine != "" {
		lines = append(lines, policyLine)
	}
	if c.ContextReduced != nil && *c.ContextReduced {
		lines = append(lines, "Context reduced · See Trace for omitted evidence")
	}
	return strings.Join(lines, "\n")
}

func (c AnnotationComponent) memberScopeLine() string {
	const maxVisibleMembers = 3

	members := append([]string(nil), c.MemberEventIDs...)
	sort.Strings(members)
	visible := len(members)
	if visible > maxVisibleMembers {
		visible = maxVisibleMembers
	}
	aliases := make([]string, 0, visible+1)
	for _, member := range members[:visible] {
		digest := sha256.Sum256([]byte("jev-card-member\x00" + member))
		aliases = append(aliases, "m-"+hex.EncodeToString(digest[:6]))
	}
	if hidden := len(members) - visible; hidden > 0 {
		aliases = append(aliases, fmt.Sprintf("+%d more", hidden))
	}
	label := "Member"
	if len(members) != 1 {
		label = "Members"
	}
	return label + ": " + strings.Join(aliases, " · ")
}

func (c AnnotationComponent) policyLineV2() string {
	if c.Capability == "policy-facts-v1" {
		label := "Oncall"
		if c.PolicyProposal == "pool" {
			label = "Pool"
		} else if c.PolicyProposal == "original" {
			label = "Original routing retained"
		}
		switch c.DecisionSource {
		case "operator_rule":
			return "Policy: " + label + " · Operator rule"
		case "model_policy":
			if c.PoolMinProbability != nil {
				return "Policy: " + label + " · Pool threshold " + formatProbability(*c.PoolMinProbability)
			}
		case "model_unavailable":
			return "Policy: " + label + " · Model unavailable"
		}
		return "Policy: " + label
	}
	if c.Routing == nil || c.PolicyProposal == c.Routing.Choice {
		return ""
	}
	label := "Oncall"
	if c.PolicyProposal == "pool" {
		label = "Pool"
	} else if c.PolicyProposal == "original" {
		label = "Original routing retained"
	}
	reason := policyReasonLabel(c.PolicyReasonCode, c.PoolMinProbability)
	if reason == "" {
		return "Policy: " + label
	}
	return "Policy: " + label + " · " + reason
}

func modelErrorLabel(code string) string {
	switch code {
	case "minimum_state_budget_exceeded", "request_body_budget_exceeded", "questions_budget_exceeded":
		return "Context budget exceeded"
	case "model_disabled":
		return "Model disabled"
	default:
		return code
	}
}

func formatProbability(value float64) string {
	return fmt.Sprintf("%.0f%%", value*100)
}

func poolReasonLabel(value string) string {
	switch value {
	case "recurring":
		return "Recurring"
	case "same_incident":
		return "Same incident"
	case "non_urgent":
		return "Non-urgent"
	case "unclear":
		return "Unclear"
	default:
		return "Unclear"
	}
}

func policyReasonLabel(code string, poolMinProbability *float64) string {
	switch code {
	case "pool_probability_below_threshold":
		if poolMinProbability != nil {
			return "Pool threshold " + formatProbability(*poolMinProbability)
		}
		return "Pool threshold not met"
	case "first_notification_unconfirmed":
		return "First notification unconfirmed"
	case "model_selected_oncall":
		return "Model selected Oncall"
	case "routing_tie":
		return "Routing tie"
	case "pool_reason_unclear":
		return "Pool reason unclear"
	case "pool_reason_tie":
		return "Pool reason tie"
	case "incident_binding_unconfirmed":
		return "Incident binding unconfirmed"
	case "critical_evidence_missing":
		return "Critical evidence missing"
	case "material_risk_present":
		return "Material risk present"
	case "recurring_episode_unverified":
		return "Recurring episode unverified"
	default:
		return ""
	}
}

func protectedReason(code string) bool {
	return code == "protected_scope" || strings.HasPrefix(code, "protected_") ||
		strings.HasSuffix(code, "_protected")
}

func (c AnnotationComponent) detailURL() *string {
	if c.SchemaVersion == "2" {
		return c.TraceURL
	}
	return c.DetailURL
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
	detailURL := c.detailURL()
	return map[string]interface{}{
		"tag":    "action",
		"layout": "flow",
		"actions": []interface{}{
			map[string]interface{}{
				"tag":  "button",
				"type": "default",
				"text": map[string]interface{}{
					"tag":     "plain_text",
					"content": "查看判断详情",
				},
				"url": *detailURL,
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
