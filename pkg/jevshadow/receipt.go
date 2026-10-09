package jevshadow

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/kubesphere/notification-manager/pkg/template"
)

type deliveryReceipt struct {
	ProfileID            string           `json:"profile_id,omitempty"`
	ProfileVersion       string           `json:"profile_version,omitempty"`
	OriginalDestination  string           `json:"original_destination,omitempty"`
	CardOwnerID          string           `json:"card_owner_id,omitempty"`
	ExecutionDomain      string           `json:"execution_domain,omitempty"`
	SchemaVersion        string           `json:"schema_version"`
	LogicalSource        string           `json:"logical_source"`
	Environment          string           `json:"environment"`
	SourceRegion         string           `json:"source_region"`
	PermissionDomain     string           `json:"permission_domain"`
	Receiver             string           `json:"receiver"`
	DeliveryID           string           `json:"delivery_id"`
	Destination          string           `json:"destination"`
	SenderApp            string           `json:"sender_app"`
	MessageID            string           `json:"message_id"`
	SendState            string           `json:"send_state"`
	SentAt               time.Time        `json:"sent_at"`
	BaseCardRevision     int              `json:"base_card_revision"`
	BaseCardSnapshotHash string           `json:"base_card_snapshot_hash"`
	Members              []receiptMember  `json:"members"`
	Observation          *observationWire `json:"observation,omitempty"`
}

type receiptMember struct {
	EventID     string    `json:"event_id,omitempty"`
	Fingerprint string    `json:"fingerprint"`
	StartsAt    time.Time `json:"starts_at"`
}

func (s *Service) buildReceipt(data *template.Data, receiver, destination, messageID, cardHash string) (deliveryReceipt, error) {
	selected, err := s.scopeService(data)
	if err != nil {
		return deliveryReceipt{}, err
	}
	if selected != s {
		return selected.buildReceipt(data, receiver, destination, messageID, cardHash)
	}
	if s.config.expectedProfileID != "" && !s.ShouldCapture(receiver, destination) {
		return deliveryReceipt{}, ErrInvalidPayload
	}
	if s.config.Environment == "production" {
		var err error
		data, err = s.canonicalData(data)
		if err != nil {
			return deliveryReceipt{}, err
		}
	}
	deliveryID := strings.TrimSpace(data.DeliveryID)
	if deliveryID == "" && s.config.Environment == "production" {
		deliveryID = "nm_" + framedDigest("sent-notification-v2", s.config.LogicalSource, receiver, destination, messageID)[:32]
	}
	if deliveryID == "" {
		var err error
		deliveryID, err = deriveDeliveryID(data, s.config.ObservationReceiver)
		if err != nil {
			return deliveryReceipt{}, err
		}
	}
	members := make([]receiptMember, 0, len(data.Alerts))
	for _, alert := range data.Alerts {
		fingerprint := strings.TrimSpace(alert.ID)
		if fingerprint == "" {
			labels := labelsWithoutReceiver(alert.Labels)
			encoded, marshalErr := json.Marshal(labels)
			if marshalErr != nil {
				return deliveryReceipt{}, marshalErr
			}
			digest := sha256.Sum256(encoded)
			fingerprint = hex.EncodeToString(digest[:])
		}
		member := receiptMember{Fingerprint: fingerprint, StartsAt: alert.StartsAt.UTC()}
		if s.config.Environment == "production" {
			ep, err := episodeV2(s.config.LogicalSource, s.config.Environment, s.config.PermissionDomain, fingerprint, alert.StartsAt)
			if err != nil {
				return deliveryReceipt{}, err
			}
			member.EventID = occurrenceV2(ep, alert)
		}
		members = append(members, member)
	}
	sort.Slice(members, func(i, j int) bool {
		if members[i].Fingerprint == members[j].Fingerprint {
			return members[i].StartsAt.Before(members[j].StartsAt)
		}
		return members[i].Fingerprint < members[j].Fingerprint
	})
	receipt := deliveryReceipt{
		ProfileID: data.ProfileID, ProfileVersion: data.ProfileVersion, OriginalDestination: data.OriginalDestination,
		CardOwnerID: s.config.CardOwnerID, ExecutionDomain: s.config.ExecutionDomain,
		SchemaVersion:        "1",
		LogicalSource:        s.config.LogicalSource,
		Environment:          s.config.Environment,
		SourceRegion:         s.config.SourceRegion,
		PermissionDomain:     s.config.PermissionDomain,
		Receiver:             s.config.ObservationReceiver,
		DeliveryID:           deliveryID,
		Destination:          destination,
		SenderApp:            s.config.SenderApp,
		MessageID:            messageID,
		SendState:            "succeeded",
		SentAt:               time.Now().UTC(),
		BaseCardRevision:     1,
		BaseCardSnapshotHash: cardHash,
		Members:              members,
	}
	if data.CardOwnerID != "" && data.CardOwnerID != s.config.CardOwnerID {
		return deliveryReceipt{}, fmt.Errorf("frozen card owner is outside this executor scope")
	}
	if s.config.Environment == "production" {
		receipt.Observation = s.observation(data, receipt)
	}
	return receipt, nil
}

// StampDeliveryID keeps the original member-derived identity available for
// callers outside the normal notify-stage fanout. Runtime fanout uses
// StampDeliveryIDForAttempt so differing channel member sets still correlate.
func (s *Service) StampDeliveryID(data *template.Data) error {
	deliveryID, err := deriveDeliveryID(data, s.config.ObservationReceiver)
	if err != nil {
		return err
	}
	data.DeliveryID = deliveryID
	return nil
}

// NewDeliveryAttemptID creates the immutable parent identity for one notify-stage
// fanout. The same value must be used for every channel in that fanout; a later
// Alertmanager repeat gets a new value even when its members and content are
// unchanged.
func (s *Service) NewDeliveryAttemptID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

// StampDeliveryIDForAttempt derives a channel-independent delivery envelope.
// Channel selectors may turn the same group into A+B for the observation
// webhook and A for Feishu, so members must not participate in this ID. The
// receipt's members remain the authoritative scope and are matched by their
// immutable (delivery_id, fingerprint, starts_at) occurrence identity.
func (s *Service) StampDeliveryIDForAttempt(data *template.Data, attemptID string) error {
	deliveryID, err := deriveAttemptDeliveryID(data, s.config.ObservationReceiver, attemptID)
	if err != nil {
		return err
	}
	data.DeliveryID = deliveryID
	return nil
}

func deriveAttemptDeliveryID(data *template.Data, observationReceiver, attemptID string) (string, error) {
	if strings.TrimSpace(attemptID) == "" {
		return "", fmt.Errorf("delivery attempt ID is required")
	}
	canonical := map[string]interface{}{
		"identity_version": 2,
		"attempt_id":       attemptID,
		"receiver":         observationReceiver,
		"group_labels":     map[string]string(data.GroupLabels),
	}
	encoded, err := canonicalJSON(canonical)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return "nm_" + hex.EncodeToString(digest[:16]), nil
}

func deriveDeliveryID(data *template.Data, observationReceiver string) (string, error) {
	alerts := make([]map[string]interface{}, 0, len(data.Alerts))
	for _, alert := range data.Alerts {
		labels := labelsWithoutReceiver(alert.Labels)
		fingerprint := strings.TrimSpace(alert.ID)
		if fingerprint == "" {
			encoded, err := canonicalJSON(labels)
			if err != nil {
				return "", err
			}
			digest := sha256.Sum256(encoded)
			fingerprint = hex.EncodeToString(digest[:])
		}
		var endsAt *string
		if alert.Status == "resolved" && !alert.EndsAt.Before(alert.StartsAt) {
			value := pythonISOTime(alert.EndsAt)
			endsAt = &value
		}
		alerts = append(alerts, map[string]interface{}{
			"fingerprint":       fingerprint,
			"status":            alert.Status,
			"starts_at":         pythonISOTime(alert.StartsAt),
			"ends_at":           endsAt,
			"notification_time": occurrenceTime(alert),
		})
	}
	sort.Slice(alerts, func(i, j int) bool {
		left, _ := canonicalJSON(alerts[i])
		right, _ := canonicalJSON(alerts[j])
		return string(left) < string(right)
	})
	canonical := map[string]interface{}{
		"receiver":     observationReceiver,
		"group_labels": map[string]string(data.GroupLabels),
		"alerts":       alerts,
	}
	encoded, err := canonicalJSON(canonical)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return "nm_" + hex.EncodeToString(digest[:16]), nil
}

func occurrenceTime(alert *template.Alert) *string {
	if alert.NotificationTime.IsZero() || alert.NotificationTime.Year() < 1970 {
		return nil
	}
	value := pythonISOTime(alert.NotificationTime)
	return &value
}

func labelsWithoutReceiver(labels template.KV) map[string]string {
	result := make(map[string]string, len(labels))
	for key, value := range labels {
		if key != "receiver" {
			result[key] = value
		}
	}
	return result
}

func pythonISOTime(value time.Time) string {
	value = value.UTC().Truncate(time.Microsecond)
	if value.Nanosecond() == 0 {
		return value.Format("2006-01-02T15:04:05+00:00")
	}
	return value.Format("2006-01-02T15:04:05.000000+00:00")
}

func canonicalJSON(value interface{}) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buffer.Bytes(), []byte("\n")), nil
}
