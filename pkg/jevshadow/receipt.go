package jevshadow

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/kubesphere/notification-manager/pkg/template"
)

type deliveryReceipt struct {
	SchemaVersion        string          `json:"schema_version"`
	LogicalSource        string          `json:"logical_source"`
	Environment          string          `json:"environment"`
	SourceRegion         string          `json:"source_region"`
	PermissionDomain     string          `json:"permission_domain"`
	Receiver             string          `json:"receiver"`
	DeliveryID           string          `json:"delivery_id"`
	Destination          string          `json:"destination"`
	SenderApp            string          `json:"sender_app"`
	MessageID            string          `json:"message_id"`
	SendState            string          `json:"send_state"`
	SentAt               time.Time       `json:"sent_at"`
	BaseCardRevision     int             `json:"base_card_revision"`
	BaseCardSnapshotHash string          `json:"base_card_snapshot_hash"`
	Members              []receiptMember `json:"members"`
}

type receiptMember struct {
	Fingerprint string    `json:"fingerprint"`
	StartsAt    time.Time `json:"starts_at"`
}

func (s *Service) buildReceipt(data *template.Data, receiver, destination, messageID, cardHash string) (deliveryReceipt, error) {
	deliveryID := strings.TrimSpace(data.DeliveryID)
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
		members = append(members, receiptMember{Fingerprint: fingerprint, StartsAt: alert.StartsAt.UTC()})
	}
	sort.Slice(members, func(i, j int) bool {
		if members[i].Fingerprint == members[j].Fingerprint {
			return members[i].StartsAt.Before(members[j].StartsAt)
		}
		return members[i].Fingerprint < members[j].Fingerprint
	})
	return deliveryReceipt{
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
	}, nil
}

// StampDeliveryID records the occurrence identity before notifier-specific
// clones or templates can change the channel payload. The webhook observation
// and every delivery receipt then carry the exact same identifier.
func (s *Service) StampDeliveryID(data *template.Data) error {
	deliveryID, err := deriveDeliveryID(data, s.config.ObservationReceiver)
	if err != nil {
		return err
	}
	data.DeliveryID = deliveryID
	return nil
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
	return value.Format("2006-01-02T15:04:05.999999+00:00")
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

func (s *Service) runReceiptWorker() {
	defer close(s.done)
	for {
		select {
		case <-s.stop:
			return
		case receipt := <-s.receipts:
			s.deliverReceipt(receipt)
		}
	}
}

func (s *Service) deliverReceipt(receipt deliveryReceipt) {
	for attempt := 1; attempt <= 3; attempt++ {
		if s.expired() {
			return
		}
		body, err := newRequestBody(receipt)
		if err != nil {
			_ = level.Error(s.logger).Log("msg", "Jev shadow failed to encode delivery receipt", "error", err)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.config.ReceiptURL, body)
		if err == nil {
			request.Header.Set("Authorization", "Bearer "+s.config.Token)
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Idempotency-Key", receipt.DeliveryID+":"+receipt.MessageID)
			var response *http.Response
			response, err = s.client.Do(request)
			if response != nil {
				_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
				_ = response.Body.Close()
				if response.StatusCode == http.StatusAccepted || response.StatusCode == http.StatusOK {
					cancel()
					return
				}
				err = fmt.Errorf("receipt endpoint returned %d", response.StatusCode)
			}
		}
		cancel()
		if attempt < 3 {
			time.Sleep(time.Duration(attempt) * 250 * time.Millisecond)
		}
		if attempt == 3 {
			_ = level.Error(s.logger).Log("msg", "Jev shadow delivery receipt failed", "deliveryID", receipt.DeliveryID, "error", err)
		}
	}
}
