package jevshadow

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/kubesphere/notification-manager/pkg/template"
)

type observationAlert struct {
	Fingerprint string            `json:"fingerprint"`
	Status      string            `json:"status"`
	StartsAt    time.Time         `json:"starts_at"`
	EndsAt      *time.Time        `json:"ends_at,omitempty"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
	SourceID    string            `json:"source_id"`
}
type observationWire struct {
	SchemaVersion    string             `json:"schema_version"`
	IdentityVersion  string             `json:"identity_version"`
	LogicalSource    string             `json:"logical_source"`
	Environment      string             `json:"environment"`
	SourceRegion     string             `json:"source_region"`
	PermissionDomain string             `json:"permission_domain"`
	Receiver         string             `json:"receiver"`
	DeliveryID       string             `json:"delivery_id"`
	ObservedAt       time.Time          `json:"observed_at"`
	GroupKey         string             `json:"group_key"`
	Alerts           []observationAlert `json:"alerts"`
}

func (s *Service) observation(data *template.Data, receipt deliveryReceipt) *observationWire {
	o := &observationWire{SchemaVersion: "1", IdentityVersion: "2", LogicalSource: s.config.LogicalSource, Environment: s.config.Environment, SourceRegion: s.config.SourceRegion, PermissionDomain: s.config.PermissionDomain, Receiver: receipt.Receiver, DeliveryID: receipt.DeliveryID, ObservedAt: receipt.SentAt, GroupKey: ""}
	groupJSON, _ := canonicalJSON(data.GroupLabels)
	o.GroupKey = framedDigest("group-v2", string(groupJSON))
	for _, a := range data.Alerts {
		var end *time.Time
		if a.Status == "resolved" && !a.EndsAt.Before(a.StartsAt) {
			v := a.EndsAt.UTC()
			end = &v
		}
		o.Alerts = append(o.Alerts, observationAlert{Fingerprint: a.ID, Status: a.Status, StartsAt: a.StartsAt.UTC(), EndsAt: end, Labels: map[string]string(a.Labels), Annotations: map[string]string(a.Annotations), SourceID: s.config.LogicalSource})
	}
	return o
}

func (s *Service) postObservation(parent context.Context, observation *observationWire, messageID string) error {
	body, err := newRequestBody(observation)
	if err != nil {
		return &receiptDeliveryFailure{err: err, permanent: true}
	}
	ctx, cancel := context.WithTimeout(parent, receiptRequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.config.ObservationURL, body)
	if err != nil {
		return &receiptDeliveryFailure{err: err, permanent: true}
	}
	req.Header.Set("Authorization", "Bearer "+s.config.Token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "executor-observation:"+observation.DeliveryID+":"+messageID)
	resp, err := s.client.Do(req)
	if err != nil {
		return &receiptDeliveryFailure{err: err}
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return &receiptDeliveryFailure{err: fmt.Errorf("observation endpoint returned %d", resp.StatusCode), permanent: resp.StatusCode == 400 || resp.StatusCode == 401 || resp.StatusCode == 403 || resp.StatusCode == 422}
}
