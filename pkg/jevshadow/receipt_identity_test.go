package jevshadow

import (
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/kubesphere/notification-manager/pkg/template"
)

func TestDeliveryAttemptIdentitySurvivesDifferentChannelMemberSets(t *testing.T) {
	service := &Service{
		config: Config{ObservationReceiver: "jev-shadow-uat-receiver"},
		logger: log.NewNopLogger(),
	}
	dispatchTime := time.Date(2026, 9, 28, 8, 9, 10, 123456000, time.UTC)
	startsAt := time.Date(2026, 9, 28, 8, 0, 0, 0, time.UTC)
	memberA := &template.Alert{
		ID:               "fingerprint-a",
		Status:           "firing",
		Labels:           template.KV{"alertname": "NodeMemoryHigh", "node": "node-a"},
		StartsAt:         startsAt,
		NotificationTime: dispatchTime,
	}
	memberB := &template.Alert{
		ID:               "fingerprint-b",
		Status:           "firing",
		Labels:           template.KV{"alertname": "NodeMemoryHigh", "node": "node-b"},
		StartsAt:         startsAt.Add(time.Second),
		NotificationTime: dispatchTime,
	}
	webhookData := &template.Data{
		GroupLabels: template.KV{"alertname": "NodeMemoryHigh"},
		Alerts:      template.Alerts{memberA.Clone(), memberB.Clone()},
	}
	feishuData := &template.Data{
		GroupLabels: template.KV{"alertname": "NodeMemoryHigh"},
		Alerts:      template.Alerts{memberA.Clone()},
	}

	const attemptID = "5f9c6ca0ddf54d63a315d2dcf4e113ac"
	if err := service.StampDeliveryIDForAttempt(webhookData, attemptID); err != nil {
		t.Fatal(err)
	}
	if err := service.StampDeliveryIDForAttempt(feishuData, attemptID); err != nil {
		t.Fatal(err)
	}
	if webhookData.DeliveryID != feishuData.DeliveryID {
		t.Fatalf(
			"same fanout got channel-specific delivery IDs: webhook=%s feishu=%s",
			webhookData.DeliveryID,
			feishuData.DeliveryID,
		)
	}

	receipt, err := service.buildReceipt(
		feishuData,
		"jev-shadow-uat-receiver",
		"oc_test",
		"om_test",
		"0123456789abcdef",
	)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.DeliveryID != webhookData.DeliveryID {
		t.Fatalf("receipt lost the common fanout identity: %+v", receipt)
	}
	if len(receipt.Members) != 1 || receipt.Members[0].Fingerprint != memberA.ID ||
		!receipt.Members[0].StartsAt.Equal(memberA.StartsAt) {
		t.Fatalf("receipt did not preserve the actual Feishu member scope: %+v", receipt.Members)
	}
}

func TestDeliveryAttemptIdentitySeparatesRepeatsAndGroups(t *testing.T) {
	service := &Service{config: Config{ObservationReceiver: "jev-shadow-uat-receiver"}}
	data := &template.Data{
		GroupLabels: template.KV{"alertname": "NodeMemoryHigh"},
		Alerts: template.Alerts{&template.Alert{
			ID:       "fingerprint-a",
			Status:   "firing",
			Labels:   template.KV{"alertname": "NodeMemoryHigh"},
			StartsAt: time.Date(2026, 9, 28, 8, 0, 0, 0, time.UTC),
		}},
	}
	first := data.Clone()
	if err := service.StampDeliveryIDForAttempt(first, "attempt-one"); err != nil {
		t.Fatal(err)
	}
	repeat := data.Clone()
	if err := service.StampDeliveryIDForAttempt(repeat, "attempt-two"); err != nil {
		t.Fatal(err)
	}
	if first.DeliveryID == repeat.DeliveryID {
		t.Fatal("a later notification attempt reused the previous delivery identity")
	}

	otherGroup := data.Clone()
	otherGroup.GroupLabels = template.KV{"alertname": "NodeDiskHigh"}
	if err := service.StampDeliveryIDForAttempt(otherGroup, "attempt-one"); err != nil {
		t.Fatal(err)
	}
	if first.DeliveryID == otherGroup.DeliveryID {
		t.Fatal("different groups in one fanout shared a delivery identity")
	}
	if err := service.StampDeliveryIDForAttempt(data.Clone(), " "); err == nil {
		t.Fatal("blank delivery attempt identity was accepted")
	}
}
