package jevshadow

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
)

func TestReceiptOutboxRecoversPendingEntryAfterRestart(t *testing.T) {
	directory := t.TempDir()
	config := receiptOutboxTestConfig(directory)
	config.ReceiptRetryMin = 250 * time.Millisecond
	config.ReceiptRetryMax = 250 * time.Millisecond
	config.ReceiptDrainTimeout = 100 * time.Millisecond

	firstAttempt := make(chan struct{}, 1)
	first, err := newReceiptOutbox(log.NewNopLogger(), config, func(context.Context, deliveryReceipt) error {
		select {
		case firstAttempt <- struct{}{}:
		default:
		}
		return errors.New("temporary outage")
	})
	if err != nil {
		t.Fatal(err)
	}
	receipt := testDeliveryReceipt("delivery-restart", "message-restart")
	if err := first.Enqueue(receipt); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstAttempt:
	case <-time.After(time.Second):
		t.Fatal("first outbox did not attempt the receipt")
	}
	first.Close()
	if count := receiptFileCount(t, filepath.Join(directory, "pending")); count != 1 {
		t.Fatalf("pending receipt was not retained at shutdown: got %d files", count)
	}

	delivered := make(chan deliveryReceipt, 1)
	second, err := newReceiptOutbox(log.NewNopLogger(), config, func(_ context.Context, receipt deliveryReceipt) error {
		delivered <- receipt
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-delivered:
		if got.DeliveryID != receipt.DeliveryID || got.MessageID != receipt.MessageID {
			t.Fatalf("recovered the wrong receipt: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("restarted outbox did not recover the pending receipt")
	}
	waitForReceiptFiles(t, filepath.Join(directory, "pending"), 0)
	second.Close()
}

func TestReceiptOutboxDoesNotDropWhenManyReceiptsArrive(t *testing.T) {
	directory := t.TempDir()
	config := receiptOutboxTestConfig(directory)
	config.ReceiptMaxAttempts = 100
	config.ReceiptRetryMin = time.Hour
	config.ReceiptRetryMax = time.Hour
	config.ReceiptDrainTimeout = time.Second
	outbox, err := newReceiptOutbox(log.NewNopLogger(), config, func(context.Context, deliveryReceipt) error {
		return errors.New("endpoint unavailable")
	})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 64; index++ {
		receipt := testDeliveryReceipt(
			"delivery-pressure-"+time.Unix(int64(index), 0).Format("150405"),
			"message-pressure-"+time.Unix(int64(index), 0).Format("150405"),
		)
		if err := outbox.Enqueue(receipt); err != nil {
			t.Fatalf("enqueue receipt %d: %v", index, err)
		}
	}
	outbox.Close()
	if count := receiptFileCount(t, filepath.Join(directory, "pending")); count != 64 {
		t.Fatalf("durable outbox dropped receipts under pressure: got %d, want 64", count)
	}
	if count := receiptFileCount(t, filepath.Join(directory, "dead-letter")); count != 0 {
		t.Fatalf("retryable receipts were unexpectedly dead-lettered: %d", count)
	}
}

func TestReceiptOutboxDeadLettersPermanentFailure(t *testing.T) {
	directory := t.TempDir()
	config := receiptOutboxTestConfig(directory)
	outbox, err := newReceiptOutbox(log.NewNopLogger(), config, func(context.Context, deliveryReceipt) error {
		return &receiptDeliveryFailure{err: errors.New("unauthorized"), permanent: true}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := outbox.Enqueue(testDeliveryReceipt("delivery-dead", "message-dead")); err != nil {
		t.Fatal(err)
	}
	waitForReceiptFiles(t, filepath.Join(directory, "dead-letter"), 1)
	if count := receiptFileCount(t, filepath.Join(directory, "pending")); count != 0 {
		t.Fatalf("dead-lettered receipt remained pending: %d", count)
	}
	outbox.Close()
}

func TestReceiptOutboxDeadLettersAfterRetryBudget(t *testing.T) {
	directory := t.TempDir()
	config := receiptOutboxTestConfig(directory)
	config.ReceiptMaxAttempts = 3
	var attempts atomic.Int32
	outbox, err := newReceiptOutbox(log.NewNopLogger(), config, func(context.Context, deliveryReceipt) error {
		attempts.Add(1)
		return errors.New("temporary outage")
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := outbox.Enqueue(testDeliveryReceipt("delivery-budget", "message-budget")); err != nil {
		t.Fatal(err)
	}
	waitForReceiptFiles(t, filepath.Join(directory, "dead-letter"), 1)
	if got := attempts.Load(); got != 3 {
		t.Fatalf("receipt used %d attempts, want 3", got)
	}
	if count := receiptFileCount(t, filepath.Join(directory, "pending")); count != 0 {
		t.Fatalf("exhausted receipt remained pending: %d", count)
	}
	outbox.Close()
}

func TestReceiptOutboxCloseDrainsPendingReceipt(t *testing.T) {
	directory := t.TempDir()
	config := receiptOutboxTestConfig(directory)
	config.ReceiptRetryMin = time.Hour
	config.ReceiptRetryMax = time.Hour
	var delivered atomic.Int32
	firstAttempt := make(chan struct{})
	outbox, err := newReceiptOutbox(log.NewNopLogger(), config, func(context.Context, deliveryReceipt) error {
		if delivered.Add(1) == 1 {
			close(firstAttempt)
			return errors.New("temporary outage")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := outbox.Enqueue(testDeliveryReceipt("delivery-drain", "message-drain")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstAttempt:
	case <-time.After(time.Second):
		t.Fatal("outbox did not make the first delivery attempt")
	}
	outbox.Close()
	if got := delivered.Load(); got != 2 {
		t.Fatalf("graceful close made %d delivery attempts, want initial plus drain", got)
	}
	if count := receiptFileCount(t, filepath.Join(directory, "pending")); count != 0 {
		t.Fatalf("successfully drained receipt remained pending: %d", count)
	}
	if err := outbox.Enqueue(testDeliveryReceipt("delivery-late", "message-late")); !errors.Is(err, errReceiptOutboxClosing) {
		t.Fatalf("enqueue after close returned %v, want closing error", err)
	}
}

func TestReceiptOutboxQuarantinesCorruptEntry(t *testing.T) {
	directory := t.TempDir()
	pending := filepath.Join(directory, "pending")
	if err := os.MkdirAll(pending, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pending, "corrupt.json"), []byte("not-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	outbox, err := newReceiptOutbox(log.NewNopLogger(), receiptOutboxTestConfig(directory), func(context.Context, deliveryReceipt) error {
		t.Fatal("corrupt receipt must not be delivered")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForReceiptFiles(t, filepath.Join(directory, "dead-letter"), 1)
	if count := receiptFileCount(t, pending); count != 0 {
		t.Fatalf("corrupt receipt remained pending: %d", count)
	}
	outbox.Close()
}

func TestReceiptOutboxInitializationFailureDisablesOnlyShadowAdapter(t *testing.T) {
	parent := t.TempDir()
	file := filepath.Join(parent, "not-a-directory")
	if err := os.WriteFile(file, []byte("occupied"), 0o600); err != nil {
		t.Fatal(err)
	}
	config := testConfig("https://jev.example.com/v1/delivery-receipts")
	config.ReceiptOutboxDir = filepath.Join(file, "outbox")
	service, err := New(log.NewNopLogger(), config, nil)
	if err != nil {
		t.Fatalf("receipt storage failure escaped into the notification-manager startup path: %v", err)
	}
	defer service.Close()
	if service.Enabled() {
		t.Fatal("adapter remained enabled without a durable receipt outbox")
	}
}

func receiptOutboxTestConfig(directory string) Config {
	return Config{
		ReceiptOutboxDir:    directory,
		ReceiptMaxAttempts:  5,
		ReceiptRetryMin:     10 * time.Millisecond,
		ReceiptRetryMax:     100 * time.Millisecond,
		ReceiptDrainTimeout: time.Second,
	}
}

func testDeliveryReceipt(deliveryID, messageID string) deliveryReceipt {
	return deliveryReceipt{
		SchemaVersion:    "1",
		LogicalSource:    "notification-manager-uat",
		Environment:      "uat",
		SourceRegion:     "us-west-2",
		PermissionDomain: "sre-uat",
		Receiver:         "jev-shadow-uat",
		DeliveryID:       deliveryID,
		Destination:      "oc_test",
		SenderApp:        "infra-alerts",
		MessageID:        messageID,
		SendState:        "succeeded",
		SentAt:           time.Now().UTC(),
		Members: []receiptMember{
			{Fingerprint: "fingerprint", StartsAt: time.Now().UTC()},
		},
	}
}

func receiptFileCount(t *testing.T, directory string) int {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".json" {
			count++
		}
	}
	return count
}

func waitForReceiptFiles(t *testing.T, directory string, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if receiptFileCount(t, directory) == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d receipt files in %s", want, directory)
}
