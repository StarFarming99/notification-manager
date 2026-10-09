package jevshadow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
)

func TestCloseFsyncsEveryAcceptedRelayGapBeforeReturn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gaps.jsonl")
	t.Setenv("JEV_RELAY_GAP_PATH", path)
	transport := blockedRelayTransport{entered: make(chan struct{}, 1)}
	s := newRelay(log.NewNopLogger(), testConfig("http://jev.invalid"), "http://executor.invalid", "synthetic", &http.Client{Transport: transport})
	s.CaptureSuccessfulDelivery(persistentCardTestData(), "jev-shadow-feishu-uat", "oc_test", "first", persistentCardBase(), nil)
	select {
	case <-transport.entered:
	case <-time.After(time.Second):
		t.Fatal("worker did not start")
	}
	for i := 0; i < 100; i++ {
		s.CaptureSuccessfulDelivery(persistentCardTestData(), "jev-shadow-feishu-uat", "oc_test", "queued", persistentCardBase(), nil)
	}
	s.Close()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if records := len(strings.Split(strings.TrimSpace(string(raw)), "\n")); records != 101 {
		t.Fatalf("Close returned before fsync: records=%d", records)
	}
	if s.RelayStatus()["failed"] != 101 || !s.RelaySnapshot()["reconciliation_complete"].(bool) {
		t.Fatal(s.RelaySnapshot())
	}
	s.CaptureSuccessfulDelivery(persistentCardTestData(), "jev-shadow-feishu-uat", "oc_test", "after-close", persistentCardBase(), nil)
	if s.RelayStatus()["accepted"] != 101 {
		t.Fatal("accepted after shutdown")
	}
}

func TestRelayShutdownBudgetMarksIncompleteReconciliation(t *testing.T) {
	r := &successRelay{queue: make(chan SuccessfulDelivery), gaps: make(chan RelayGap), gapDone: make(chan struct{}), cancel: func() {}, logger: log.NewNopLogger(), journalPath: "/synthetic/not-written"}
	s := &Service{relay: r}
	start := time.Now()
	r.shutdown(20 * time.Millisecond)
	if time.Since(start) > time.Second {
		t.Fatal("extension delayed original shutdown")
	}
	if s.RelayStatus()["shutdown_incomplete"] != 1 || s.RelaySnapshot()["reconciliation_complete"].(bool) {
		t.Fatal("shutdown timeout reported complete")
	}
}

type blockedRelayTransport struct{ entered chan struct{} }

func (b blockedRelayTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	select {
	case b.entered <- struct{}{}:
	default:
	}
	<-r.Context().Done()
	return nil, r.Context().Err()
}

func TestSuccessHookBoundedWhileExecutorUnavailable(t *testing.T) {
	transport := blockedRelayTransport{entered: make(chan struct{}, 1)}
	config := testConfig("http://jev.invalid")
	config.ReceiptOutboxDir = filepath.Join(t.TempDir(), "must-not-exist")
	s := newRelay(log.NewNopLogger(), config, "http://executor.invalid", "relay-test-only", &http.Client{Transport: transport})
	defer s.Close()
	data, card := persistentCardTestData(), persistentCardBase()
	s.CaptureSuccessfulDelivery(data, "jev-shadow-feishu-uat", "oc_test", "om_first", card, nil)
	select {
	case <-transport.entered:
	case <-time.After(time.Second):
		t.Fatal("worker did not start")
	}
	samples := make([]time.Duration, 10000)
	for i := range samples {
		start := time.Now()
		s.CaptureSuccessfulDelivery(data, "jev-shadow-feishu-uat", "oc_test", "om_test", card, nil)
		samples[i] = time.Since(start)
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	if samples[9900] >= 5*time.Millisecond {
		t.Fatalf("hook P99 exceeded 5ms: %s", samples[9900])
	}
	status := s.RelayStatus()
	if status["queued"] != relayCapacity || status["dropped"] == 0 {
		t.Fatalf("queue not bounded: %v", status)
	}
	if allocations := testing.AllocsPerRun(1000, func() { s.CaptureSuccessfulDelivery(data, "jev-shadow-feishu-uat", "oc_test", "om_test", card, nil) }); allocations != 0 {
		t.Fatalf("hook allocated: %f", allocations)
	}
	if _, err := os.Stat(config.ReceiptOutboxDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("NM relay accessed receipt disk")
	}
	if s.FeedbackEnabled() {
		t.Fatal("NM relay started feedback")
	}
	t.Logf("10000 hooks: P99=%s; queued=%d dropped=%d", samples[9900], status["queued"], status["dropped"])
}

func TestRelayGapRecordsHaveOwnershipAndDurableJournal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gaps.jsonl")
	t.Setenv("JEV_RELAY_GAP_PATH", path)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer server.Close()
	s := newRelay(log.NewNopLogger(), testConfig("http://jev.invalid"), server.URL, "synthetic", server.Client())
	defer s.Close()
	s.CaptureSuccessfulDelivery(persistentCardTestData(), "jev-shadow-feishu-uat", "oc_test", "om_gap", persistentCardBase(), nil)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if raw, err := os.ReadFile(path); err == nil {
			var gap RelayGap
			if json.Unmarshal(bytes.TrimSpace(raw), &gap) != nil {
				time.Sleep(time.Millisecond)
				continue
			}
			if gap.MessageID != "om_gap" || gap.Receiver != "jev-shadow-feishu-uat" || gap.Destination != "oc_test" || gap.Instance == "" {
				t.Fatalf("missing reconciliation keys: %+v", gap)
			}
			if s.RelayStatus()["failed"] != 1 || len(s.RelaySnapshot()["recent_gaps"].([]RelayGap)) != 1 {
				t.Fatal("gap invisible in status")
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("executor rejection gap was not persisted")
}

func TestExecutorAcknowledgesDurableBindingAndRejectsUntrustedIdentity(t *testing.T) {
	receiptServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusAccepted) }))
	defer receiptServer.Close()
	config := persistentCardTestConfig(receiptServer.URL, t.TempDir())
	s, err := New(log.NewNopLogger(), config, receiptServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.SetCardPatcherResolver(func(context.Context, string, string) (CardPatcher, error) {
		return func(context.Context, string, map[string]interface{}) error { return nil }, nil
	})
	token := "executor-relay-test-credential-32-bytes"
	handler := s.HandleSuccessfulDelivery(token)
	delivery := SuccessfulDelivery{Data: persistentCardTestData(), Receiver: "jev-shadow-feishu-uat", Destination: "oc_test", MessageID: "om_executor", BaseCard: persistentCardBase(), SenderApp: config.SenderApp}
	call := func(authorization string, payload SuccessfulDelivery) int {
		body, _ := json.Marshal(payload)
		req := httptest.NewRequest(http.MethodPost, "/internal/jev/successful-deliveries", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+authorization)
		res := httptest.NewRecorder()
		handler(res, req)
		return res.Code
	}
	if call("forged", delivery) != http.StatusUnauthorized {
		t.Fatal("forged service accepted")
	}
	forged := delivery
	forged.SenderApp = "other-app"
	if call(token, forged) != http.StatusBadRequest {
		t.Fatal("wrong owner accepted")
	}
	if call(token, delivery) != http.StatusAccepted {
		t.Fatal("durable receipt rejected")
	}
	if _, err := s.cardStore.Load(delivery.MessageID); err != nil {
		t.Fatal("accepted without durable binding", err)
	}
	// A missing writer/store must never be acknowledged as accepted.
	s.outbox.Close()
	s.outbox = nil
	delivery.MessageID = "om_unavailable"
	if call(token, delivery) != http.StatusServiceUnavailable {
		t.Fatal("unavailable outbox acknowledged")
	}
}

func TestNMRelayConfigurationDoesNotRequireJevOrFeedbackSecrets(t *testing.T) {
	t.Setenv(envEnabled, "true")
	t.Setenv(envSenderApp, "test-app")
	t.Setenv(envReceiverAllowlist, "test-receiver")
	t.Setenv(envDestinationAllowlist, "test-chat")
	t.Setenv("JEV_EXECUTOR_SUCCESS_URL", "http://executor.invalid/internal/jev/successful-deliveries")
	t.Setenv("JEV_EXECUTOR_TOKEN", "relay-test-only-credential-of-32-bytes")
	t.Setenv(envReceiptURL, ":bad-config")
	t.Setenv(envToken, "")
	t.Setenv(envFeedbackEnabled, "invalid-feedback-config")
	s, err := NewNMFromEnv(log.NewNopLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if !s.IsRelay() || !s.Enabled() || s.FeedbackEnabled() {
		t.Fatal("extension isolation failed")
	}
}
