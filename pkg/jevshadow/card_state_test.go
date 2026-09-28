package jevshadow

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/kubesphere/notification-manager/pkg/template"
)

func TestCardWriterRestoresStateAndIdempotencyAfterRestart(t *testing.T) {
	receiptServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer receiptServer.Close()
	directory := t.TempDir()
	config := persistentCardTestConfig(receiptServer.URL, directory)
	messageID := "om_persistent"

	first, err := New(log.NewNopLogger(), config, receiptServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	first.CaptureSuccessfulDelivery(
		persistentCardTestData(),
		"jev-shadow-feishu-uat",
		"oc_test",
		messageID,
		persistentCardBase(),
		func(context.Context, string, map[string]interface{}) error { return nil },
	)
	first.Close()

	second, err := New(log.NewNopLogger(), config, receiptServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	var patches atomic.Int32
	second.SetCardPatcherResolver(func(_ context.Context, receiver, destination string) (CardPatcher, error) {
		if receiver != "jev-shadow-feishu-uat" || destination != "oc_test" {
			t.Fatalf("unexpected restored writer identity: %s %s", receiver, destination)
		}
		return func(_ context.Context, gotMessageID string, _ map[string]interface{}) error {
			if gotMessageID != messageID {
				t.Fatalf("patched unexpected message %s", gotMessageID)
			}
			patches.Add(1)
			return nil
		}, nil
	})
	component := validV2Component()
	requestID, err := second.applyAnnotation(context.Background(), messageID, "persistent-idempotency", component)
	if err != nil {
		t.Fatal(err)
	}
	if requestID == "" || patches.Load() != 1 {
		t.Fatalf("restored writer did not patch exactly once: request=%q patches=%d", requestID, patches.Load())
	}
	state, err := second.cardStore.Load(messageID)
	if err != nil {
		t.Fatal(err)
	}
	if state.BaseSendState != baseSendConfirmed || state.PatchSendState != patchSendConfirmed || state.AnnotationRevision != 1 {
		t.Fatalf("unexpected persisted send state: %+v", state)
	}
	second.Close()

	third, err := New(log.NewNopLogger(), config, receiptServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	defer third.Close()
	third.SetCardPatcherResolver(func(context.Context, string, string) (CardPatcher, error) {
		return func(context.Context, string, map[string]interface{}) error {
			patches.Add(1)
			return nil
		}, nil
	})
	replayedRequestID, err := third.applyAnnotation(context.Background(), messageID, "persistent-idempotency", component)
	if err != nil {
		t.Fatal(err)
	}
	if replayedRequestID != requestID || patches.Load() != 1 {
		t.Fatalf("restart lost idempotency: request=%q patches=%d", replayedRequestID, patches.Load())
	}
}

func TestCardWriterUnknownPatchFailsClosedAcrossRestart(t *testing.T) {
	receiptServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer receiptServer.Close()
	directory := t.TempDir()
	config := persistentCardTestConfig(receiptServer.URL, directory)
	messageID := "om_unknown"
	first, err := New(log.NewNopLogger(), config, receiptServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	first.CaptureSuccessfulDelivery(
		persistentCardTestData(),
		"jev-shadow-feishu-uat",
		"oc_test",
		messageID,
		persistentCardBase(),
		func(context.Context, string, map[string]interface{}) error {
			return &CardPatchError{Outcome: CardPatchUnknown, Err: errors.New("connection reset after write")}
		},
	)
	component := validV2Component()
	if _, err := first.applyAnnotation(context.Background(), messageID, "unknown-patch-key", component); !errors.Is(err, ErrPatchStateUnknown) {
		t.Fatalf("unknown patch returned %v", err)
	}
	first.Close()

	second, err := New(log.NewNopLogger(), config, receiptServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	var patchCalls atomic.Int32
	second.SetCardPatcherResolver(func(context.Context, string, string) (CardPatcher, error) {
		return func(context.Context, string, map[string]interface{}) error {
			patchCalls.Add(1)
			return nil
		}, nil
	})
	if _, err := second.applyAnnotation(context.Background(), messageID, "unknown-patch-key", component); !errors.Is(err, ErrPatchStateUnknown) {
		t.Fatalf("restored unknown patch returned %v", err)
	}
	if patchCalls.Load() != 0 {
		t.Fatalf("unknown full-card patch was blindly replayed %d times", patchCalls.Load())
	}
	state, err := second.cardStore.Load(messageID)
	if err != nil {
		t.Fatal(err)
	}
	if state.PatchSendState != patchSendUnknown || state.PendingPatch == nil {
		t.Fatalf("unknown patch evidence was not retained: %+v", state)
	}
}

func TestCardWriterRecoversInterruptedSendingAsUnknown(t *testing.T) {
	receiptServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer receiptServer.Close()
	config := persistentCardTestConfig(receiptServer.URL, t.TempDir())
	messageID := "om_interrupted"
	first, err := New(log.NewNopLogger(), config, receiptServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	first.CaptureSuccessfulDelivery(
		persistentCardTestData(),
		"jev-shadow-feishu-uat",
		"oc_test",
		messageID,
		persistentCardBase(),
		func(context.Context, string, map[string]interface{}) error { return nil },
	)
	component := validV2Component()
	state, err := first.cardStore.Load(messageID)
	if err != nil {
		t.Fatal(err)
	}
	componentKey := stableComponentKey(component.MemberEventIDs)
	annotations := map[string]AnnotationComponent{componentKey: component}
	rendered, err := renderCard(state.BaseCard, annotations)
	if err != nil {
		t.Fatal(err)
	}
	payloadHash, _ := hashCanonical(component)
	renderedHash, _ := hashCanonical(rendered)
	state.PatchSendState = patchSendSending
	state.PendingPatch = &cardPatchIntent{
		IdempotencyKey:     "interrupted-patch",
		PayloadHash:        payloadHash,
		RequestID:          requestID(messageID, "interrupted-patch"),
		ComponentKey:       componentKey,
		Component:          component,
		RenderedCard:       rendered,
		RenderedCardHash:   renderedHash,
		AnnotationRevision: 1,
		StartedAt:          time.Now().UTC(),
	}
	if err := first.cardStore.Save(&state); err != nil {
		t.Fatal(err)
	}
	first.Close()

	second, err := New(log.NewNopLogger(), config, receiptServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	second.SetCardPatcherResolver(func(context.Context, string, string) (CardPatcher, error) {
		return func(context.Context, string, map[string]interface{}) error {
			t.Fatal("interrupted full-card patch must not be replayed")
			return nil
		}, nil
	})
	if _, err := second.applyAnnotation(context.Background(), messageID, "interrupted-patch", component); !errors.Is(err, ErrPatchStateUnknown) {
		t.Fatalf("interrupted patch returned %v", err)
	}
	recovered, err := second.cardStore.Load(messageID)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.PatchSendState != patchSendUnknown || recovered.PendingPatch == nil {
		t.Fatalf("interrupted patch was not persisted as unknown: %+v", recovered)
	}
}

func TestCardWriterKnownFailureCanRetry(t *testing.T) {
	receiptServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer receiptServer.Close()
	config := persistentCardTestConfig(receiptServer.URL, t.TempDir())
	service, err := New(log.NewNopLogger(), config, receiptServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	var attempts atomic.Int32
	service.CaptureSuccessfulDelivery(
		persistentCardTestData(),
		"jev-shadow-feishu-uat",
		"oc_test",
		"om_retry",
		persistentCardBase(),
		func(context.Context, string, map[string]interface{}) error {
			if attempts.Add(1) == 1 {
				return &CardPatchError{Outcome: CardPatchKnownFailed, Err: errors.New("request rejected before update")}
			}
			return nil
		},
	)
	component := validV2Component()
	if _, err := service.applyAnnotation(context.Background(), "om_retry", "known-failure-key", component); err == nil || errors.Is(err, ErrPatchStateUnknown) {
		t.Fatalf("known failure returned %v", err)
	}
	if _, err := service.applyAnnotation(context.Background(), "om_retry", "known-failure-key", component); err != nil {
		t.Fatalf("known failed patch could not retry: %v", err)
	}
	if attempts.Load() != 2 {
		t.Fatalf("known failure used %d attempts, want 2", attempts.Load())
	}
}

func TestCardStateStoreSerializesAcrossInstances(t *testing.T) {
	directory := t.TempDir()
	first, err := newCardStateStore(log.NewNopLogger(), directory)
	if err != nil {
		t.Fatal(err)
	}
	second, err := newCardStateStore(log.NewNopLogger(), directory)
	if err != nil {
		t.Fatal(err)
	}
	locked := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		finished <- first.WithMessageLock("om_lock", func() error {
			close(locked)
			<-release
			return nil
		})
	}()
	<-locked
	if err := second.WithMessageLock("om_lock", func() error { return nil }); !errors.Is(err, ErrCardWriterBusy) {
		t.Fatalf("concurrent writer returned %v, want busy", err)
	}
	close(release)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
}

func TestCardStateStoreQuarantinesCorruption(t *testing.T) {
	directory := t.TempDir()
	store, err := newCardStateStore(log.NewNopLogger(), directory)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(store.stateDir, "corrupt.json"), []byte("not-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if states := store.LoadRecent(10); len(states) != 0 {
		t.Fatalf("corrupt state was loaded: %+v", states)
	}
	if count := receiptFileCount(t, store.deadDir); count != 1 {
		t.Fatalf("corrupt state was not quarantined: %d", count)
	}
}

func TestCardStateDefaultLivesUnderReceiptOutbox(t *testing.T) {
	root := filepath.Join(t.TempDir(), "durable")
	config := (Config{ReceiptOutboxDir: root}).withReceiptDefaults()
	if want := filepath.Join(root, "card-state"); config.CardStateDir != want {
		t.Fatalf("card state default is %q, want %q", config.CardStateDir, want)
	}
}

func persistentCardTestConfig(receiptURL, directory string) Config {
	config := testConfig(receiptURL)
	config.ReceiptOutboxDir = filepath.Join(directory, "receipts")
	config.CardStateDir = filepath.Join(directory, "cards")
	return config
}

func persistentCardTestData() *template.Data {
	return &template.Data{
		GroupLabels: template.KV{"alertname": "Watchdog"},
		Alerts: template.Alerts{
			&template.Alert{
				ID:       "persistent-alert",
				Status:   "firing",
				Labels:   template.KV{"alertname": "Watchdog"},
				StartsAt: time.Date(2026, 9, 28, 1, 2, 3, 0, time.UTC),
			},
		},
	}
}

func persistentCardBase() map[string]interface{} {
	return map[string]interface{}{
		"config": map[string]interface{}{"update_multi": true},
		"elements": []interface{}{
			map[string]interface{}{"tag": "div", "text": map[string]interface{}{"tag": "plain_text", "content": "original"}},
		},
	}
}
