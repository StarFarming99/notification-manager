package jevshadow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
)

func TestPatchGlobalBudgetHeaderIdempotencyAndRestart(t *testing.T) {
	cfg := persistentCardTestConfig("http://unused.invalid/receipts", t.TempDir())
	cfg.PatchInterval = 12 * time.Second
	var calls atomic.Int32
	patch := func(context.Context, string, map[string]interface{}) error { calls.Add(1); return nil }
	s, err := New(log.NewNopLogger(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, mid := range []string{"om-one", "om-two"} {
		if err := s.captureCardBinding("jev-shadow-feishu-uat", "oc_test", mid, persistentCardBase(), patch); err != nil {
			t.Fatal(err)
		}
	}
	request := func(mid, key, header string, component AnnotationComponent) *httptest.ResponseRecorder {
		body, _ := json.Marshal(component)
		r := httptest.NewRequest(http.MethodPut, "/internal/jev/annotations/"+mid, bytes.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+cfg.Token)
		r.Header.Set("Idempotency-Key", key)
		if header != "" {
			r.Header.Set("X-Jev-Min-Update-Interval-Seconds", header)
		}
		w := httptest.NewRecorder()
		s.HandleAnnotation(w, r, mid)
		return w
	}
	c := validV2Component()
	if w := request("om-one", "budget-first", "40", c); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	c.AnnotationRevision = 2
	start := time.Now()
	if w := request("om-one", "budget-second", "15", c); w.Code != 429 || w.Header().Get("Retry-After") != "40" {
		t.Fatal("same MID header ignored", w.Code, w.Header(), w.Body.String())
	}
	if time.Since(start) > time.Second {
		t.Fatal("budget handler blocked")
	}
	if w := request("om-two", "budget-other", "15", validV2Component()); w.Code != 429 {
		t.Fatal("separate MID bypassed global gate", w.Code)
	}
	if w := request("om-one", "budget-first", "40", validV2Component()); w.Code != 200 || calls.Load() != 1 {
		t.Fatal("applied idempotency consumed new budget", w.Code, calls.Load())
	}
	state, _ := s.cardStore.Load("om-one")
	if state.AnnotationRevision != 1 || state.PendingPatch != nil {
		t.Fatal("throttle created external intent", state)
	}
	s.Close()
	s, err = New(log.NewNopLogger(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.SetCardPatcherResolver(func(context.Context, string, string) (CardPatcher, error) { return patch, nil })
	if w := request("om-two", "budget-other", "15", validV2Component()); w.Code != 429 || calls.Load() != 1 {
		t.Fatal("restart reset global PATCH budget", w.Code, calls.Load())
	}
}

func TestKnownPatchFailureRetryConsumesPersistedBudget(t *testing.T) {
	cfg := persistentCardTestConfig("http://unused.invalid/receipts", t.TempDir())
	cfg.PatchInterval = 12 * time.Second
	s, err := New(log.NewNopLogger(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var calls atomic.Int32
	patch := func(context.Context, string, map[string]interface{}) error {
		calls.Add(1)
		return &CardPatchError{Outcome: CardPatchKnownFailed, Err: errors.New("mock 429")}
	}
	if err := s.captureCardBinding("jev-shadow-feishu-uat", "oc_test", "om-retry", persistentCardBase(), patch); err != nil {
		t.Fatal(err)
	}
	if _, err := s.applyAnnotation(context.Background(), "om-retry", "retry-budget-key", validV2Component()); err == nil {
		t.Fatal("missing rejection")
	}
	var deferred *patchBudgetDeferred
	if _, err := s.applyAnnotation(context.Background(), "om-retry", "retry-budget-key", validV2Component()); !errors.As(err, &deferred) || calls.Load() != 1 {
		t.Fatal("platform retry bypassed budget", err, calls.Load())
	}
}

func TestWriterPauseIsDurableCASAndDrainsInFlight(t *testing.T) {
	cfg := persistentCardTestConfig("http://unused.invalid/receipts", t.TempDir())
	token := strings.Repeat("w", 32)
	s, err := New(log.NewNopLogger(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	started, release, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		finished <- s.writerGate.withAttempt(context.Background(), "om-active", func() error { close(started); <-release; return nil })
	}()
	<-started
	control := func(action string, revision uint64, domain string) *httptest.ResponseRecorder {
		body, _ := json.Marshal(map[string]interface{}{"action": action, "expected_revision": revision, "permission_domain": domain, "actor": "operator", "evidence": "AP withdraw"})
		r := httptest.NewRequest(http.MethodPost, "/internal/jev/writer-control", bytes.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		s.HandleWriterControl(token)(w, r)
		return w
	}
	paused := make(chan *httptest.ResponseRecorder, 1)
	go func() { paused <- control("pause", 1, cfg.PermissionDomain) }()
	select {
	case <-paused:
		t.Fatal("pause bypassed in-flight PATCH")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if w := <-paused; w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := control("resume", 1, cfg.PermissionDomain); w.Code != 409 {
		t.Fatal("stale CAS admitted", w.Code)
	}
	if w := control("resume", 2, "wrong-domain"); w.Code != 400 {
		t.Fatal("wrong domain admitted", w.Code)
	}
	s.Close()
	s, err = New(log.NewNopLogger(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	called := false
	if err := s.writerGate.withAttempt(context.Background(), "om-next", func() error { called = true; return nil }); !errors.Is(err, ErrPatchWriterPaused) || called {
		t.Fatal("restart reset writer pause", err)
	}
	if w := control("resume", 2, cfg.PermissionDomain); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if err := s.writerGate.withAttempt(context.Background(), "om-next", func() error { called = true; return nil }); err != nil || !called {
		t.Fatal("resumed writer stayed closed", err)
	}
}

func TestRegionalPauseLeavesOtherDomainsRunning(t *testing.T) {
	gates := map[string]*patchWriterGate{}
	for _, domain := range []string{"us", "ap", "eu"} {
		gate, err := newPatchWriterGate(t.TempDir(), 12*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		gates[domain] = gate
	}
	gates["ap"].state.Paused = true
	if err := gates["ap"].persist(); err != nil {
		t.Fatal(err)
	}
	for domain, gate := range gates {
		called := false
		err := gate.withAttempt(context.Background(), "shared-app-shared-chat-same-mid", func() error { called = true; return nil })
		if domain == "ap" {
			if !errors.Is(err, ErrPatchWriterPaused) || called {
				t.Fatal("AP writer did not stop", err)
			}
		} else if err != nil || !called {
			t.Fatal("AP pause affected other regional writer", domain, err)
		}
	}
}
