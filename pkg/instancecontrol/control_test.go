package instancecontrol

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	spool "github.com/kubesphere/notification-manager/pkg/notification_spool"
)

func fixture(t *testing.T, mode string) (*spool.Store, *Manager) {
	t.Helper()
	db, err := spool.Open(filepath.Join(t.TempDir(), "ledger.db"), spool.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m, err := New(db, "blue", mode, strings.Repeat("r", 32), strings.Repeat("c", 32), []byte(strings.Repeat("p", 32)))
	if err != nil {
		t.Fatal(err)
	}
	return db, m
}

func call(handler http.HandlerFunc, method, token, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "/internal/instance", strings.NewReader(body))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	handler(w, r)
	return w
}

func TestIndependentReadControlAndTransferProtection(t *testing.T) {
	db, m := fixture(t, "active")
	for _, token := range []string{"", strings.Repeat("c", 32), strings.Repeat("p", 32)} {
		if w := call(m.ReadGuard(m.Status), "GET", token, ""); w.Code != 401 {
			t.Fatal("read credential boundary", w.Code)
		}
	}
	w := call(m.ReadGuard(m.Status), "GET", strings.Repeat("r", 32), "")
	if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, action := range []http.HandlerFunc{m.Control, m.Export, m.Import} {
		for _, token := range []string{"", strings.Repeat("r", 32), strings.Repeat("p", 32)} {
			if w := call(m.ControlGuard(action), "POST", token, `{}`); w.Code != 401 {
				t.Fatal("read/proof identity mutated or exported state", w.Code)
			}
		}
	}
	// Transfer payloads can contain encrypted credential envelopes. GET exposes
	// only aggregate ledger counts and release identity, never the ledger rows.
	secretMarker := "fixture-private-transfer-marker"
	_, _, err := db.Submit("fixture", "hash", []spool.Target{{Receiver: "fixture", Channel: "webhook", Destination: "fixture", ContentRevision: "revision", Payload: json.RawMessage(`{"opaque":"` + secretMarker + `"}`)}}, "")
	if err != nil {
		t.Fatal(err)
	}
	w = call(m.ReadGuard(m.Status), "GET", strings.Repeat("r", 32), "")
	for _, forbidden := range []string{secretMarker, strings.Repeat("p", 32), strings.Repeat("c", 32), "key_fingerprint", `"payload"`} {
		if strings.Contains(w.Body.String(), forbidden) {
			t.Fatal("status exposed transfer or credentials", forbidden)
		}
	}
}

func TestControlRejectsUnknownFieldsAndGETReconcilesLostResponse(t *testing.T) {
	db, m := fixture(t, "active")
	command := spool.ReleaseCommand{OperationID: "operation-1", ExpectedRevision: 1, Action: "begin-active", PeerID: "green", Deadline: time.Now().UTC().Add(time.Minute), MaxSendDelayMillis: 60000, SyncRemainingMillis: 1000, Actor: "fixture", Evidence: "offline fixture"}
	raw, _ := json.Marshal(command)
	w := call(m.ControlGuard(m.Control), "POST", strings.Repeat("c", 32), string(raw))
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	// An exact duplicate returns its persisted receipt without resetting the
	// revision or budget. Operational recovery still uses authenticated GET.
	w = call(m.ControlGuard(m.Control), "POST", strings.Repeat("c", 32), string(raw))
	if w.Code != 200 {
		t.Fatal("exact command receipt was not idempotent", w.Code)
	}
	stale := command
	stale.Evidence = "different stale mutation"
	staleRaw, _ := json.Marshal(stale)
	w = call(m.ControlGuard(m.Control), "POST", strings.Repeat("c", 32), string(staleRaw))
	if w.Code != 409 {
		t.Fatal("stale CAS mutation unexpectedly succeeded", w.Code)
	}
	w = call(m.ReadGuard(m.Status), "GET", strings.Repeat("r", 32), "")
	var status struct {
		State spool.ReleaseState `json:"state"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &status); err != nil || status.State.OperationID != "operation-1" || status.State.Revision != 2 || !status.State.Deadline.Equal(command.Deadline) {
		t.Fatal("lost response could not be reconciled", err, w.Body.String())
	}
	for _, body := range []string{`{"injected":"private-body"}`, `{}` + `{}`, `{"action":"activate","retirement_proof":{"payload":"private-body"}}`} {
		w = call(m.ControlGuard(m.Control), "POST", strings.Repeat("c", 32), body)
		if w.Code != 400 || strings.Contains(w.Body.String(), "private-body") {
			t.Fatal("malformed control was not safely rejected", w.Code, w.Body.String())
		}
	}
	state, _ := db.ReleaseStatus()
	if state.Revision != 2 {
		t.Fatal("invalid control mutated ledger", state)
	}
}

func TestFromEnvNeverDisablesPersistedFence(t *testing.T) {
	db, _ := fixture(t, "standby")
	for _, name := range []string{"NM_INSTANCE_ID", "NM_INSTANCE_INITIAL_MODE", "NM_INSTANCE_READ_TOKEN_FILE", "NM_INSTANCE_CONTROL_TOKEN_FILE", "NM_INSTANCE_PROTOCOL_KEY_FILE"} {
		t.Setenv(name, "")
	}
	if _, err := FromEnv(db); err == nil {
		t.Fatal("removing environment disabled persisted release state")
	}
	dir := t.TempDir()
	for name, value := range map[string]string{"NM_INSTANCE_READ_TOKEN_FILE": strings.Repeat("r", 32), "NM_INSTANCE_CONTROL_TOKEN_FILE": strings.Repeat("c", 32), "NM_INSTANCE_PROTOCOL_KEY_FILE": strings.Repeat("p", 32)} {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(value+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		t.Setenv(name, path)
	}
	t.Setenv("NM_INSTANCE_ID", "blue")
	t.Setenv("NM_INSTANCE_INITIAL_MODE", "active")
	if _, err := FromEnv(db); err != nil {
		t.Fatal(err)
	}
	state, _ := db.ReleaseStatus()
	if state.Phase != "standby" || db.AdmissionAllowed(true) == nil {
		t.Fatal("restart reset persisted standby fence", state)
	}
}

func TestCredentialsCannotShareAuthority(t *testing.T) {
	db, _ := fixture(t, "active")
	for _, values := range [][3]string{{"short", strings.Repeat("c", 32), strings.Repeat("p", 32)}, {strings.Repeat("c", 32), strings.Repeat("c", 32), strings.Repeat("p", 32)}, {strings.Repeat("r", 32), strings.Repeat("c", 32), strings.Repeat("r", 32)}} {
		if _, err := New(db, "blue", "active", values[0], values[1], []byte(values[2])); err == nil {
			t.Fatal("credential authority overlap accepted")
		}
	}
}

func TestCanceledControlCannotMutateAfterWaitingForSynchronousSend(t *testing.T) {
	db, m := fixture(t, "active")
	command := spool.ReleaseCommand{OperationID: "late-operation", ExpectedRevision: 1, Action: "begin-active", PeerID: "green", Deadline: time.Now().UTC().Add(time.Minute), MaxSendDelayMillis: 60000, SyncRemainingMillis: 1000, Actor: "fixture", Evidence: "offline canceled request"}
	state, err := db.ControlRelease(command)
	if err != nil {
		t.Fatal(err)
	}
	command.ExpectedRevision, command.Action = state.Revision, "close-admission"
	state, err = db.ControlRelease(command)
	if err != nil {
		t.Fatal(err)
	}
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		finished <- db.WithForwardedSynchronousPermit(context.Background(), command.OperationID, command.PeerID, nil, func() error { close(entered); <-release; return nil })
	}()
	<-entered
	command.ExpectedRevision, command.Action = state.Revision, "freeze"
	before := state
	raw, _ := json.Marshal(command)
	ctx, cancel := context.WithCancel(context.Background())
	r := httptest.NewRequest("POST", "/internal/instance/control", strings.NewReader(string(raw))).WithContext(ctx)
	r.Header.Set("Authorization", "Bearer "+strings.Repeat("c", 32))
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { m.ControlGuard(m.Control)(w, r); close(done) }()
	select {
	case <-done:
		close(release)
		t.Fatal("release mutation did not wait for the active synchronous send")
	case <-time.After(10 * time.Millisecond):
	}
	cancel()
	close(release)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("canceled control did not complete after releasing the send boundary")
	}
	state, _ = db.ReleaseStatus()
	if w.Code == 200 || state.Revision != before.Revision || state.Phase != "draining" || state.OperationID != command.OperationID {
		t.Fatal("late canceled control mutated release state", w.Code, state)
	}
}

func TestMetadataPreseedAndCancelDoNotWaitForUnrelatedSynchronousCall(t *testing.T) {
	a, _ := fixture(t, "active")
	b, err := spool.Open(filepath.Join(t.TempDir(), "new.db"), spool.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if _, err = New(b, "green", "standby", strings.Repeat("r", 32), strings.Repeat("c", 32), []byte(strings.Repeat("p", 32))); err != nil {
		t.Fatal(err)
	}
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() { finished <- a.WithInstancePermit(true, func() error { close(entered); <-release; return nil }) }()
	<-entered
	defer func() {
		close(release)
		if err := <-finished; err != nil {
			t.Error(err)
		}
	}()
	duringSend := func(label string, fn func() error) {
		t.Helper()
		done := make(chan error, 1)
		go func() { done <- fn() }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(label, err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal(label, "waited for unrelated platform request")
		}
	}
	deadline := time.Now().UTC().Add(time.Minute)
	command := spool.ReleaseCommand{OperationID: "metadata-live", ExpectedRevision: 1, Action: "begin-active", PeerID: "green", Deadline: deadline, MaxSendDelayMillis: 60000, SyncRemainingMillis: 1000, Actor: "fixture", Evidence: "platform still in flight"}
	var state spool.ReleaseState
	duringSend("begin-active", func() error { var err error; state, err = a.ControlRelease(command); return err })
	command.ExpectedRevision, command.Action, command.PeerID = 1, "begin-standby", "blue"
	duringSend("begin-standby", func() error { _, err := b.ControlRelease(command); return err })
	var preseed spool.ReleaseManifest
	duringSend("preseed export", func() error {
		var err error
		preseed, err = a.ExportRelease(state.OperationID, state.Revision, false)
		return err
	})
	duringSend("preseed import", func() error {
		bs, _ := b.ReleaseStatus()
		_, err := b.ImportRelease(preseed, spool.ReleaseCommand{OperationID: bs.OperationID, ExpectedRevision: bs.Revision, Action: "import", Actor: "fixture", Evidence: "consistent observation only"})
		return err
	})
	bs, _ := b.ReleaseStatus()
	if bs.SynchronousInFlight != 0 || bs.SynchronousUnresolved != 1 {
		t.Fatal("preseed manufactured local platform in-flight", bs)
	}
	duringSend("cancel", func() error {
		as, _ := a.ReleaseStatus()
		_, err := a.ControlRelease(spool.ReleaseCommand{OperationID: as.OperationID, ExpectedRevision: as.Revision, Action: "cancel", Actor: "fixture", Evidence: "return without stopping original call"})
		return err
	})
	duringSend("close candidate", func() error {
		bs, _ := b.ReleaseStatus()
		_, err := b.ControlRelease(spool.ReleaseCommand{OperationID: bs.OperationID, ExpectedRevision: bs.Revision, Action: "close-admission", Actor: "fixture", Evidence: "zero-attempt buffer"})
		return err
	})
	var delta spool.ReleaseManifest
	duringSend("final candidate buffer", func() error {
		bs, _ := b.ReleaseStatus()
		var err error
		delta, err = b.ExportRelease(bs.OperationID, bs.Revision, true)
		return err
	})
	duringSend("cancel-import", func() error {
		as, _ := a.ReleaseStatus()
		_, err := a.ImportRelease(delta, spool.ReleaseCommand{OperationID: as.OperationID, ExpectedRevision: as.Revision, Action: "cancel-import", Actor: "fixture", Evidence: "atomic return while original sender continues"})
		return err
	})
	var retired spool.ReleaseState
	duringSend("candidate retirement", func() error {
		bs, _ := b.ReleaseStatus()
		var err error
		retired, err = b.ControlRelease(spool.ReleaseCommand{OperationID: bs.OperationID, ExpectedRevision: bs.Revision, Action: "retire", Actor: "fixture", Evidence: "candidate never sent"})
		return err
	})
	duringSend("cancel-complete", func() error {
		as, _ := a.ReleaseStatus()
		_, err := a.ControlRelease(spool.ReleaseCommand{OperationID: as.OperationID, ExpectedRevision: as.Revision, Action: "cancel-complete", Proof: retired.RetirementProof, Actor: "fixture", Evidence: "original call still running"})
		return err
	})
	if as, _ := a.ReleaseStatus(); as.Phase != "active" || as.SynchronousInFlight != 1 {
		t.Fatal("original request disappeared or sender stopped", as)
	}
}
