package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/kubesphere/notification-manager/apis/v2beta2"
	"github.com/kubesphere/notification-manager/pkg/controller"
	"github.com/kubesphere/notification-manager/pkg/deliveryprofiles"
	"github.com/kubesphere/notification-manager/pkg/instancecontrol"
	spool "github.com/kubesphere/notification-manager/pkg/notification_spool"
	"github.com/kubesphere/notification-manager/pkg/store"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type syncGeneration struct {
	h       *Webhook
	db      *spool.Store
	manager *instancecontrol.Manager
	server  *httptest.Server
}

func synchronousGeneration(t *testing.T, id, mode string) syncGeneration {
	return synchronousGenerationBudget(t, id, mode, 3*time.Second)
}

func synchronousGenerationBudget(t *testing.T, id, mode string, budget time.Duration) syncGeneration {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	unused := "https://unused.invalid"
	var options *v2beta2.Options
	if budget > 5*time.Second {
		seconds := int32((budget + time.Second - 1) / time.Second)
		options = &v2beta2.Options{Webhook: &v2beta2.WebhookOptions{NotificationTimeout: &seconds}}
	}
	ctl, err := controller.NewStatic(ctx, log.NewNopLogger(), controller.StaticConfiguration{Environment: "test", Cluster: "synchronous-isolated", Options: options, Receivers: []v2beta2.Receiver{{ObjectMeta: metav1.ObjectMeta{Name: "unused"}, Spec: v2beta2.ReceiverSpec{Webhook: &v2beta2.WebhookReceiver{URL: &unused}}}}})
	if err != nil {
		t.Fatal(err)
	}
	if err = ctl.Run(); err != nil {
		t.Fatal(err)
	}
	db, err := spool.Open(filepath.Join(t.TempDir(), "ledger.db"), spool.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	profiles, err := deliveryprofiles.New(db, deliveryprofiles.File{Version: 1, RetryDedupeWindow: "2m", RepeatInterval: "12h", Profiles: []spool.DeliveryProfile{{ID: "test", Version: "v1", CardOwnerID: "test-owner", Receiver: "critical", SourceChatID: deliveryprofiles.ProductionChat, TestChatID: deliveryprofiles.TestChat}, {ID: "formal", Version: "v1", CardOwnerID: "formal-owner"}}}, strings.Repeat("t", 32), strings.Repeat("f", 32), strings.Repeat("c", 32))
	if err != nil {
		t.Fatal(err)
	}
	ctl.DeliveryProfiles = profiles
	if _, err = db.ControlProfile("formal", "prepare", "v1", 1, "fixture", "isolated prepare"); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ControlProfile("formal", "activate", "v1", 2, "fixture", "isolated activate"); err != nil {
		t.Fatal(err)
	}
	manager, err := instancecontrol.New(db, id, mode, strings.Repeat("r", 32), strings.Repeat("i", 32), []byte(strings.Repeat("p", 32)))
	if err != nil {
		t.Fatal(err)
	}
	h := New(log.NewNopLogger(), ctl, &store.AlertStore{Durable: db}, &Options{ListenAddress: ":19094", FormalCompatListenAddress: ":19093", FormalCompatSourceCIDRs: []string{"10.1.1.9/32"}, WebhookTimeout: budget, WorkerTimeout: budget, InstanceControl: manager})
	server := httptest.NewServer(h.router)
	t.Cleanup(server.Close)
	return syncGeneration{h, db, manager, server}
}

func synchronousPair(t *testing.T) (syncGeneration, syncGeneration) {
	return synchronousPairBudget(t, 3*time.Second)
}

func synchronousPairBudget(t *testing.T, budget time.Duration) (syncGeneration, syncGeneration) {
	t.Helper()
	a, b := synchronousGenerationBudget(t, "blue", "active", budget), synchronousGenerationBudget(t, "green", "standby", budget)
	for _, item := range []struct {
		local, peer syncGeneration
		id          string
	}{{a, b, "green"}, {b, a, "blue"}} {
		if err := item.local.manager.ConfigureSynchronousPeer(instancecontrol.SyncPeer{ID: item.id, URL: item.peer.server.URL, AllowHTTP: true}, nil); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().UTC().Add(30 * time.Second)
	for _, item := range []struct {
		g            syncGeneration
		action, peer string
	}{{a, "begin-active", "green"}, {b, "begin-standby", "blue"}} {
		st, _ := item.g.db.ReleaseStatus()
		_, err := item.g.db.ControlRelease(spool.ReleaseCommand{OperationID: "sync-upgrade", ExpectedRevision: st.Revision, Action: item.action, PeerID: item.peer, Deadline: deadline, MaxSendDelayMillis: 20000, SyncRemainingMillis: 500, Actor: "fixture", Evidence: "isolated synchronous handoff"})
		if err != nil {
			t.Fatal(err)
		}
	}
	return a, b
}

func synchronousControl(t *testing.T, g syncGeneration, action string) spool.ReleaseState {
	t.Helper()
	st, _ := g.db.ReleaseStatus()
	st, err := g.db.ControlRelease(spool.ReleaseCommand{OperationID: st.OperationID, ExpectedRevision: st.Revision, Action: action, Actor: "fixture", Evidence: "isolated handoff"})
	if err != nil {
		t.Fatal(action, err)
	}
	return st
}

func synchronousRetire(t *testing.T, a, b syncGeneration) *spool.RetirementProof {
	t.Helper()
	synchronousControl(t, a, "close-admission")
	st := synchronousControl(t, a, "freeze")
	m, err := a.db.ExportRelease(st.OperationID, st.Revision, true)
	if err != nil {
		t.Fatal(err)
	}
	bs, _ := b.db.ReleaseStatus()
	_, err = b.db.ImportRelease(m, spool.ReleaseCommand{OperationID: bs.OperationID, ExpectedRevision: bs.Revision, Action: "import", Actor: "fixture", Evidence: "isolated final snapshot"})
	if err != nil {
		t.Fatal(err)
	}
	return synchronousControl(t, a, "retire").RetirementProof
}

func synchronousBody(endpoint string) []byte {
	raw, _ := json.Marshal(map[string]interface{}{"receiver": map[string]interface{}{"metadata": map[string]interface{}{"name": "inline", "labels": map[string]string{"type": "global"}}, "spec": map[string]interface{}{"webhook": map[string]string{"url": endpoint}}}, "alert": map[string]interface{}{"alerts": []interface{}{map[string]interface{}{"labels": map[string]string{"alertname": "handoff"}}}}})
	return raw
}

func synchronousRequest(t *testing.T, g syncGeneration, api, endpoint string, ctx context.Context) (int, string) {
	t.Helper()
	req := httptest.NewRequest("POST", "/api/v2/"+api, bytes.NewReader(synchronousBody(endpoint))).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+strings.Repeat("f", 32))
	w := httptest.NewRecorder()
	g.h.router.ServeHTTP(w, req)
	return w.Code, w.Body.String()
}

func TestFutureSynchronousBufferForwardsActualResultsAndOriginalBudget(t *testing.T) {
	var calls atomic.Int32
	platform := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		switch r.URL.Path {
		case "/failure":
			w.WriteHeader(503)
		case "/slow":
			select {
			case <-r.Context().Done():
			case <-time.After(2 * time.Second):
				w.WriteHeader(200)
			}
		default:
			w.WriteHeader(200)
		}
	}))
	defer platform.Close()
	a, b := synchronousPair(t)
	for _, api := range []string{"verify", "notifications"} {
		for _, path := range []string{"/success", "/failure", "/slow"} {
			budget := 2 * time.Second
			if path == "/slow" {
				budget = 500 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), budget)
			start := time.Now()
			code, body := synchronousRequest(t, b, api, platform.URL+path, ctx)
			cancel()
			want := 400
			if path == "/success" {
				want = 200
			}
			if code != want || strings.Contains(body, "accepted for durable") {
				t.Fatal(api, path, code, body)
			}
			if path == "/slow" && time.Since(start) > time.Second {
				t.Fatal("forward renewed the original request budget")
			}
		}
	}
	if calls.Load() != 6 {
		t.Fatal("missing or blind repeated platform request", calls.Load())
	}
	old, _ := a.db.SynchronousAttempts()
	if len(old) != 6 {
		t.Fatal("actual sender did not account for every request", old)
	}
	rows, _ := b.db.ForwardAttempts()
	if len(rows) != 6 {
		t.Fatal("forwarding references missing", rows)
	}
	if err := b.db.WithInstancePermit(true, func() error { return nil }); err == nil {
		t.Fatal("forwarding accidentally enabled standby sends")
	}
}

func TestFutureSynchronousRetirementProofAssistsActivationAndOldConnection(t *testing.T) {
	var calls atomic.Int32
	platform := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(200) }))
	defer platform.Close()
	a, b := synchronousPair(t)
	synchronousRetire(t, a, b)
	// No coordinator activate request: the successor verifies the exact final
	// snapshot/proof from the peer's explicit never-executed response.
	for _, g := range []syncGeneration{a, b, a} {
		if code, body := synchronousRequest(t, g, "notifications", platform.URL, context.Background()); code != 200 {
			t.Fatal(code, body)
		}
	}
	bs, _ := b.db.ReleaseStatus()
	as, _ := a.db.ReleaseStatus()
	if bs.Phase != "active" || as.Phase != "retired" || calls.Load() != 3 {
		t.Fatal(as, bs, calls.Load())
	}
}

func TestFutureSynchronousActivePeerProofNeverWaitsForUnrelatedSlowPlatform(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	platform := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path == "/held" {
			close(entered)
			<-release
		}
		w.WriteHeader(200)
	}))
	defer platform.Close()
	a, b := synchronousPair(t)
	proof := synchronousRetire(t, a, b)
	bs, _ := b.db.ReleaseStatus()
	if _, err := b.db.ControlRelease(spool.ReleaseCommand{OperationID: bs.OperationID, ExpectedRevision: bs.Revision, Action: "activate", Proof: proof, Actor: "fixture", Evidence: "already active before old proof"}); err != nil {
		t.Fatal(err)
	}
	heldDone := make(chan int, 1)
	go func() {
		code, _ := synchronousRequest(t, b, "notifications", platform.URL+"/held", context.Background())
		heldDone <- code
	}()
	<-entered
	// The old keepalive includes its durable proof. An already active peer
	// must not acquire another activation writer lock behind the held request.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	code, body := synchronousRequest(t, a, "verify", platform.URL+"/fast", ctx)
	cancel()
	close(release)
	if code != 200 {
		t.Fatal("active peer queued a needless activation behind platform", code, body)
	}
	if other := <-heldDone; other != 200 || calls.Load() != 2 {
		t.Fatal(other, calls.Load())
	}
}

func TestFutureSynchronousOldConnectionCrossesFrozenWindowWithout503(t *testing.T) {
	var calls atomic.Int32
	platform := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(200) }))
	defer platform.Close()
	a, b := synchronousPair(t)
	synchronousControl(t, a, "close-admission")
	synchronousControl(t, a, "freeze")
	done := make(chan int, 1)
	go func() {
		code, _ := synchronousRequest(t, a, "notifications", platform.URL, context.Background())
		done <- code
	}()
	time.Sleep(20 * time.Millisecond)
	synchronousRetire(t, a, b)
	select {
	case code := <-done:
		if code != 200 {
			t.Fatal("old keepalive failed only due to publication", code)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("old keepalive did not follow activation")
	}
	if calls.Load() != 1 {
		t.Fatal("safe routing decision duplicated the platform call", calls.Load())
	}
}

func TestFutureSynchronousSuccessorObservesLaterProofAfterCoordinatorStops(t *testing.T) {
	var calls atomic.Int32
	platform := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(200) }))
	defer platform.Close()
	a, b := synchronousPair(t)
	synchronousControl(t, a, "close-admission")
	synchronousControl(t, a, "freeze")
	done := make(chan int, 1)
	go func() {
		code, _ := synchronousRequest(t, b, "verify", platform.URL, context.Background())
		done <- code
	}()
	time.Sleep(20 * time.Millisecond)
	synchronousRetire(t, a, b) // The coordinator deliberately never activates B.
	select {
	case code := <-done:
		if code != 200 {
			t.Fatal("later retirement proof was not observed", code)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("request stalled after authenticated no-send reply")
	}
	if calls.Load() != 1 {
		t.Fatal(calls.Load())
	}
}

func TestFutureSynchronousFreezeAccountsForInFlightPlatform(t *testing.T) {
	entered, finish := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	platform := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		close(entered)
		<-finish
		w.WriteHeader(200)
	}))
	defer platform.Close()
	a, b := synchronousPair(t)
	requestDone := make(chan int, 1)
	go func() {
		code, _ := synchronousRequest(t, b, "notifications", platform.URL, context.Background())
		requestDone <- code
	}()
	<-entered
	closeDone := make(chan struct{})
	go func() {
		synchronousControl(t, a, "close-admission")
		synchronousControl(t, a, "freeze")
		close(closeDone)
	}()
	select {
	case <-closeDone:
		t.Fatal("freeze passed a live platform request")
	case <-time.After(20 * time.Millisecond):
	}
	close(finish)
	if code := <-requestDone; code != 200 {
		t.Fatal(code)
	}
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("freeze remained blocked after completion")
	}
	if calls.Load() != 1 {
		t.Fatal("platform call duplicated")
	}
}

func TestFutureSynchronousAuthenticationSourceAndSingleHop(t *testing.T) {
	var calls atomic.Int32
	platform := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(200) }))
	defer platform.Close()
	a, b := synchronousPair(t)
	bad := httptest.NewRequest("POST", "/api/v2/notifications", bytes.NewReader(synchronousBody(platform.URL)))
	bad.Header.Set("Authorization", "Bearer wrong")
	w := httptest.NewRecorder()
	b.h.router.ServeHTTP(w, bad)
	if w.Code != 401 {
		t.Fatal(w.Code)
	}
	bad = httptest.NewRequest("POST", "/api/v2/notifications", bytes.NewReader(synchronousBody(platform.URL)))
	bad.RemoteAddr = "10.1.1.8:1234"
	w = httptest.NewRecorder()
	b.h.compatRouter.ServeHTTP(w, bad)
	if w.Code != 403 {
		t.Fatal(w.Code)
	}
	good := httptest.NewRequest("POST", "/api/v2/verify", bytes.NewReader(synchronousBody(platform.URL)))
	good.RemoteAddr = "10.1.1.9:1234"
	w = httptest.NewRecorder()
	b.h.compatRouter.ServeHTTP(w, good)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	forged := httptest.NewRequest("POST", "/internal/instance/synchronous/forward", bytes.NewReader(synchronousBody(platform.URL)))
	w = httptest.NewRecorder()
	a.h.router.ServeHTTP(w, forged)
	if w.Code != 401 || calls.Load() != 1 {
		t.Fatal(w.Code, calls.Load())
	}
	// The internal receiver never routes a denied send to another peer.
	synchronousControl(t, a, "close-admission")
	synchronousControl(t, a, "freeze")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	code, _ := synchronousRequest(t, b, "notifications", platform.URL, ctx)
	if code != 200 || calls.Load() != 2 {
		t.Fatal("unretired sender did not cancel stalled publication before caller budget", code, calls.Load())
	}
}

func TestFutureSynchronousFrozenDisconnectCancelsBeforeSameCallerBudget(t *testing.T) {
	var calls atomic.Int32
	platform := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(200) }))
	defer platform.Close()
	a, b := synchronousPair(t)
	synchronousControl(t, a, "close-admission")
	frozen := synchronousControl(t, a, "freeze")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	code, body := synchronousRequest(t, b, "notifications", platform.URL, ctx)
	current, _ := a.db.ReleaseStatus()
	newState, _ := b.db.ReleaseStatus()
	if code != 200 || calls.Load() != 1 || current.Phase != "cancelling" || current.RetirementProof != nil || !current.Deadline.Equal(frozen.Deadline) || newState.Phase != "buffering" {
		t.Fatal(code, body, calls.Load(), current, newState)
	}
}

func TestFutureSynchronousShutdownPreservesRequestLongerThanOldTenSecondGrace(t *testing.T) {
	entered, finish := make(chan struct{}), make(chan struct{})
	platform := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-finish; w.WriteHeader(200) }))
	defer platform.Close()
	g, successor := synchronousPairBudget(t, 15*time.Second)
	synchronousRetire(t, g, successor)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	g.h.ListenAddress = address
	g.h.FormalCompatListenAddress = ""
	g.h.compatRouter = nil
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- g.h.Run(ctx) }()
	readyDeadline := time.Now().Add(3 * time.Second)
	for {
		resp, err := http.Get("http://" + address + "/-/live")
		if err == nil {
			_ = resp.Body.Close()
			break
		}
		if time.Now().After(readyDeadline) {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	result := make(chan int, 1)
	go func() {
		req, _ := http.NewRequest("POST", "http://"+address+"/api/v2/notifications", bytes.NewReader(synchronousBody(platform.URL)))
		req.Header.Set("Authorization", "Bearer "+strings.Repeat("f", 32))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			result <- 0
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		result <- resp.StatusCode
	}()
	<-entered
	cancel()
	select {
	case err := <-done:
		close(finish)
		t.Fatal("HTTP server exited before original request grace", err)
	case <-time.After(10250 * time.Millisecond):
	}
	close(finish)
	select {
	case code := <-result:
		if code != 200 {
			t.Fatal("long synchronous call was cut during shutdown", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("platform response not returned")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not complete after in-flight response")
	}
}

func TestFutureSynchronousTransportResponseLossIsNotBlindlyRetried(t *testing.T) {
	var calls atomic.Int32
	platform := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(200) }))
	defer platform.Close()
	a, b := synchronousPair(t)
	// A real peer executes the platform call, then its response is lost at the
	// HTTP transport boundary. The source must retain unknown, not call again.
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		resp, err := http.DefaultTransport.RoundTrip(req)
		if err != nil {
			return nil, err
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		return nil, io.ErrUnexpectedEOF
	})}
	if err := b.manager.ConfigureSynchronousPeer(instancecontrol.SyncPeer{ID: "blue", URL: a.server.URL, AllowHTTP: true}, client); err != nil {
		t.Fatal(err)
	}
	code, _ := synchronousRequest(t, b, "notifications", platform.URL, context.Background())
	if code != 400 || calls.Load() != 1 {
		t.Fatal(code, calls.Load())
	}
	rows, _ := b.db.ForwardAttempts()
	if len(rows) != 1 || rows[0].State != spool.Unknown {
		t.Fatal(rows)
	}
}

func TestFutureSynchronousForwardRequestReplayCannotCallPlatformAgain(t *testing.T) {
	var calls atomic.Int32
	platform := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(200) }))
	defer platform.Close()
	a, b := synchronousPair(t)
	var replay *http.Request
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		raw, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		_ = req.Body.Close()
		replay = req.Clone(context.Background())
		replay.Body = io.NopCloser(bytes.NewReader(raw))
		replay.GetBody = nil
		req.Body = io.NopCloser(bytes.NewReader(raw))
		return http.DefaultTransport.RoundTrip(req)
	})}
	if err := b.manager.ConfigureSynchronousPeer(instancecontrol.SyncPeer{ID: "blue", URL: a.server.URL, AllowHTTP: true}, client); err != nil {
		t.Fatal(err)
	}
	if code, body := synchronousRequest(t, b, "verify", platform.URL, context.Background()); code != 200 {
		t.Fatal(code, body)
	}
	resp, err := http.DefaultClient.Do(replay)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 409 || resp.Header.Get("X-NM-Release-Not-Sent") != "" || calls.Load() != 1 {
		t.Fatal("replay was treated as proof of no platform attempt", resp.StatusCode, calls.Load())
	}
}

func TestFutureSynchronousOversizedPeerResponseCannotReportSuccess(t *testing.T) {
	var calls atomic.Int32
	platform := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(200) }))
	defer platform.Close()
	a, b := synchronousPair(t)
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		resp, err := http.DefaultTransport.RoundTrip(req)
		if err != nil {
			return nil, err
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		resp.Body = io.NopCloser(strings.NewReader(strings.Repeat("x", (2<<20)+1)))
		return resp, nil
	})}
	if err := b.manager.ConfigureSynchronousPeer(instancecontrol.SyncPeer{ID: "blue", URL: a.server.URL, AllowHTTP: true}, client); err != nil {
		t.Fatal(err)
	}
	code, _ := synchronousRequest(t, b, "notifications", platform.URL, context.Background())
	rows, _ := b.db.ForwardAttempts()
	if code != 400 || calls.Load() != 1 || len(rows) != 1 || rows[0].State != spool.Unknown {
		t.Fatal(code, calls.Load(), rows)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
