package jevshadow

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-kit/kit/log"
)

func TestCutoverReadOnlyWriterStatusCannotPauseOrResume(t *testing.T) {
	cfg := persistentCardTestConfig("http://unused.invalid/receipts", t.TempDir())
	control, read := strings.Repeat("w", 32), strings.Repeat("r", 32)
	cfg.WriterControlToken = control
	cfg.CutoverReadToken = read
	s, err := New(log.NewNopLogger(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	handler := s.HandleWriterControl(control)
	for _, tc := range []struct {
		method, token string
		want          int
	}{{"GET", read, 200}, {"GET", control, 200}, {"POST", read, 401}, {"PUT", read, 401}, {"GET", cfg.Token, 401}} {
		r := httptest.NewRequest(tc.method, "/internal/jev/writer-control", strings.NewReader(`{"action":"pause","expected_revision":1,"permission_domain":"sre-us","actor":"fixture","evidence":"unauthorized probe"}`))
		r.Header.Set("Authorization", "Bearer "+tc.token)
		w := httptest.NewRecorder()
		handler(w, r)
		if w.Code != tc.want {
			t.Fatal(tc.method, w.Code)
		}
	}
	if s.writerGate.state.Paused || s.writerGate.state.Revision != 1 {
		t.Fatal("readonly probe mutated writer")
	}
	for _, invalid := range []string{"short", control, cfg.Token} {
		bad := cfg
		bad.CutoverReadToken = invalid
		if err := bad.validate(); err == nil {
			t.Fatal("invalid executor read credential accepted")
		}
	}
	// A read-only installation needs no writer control credential; empty Bearer
	// never grants POST and the installed read credential still only grants GET.
	r := httptest.NewRequest("GET", "/internal/jev/writer-control", nil)
	r.Header.Set("Authorization", "Bearer "+read)
	w := httptest.NewRecorder()
	s.HandleWriterControl("")(w, r)
	if w.Code != 200 {
		t.Fatal("read-only-only installation rejected", w.Code)
	}
}

func TestCutoverReadEnvLoadsCredentialAndFormalFence(t *testing.T) {
	setRequiredConfigEnv(t)
	t.Setenv("NM_CUTOVER_READ_TOKEN", strings.Repeat("r", 32))
	t.Setenv("JEV_EXECUTOR_WRITER_CONTROL_TOKEN", strings.Repeat("w", 32))
	t.Setenv("JEV_FORMAL_WRITER_DISABLED", "true")
	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.CutoverReadToken != strings.Repeat("r", 32) || !cfg.FormalWriterDisabled {
		t.Fatal("env contract not loaded")
	}
	t.Setenv("JEV_FORMAL_WRITER_DISABLED", "invalid")
	if _, err = ConfigFromEnv(); err == nil {
		t.Fatal("invalid fence boolean accepted")
	}
	t.Setenv("JEV_FORMAL_WRITER_DISABLED", "false")
	cfg, err = ConfigFromEnv()
	if err != nil || cfg.FormalWriterDisabled {
		t.Fatal("default false contract changed", err)
	}
	t.Setenv("NM_CUTOVER_READ_TOKEN", strings.Repeat("w", 32))
	if _, err = ConfigFromEnv(); err == nil {
		t.Fatal("read/writer control credential collision accepted")
	}
}
