package v1

import (
	"bytes"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/kubesphere/notification-manager/pkg/controller"
	spool "github.com/kubesphere/notification-manager/pkg/notification_spool"
	"github.com/kubesphere/notification-manager/pkg/store"
)

func TestAdmissionDiagnosticsUseBoundedSafeCodesAndLiveCounters(t *testing.T) {
	var logs bytes.Buffer
	h := New(log.NewLogfmtLogger(&logs), time.Second, &controller.Controller{}, &store.AlertStore{})
	h.recordAdmissionFailure(&admissionFailure{stage: "template_or_credential_freeze", err: errors.New("synthetic-token-and-payload-must-not-leak")}, spool.Hash([]byte("group")))
	h.recordAdmissionFailure(&admissionFailure{stage: "spool_commit", err: spool.ErrCapacity}, "")
	h.RecordProfileAdmissionFailure("test")
	if strings.Contains(logs.String(), "synthetic-token") {
		t.Fatal("raw failure leaked")
	}
	w := httptest.NewRecorder()
	h.ServeMetrics(w, httptest.NewRequest("GET", "/metrics", nil))
	for _, class := range []string{"template_or_credential_freeze", "spool_capacity", "test_profile_not_accepting"} {
		if !strings.Contains(w.Body.String(), `class="`+class+`"} 1`) {
			t.Fatal(w.Body.String())
		}
	}
	status := httptest.NewRecorder()
	h.ServeStatus(status, httptest.NewRequest("GET", "/status", nil))
	if !strings.Contains(status.Body.String(), `"admission_failures"`) {
		t.Fatal(status.Body.String())
	}
}
