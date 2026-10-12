package jevshadow

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-kit/kit/log"
)

type statusTestTransport struct{}

func (statusTestTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: 202, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
}
func TestDurableAnnotationStatusExactScopeReadOnlyAcrossRestart(t *testing.T) {
	config := persistentCardTestConfig("http://mock.invalid/receipts", t.TempDir())
	config.expectedProfileID = "test"
	config.expectedProfileVersion = "v1"
	client := &http.Client{Transport: statusTestTransport{}}
	s, err := New(log.NewNopLogger(), config, client)
	if err != nil {
		t.Fatal(err)
	}
	data := persistentCardTestData()
	data.ProfileID = "test"
	data.ProfileVersion = "v1"
	data.CardOwnerID = config.CardOwnerID
	base := persistentCardBase()
	base["sensitive_fixture"] = "base-card-must-not-be-returned"
	s.CaptureSuccessfulDelivery(data, "jev-shadow-feishu-uat", "oc_test", "om-status", base, func(context.Context, string, map[string]interface{}) error { return nil })
	component := validV2Component()
	if _, err = s.applyAnnotation(context.Background(), "om-status", "status-request-key", component); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = New(log.NewNopLogger(), config, client)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	call := func(query, token string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, "/internal/jev/annotations/om-status?"+query, nil)
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		s.HandleAnnotationStatus(w, r, "om-status")
		return w
	}
	query := "profile_id=test&profile_version=v1&sender_app=" + config.SenderApp + "&annotation_revision=1"
	token := config.AnnotationToken
	if token == "" {
		token = config.Token
	}
	w := call(query, token)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var out map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	if out["confirmed"] != true || out["annotation_revision"] != float64(1) || out["request_id"] == "" {
		t.Fatal(out)
	}
	if strings.Contains(w.Body.String(), "base-card-must-not-be-returned") || strings.Contains(w.Body.String(), "rendered_card") || strings.Contains(w.Body.String(), token) {
		t.Fatal("status leaked card/credential")
	}
	for _, tc := range []struct {
		query, token string
		want         int
	}{{query, "wrong", 401}, {strings.Replace(query, "profile_id=test", "profile_id=formal", 1), token, 403}, {strings.Replace(query, "sender_app="+config.SenderApp, "sender_app=other", 1), token, 403}, {query + "x", token, 400}} {
		if out := call(tc.query, tc.token); out.Code != tc.want {
			t.Fatal(tc, out.Code, out.Body.String())
		}
	}
	state, err := s.cardStore.Load("om-status")
	if err != nil || state.AnnotationRevision != 1 || state.PatchSendState != patchSendConfirmed {
		t.Fatal("GET mutated state", err)
	}
}
