package jevshadow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-kit/kit/log"
)

func projectionFixture(t *testing.T, n int) (*Service, Config, []cardScopeMember, *atomic.Int32) {
	t.Helper()
	cfg := groupedRuntimeConfig("http://mock.invalid/receipts", t.TempDir())
	cfg.expectedProfileID = "test"
	cfg.expectedProfileVersion = "v2"
	client := &http.Client{Transport: statusTestTransport{}}
	service, err := New(log.NewNopLogger(), cfg, client)
	if err != nil {
		t.Fatal(err)
	}
	calls := &atomic.Int32{}
	data := groupedRuntimeData(n)
	data.ProfileID = "test"
	data.ProfileVersion = "v2"
	data.CardOwnerID = cfg.CardOwnerID
	base := persistentCardBase()
	base["do_not_leak"] = "sensitive-base-card-fixture"
	if err := service.captureDurable(data, "jev-shadow-feishu-uat", "oc_test", "om-projection", base, func(context.Context, string, map[string]interface{}) error { calls.Add(1); return nil }); err != nil {
		t.Fatal(err)
	}
	state, err := service.cardStore.Load("om-projection")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(service.Close)
	return service, cfg, state.MemberScope.Members, calls
}
func projectionCall(t *testing.T, s *Service, cfg Config, extra, token string) (*httptest.ResponseRecorder, map[string]interface{}) {
	t.Helper()
	r := httptest.NewRequest("GET", "/internal/jev/annotations/om-projection?profile_id=test&profile_version=v2&sender_app="+cfg.SenderApp+"&annotation_revision=0"+extra, nil)
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	s.HandleAnnotationStatus(w, r, "om-projection")
	var body map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	return w, body
}
func projectionApply(t *testing.T, s *Service, member cardScopeMember, revision int, oncall bool) {
	t.Helper()
	c := validV2Component()
	c.MemberEventIDs = []string{member.EventID}
	c.JudgmentID = fmt.Sprintf("judgment-confirmed-%d", revision)
	c.AnnotationRevision = revision
	link := "https://aiops.example/apps/alert-center/cards/" + cardBindingID(s.config.SenderApp, "om-projection") + "?domain=infra-us"
	c.TraceURL = &link
	if oncall {
		c.PolicyProposal = "oncall"
		c.PolicyReasonCode = "pool_probability_below_threshold"
		c.PoolMinProbability = floatPointer(.95)
	}
	if _, err := s.applyAnnotation(context.Background(), "om-projection", fmt.Sprintf("projection-key-%d", revision), c); err != nil {
		t.Fatal(err)
	}
}

func TestCardProjectionOptInPreservesOldResponseAndReadsOneConfirmedSnapshot(t *testing.T) {
	s, cfg, members, calls := projectionFixture(t, 14)
	for i := 0; i < 4; i++ {
		projectionApply(t, s, members[i], i+1, i == 0)
	}
	before, err := os.ReadFile(s.cardStore.path("om-projection"))
	if err != nil {
		t.Fatal(err)
	}
	w, old := projectionCall(t, s, cfg, "", cfg.AnnotationToken)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if _, ok := old["card_projection"]; ok {
		t.Fatal("old wire response changed")
	}
	w, out := projectionCall(t, s, cfg, "&projection=card-summary-v1", cfg.AnnotationToken)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	p := out["card_projection"].(map[string]interface{})
	delete(out, "card_projection")
	if !reflect.DeepEqual(old, out) {
		t.Fatal("opt-in changed legacy fields")
	}
	for key, want := range map[string]interface{}{"schema_version": rendererCardSummary, "renderer_version": rendererCardSummary, "perspective": "last-confirmed-executor-state", "platform_current": "not-read", "permission_domain": "infra-us", "execution_domain": cfg.ExecutionDomain, "profile_id": "test", "profile_version": "v2", "card_owner_id": cfg.CardOwnerID, "sender_app": cfg.SenderApp, "destination": "oc_test", "message_id": "om-projection", "base_revision": float64(1), "annotation_revision": float64(4), "scope_known": true, "counts_available": true, "total": float64(14), "received": float64(4), "oncall": float64(1), "pool": float64(3), "missing": float64(10), "pending": false, "patch_unknown": false} {
		if p[key] != want {
			t.Fatalf("%s got %#v want %#v", key, p[key], want)
		}
	}
	rows := p["members"].([]interface{})
	first := rows[0].(map[string]interface{})
	if first["judgment_id"] != "judgment-confirmed-1" || first["annotation_revision"] != float64(1) || first["final_policy"] != "oncall" || first["result_state"] != "confirmed" {
		t.Fatal(first)
	}
	if rows[4].(map[string]interface{})["result_state"] != "missing" {
		t.Fatal(rows[4])
	}
	after, _ := os.ReadFile(s.cardStore.path("om-projection"))
	if string(before) != string(after) || calls.Load() != 4 {
		t.Fatal("GET changed durable state or called patcher")
	}
	if strings.Contains(w.Body.String(), "sensitive-base-card-fixture") || strings.Contains(w.Body.String(), "rendered_card") || strings.Contains(w.Body.String(), cfg.AnnotationToken) {
		t.Fatal("projection leaked full card or credential")
	}
	// A reusable, synthetic contract fixture for the Jev consumer.
	if path := os.Getenv("JEV_PROJECTION_FIXTURE_OUTPUT"); path != "" {
		raw, _ := json.MarshalIndent(p, "", "  ")
		if err := os.WriteFile(path, append(raw, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCardProjectionUnknownPatchReportsOnlyLastConfirmedAndSurvivesRestart(t *testing.T) {
	s, cfg, members, _ := projectionFixture(t, 14)
	for i := 0; i < 4; i++ {
		projectionApply(t, s, members[i], i+1, i == 0)
	}
	binding, err := s.getCardBinding("om-projection")
	if err != nil {
		t.Fatal(err)
	}
	binding.patcher = func(context.Context, string, map[string]interface{}) error {
		return errors.New("unknown network outcome")
	}
	c := validV2Component()
	c.MemberEventIDs = []string{members[4].EventID}
	c.AnnotationRevision = 5
	c.JudgmentID = "pending-must-not-be-adopted"
	if _, err := s.applyAnnotation(context.Background(), "om-projection", "projection-pending-key", c); !errors.Is(err, ErrPatchStateUnknown) {
		t.Fatal(err)
	}
	s.Close()
	restarted, err := New(log.NewNopLogger(), cfg, &http.Client{Transport: statusTestTransport{}})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	before, _ := os.ReadFile(restarted.cardStore.path("om-projection"))
	w, out := projectionCall(t, restarted, cfg, "&projection=card-summary-v1", cfg.AnnotationToken)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	p := out["card_projection"].(map[string]interface{})
	if p["annotation_revision"] != float64(4) || p["received"] != float64(4) || p["oncall"] != float64(1) || p["pending"] != true || p["patch_unknown"] != true || p["pending_annotation_revision"] != float64(5) || p["platform_current"] != "unknown" {
		t.Fatal(p)
	}
	if strings.Contains(w.Body.String(), "pending-must-not-be-adopted") || p["members"].([]interface{})[4].(map[string]interface{})["result_state"] != "missing" {
		t.Fatal("pending result adopted")
	}
	after, _ := os.ReadFile(restarted.cardStore.path("om-projection"))
	if string(before) != string(after) {
		t.Fatal("GET mutated unknown evidence")
	}
}

func TestCardProjectionLegacyUnrecordedRendererIsExplicitlyUnknown(t *testing.T) {
	s, cfg, members, _ := projectionFixture(t, 2)
	projectionApply(t, s, members[0], 1, false)
	state, _ := s.cardStore.Load("om-projection")
	state.Version = legacyCardStateVersion
	state.MemberScope = nil
	state.ConfirmedRendererVersion = ""
	if err := s.cardStore.Save(&state); err != nil {
		t.Fatal(err)
	}
	w, out := projectionCall(t, s, cfg, "&projection=card-summary-v1", cfg.AnnotationToken)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	p := out["card_projection"].(map[string]interface{})
	if p["renderer_version"] != "unknown" || p["counts_available"] != false || p["scope_known"] != false || p["received"] != nil || p["total"] != nil || p["unknown_reason"] != "renderer_unrecorded" {
		t.Fatal(p)
	}
	row := p["members"].([]interface{})[0].(map[string]interface{})
	if row["result_state"] != "unknown" || row["judgment_id"] != nil || row["annotation_revision"] != nil {
		t.Fatal("legacy renderer invented adopted result", row)
	}
}

func TestCardProjectionPaginationAndAuthDoNotExpandComponentLimit(t *testing.T) {
	s, cfg, members, calls := projectionFixture(t, 1001)
	projectionApply(t, s, members[0], 1, false)
	w, out := projectionCall(t, s, cfg, "&projection=card-summary-v1&member_offset=0&member_limit=2", cfg.AnnotationToken)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	p := out["card_projection"].(map[string]interface{})
	if p["member_total"] != float64(1001) || len(p["members"].([]interface{})) != 2 || p["next_offset"] != float64(2) || p["total"] != float64(1001) || p["missing"] != float64(1000) {
		t.Fatal(p)
	}
	w, out = projectionCall(t, s, cfg, "&projection=card-summary-v1&member_offset=1000&member_limit=1000", cfg.AnnotationToken)
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
	p = out["card_projection"].(map[string]interface{})
	if len(p["members"].([]interface{})) != 1 || p["next_offset"] != nil {
		t.Fatal(p)
	}
	for _, query := range []string{"&projection=unsupported", "&projection=card-summary-v1&member_limit=1001", "&projection=card-summary-v1&member_offset=-1"} {
		w, _ := projectionCall(t, s, cfg, query, cfg.AnnotationToken)
		if w.Code != 400 {
			t.Fatal(query, w.Code)
		}
	}
	w, _ = projectionCall(t, s, cfg, "&projection=card-summary-v1", "wrong")
	if w.Code != 401 {
		t.Fatal(w.Code)
	}
	if calls.Load() != 1 {
		t.Fatal("GET invoked platform patcher")
	}
}

func TestCardProjectionConflictingMembersCannotClaimAdoptedPool(t *testing.T) {
	s, cfg, members, _ := projectionFixture(t, 2)
	projectionApply(t, s, members[0], 1, true)
	projectionApply(t, s, members[1], 2, false)
	c := validV2Component()
	c.MemberEventIDs = []string{members[0].EventID, members[1].EventID}
	c.AnnotationRevision = 3
	if _, err := s.applyAnnotation(context.Background(), "om-projection", "overlap-projection-key", c); err != nil {
		t.Fatal(err)
	}
	w, out := projectionCall(t, s, cfg, "&projection=card-summary-v1", cfg.AnnotationToken)
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
	p := out["card_projection"].(map[string]interface{})
	if p["oncall"] != float64(1) || p["conflicts"] != float64(2) {
		t.Fatal(p)
	}
	for _, item := range p["members"].([]interface{}) {
		row := item.(map[string]interface{})
		if row["result_state"] != "conflict" || row["judgment_id"] != nil || row["final_policy"] != nil {
			t.Fatal("conflict invented adoption", row)
		}
	}
}

func TestCardProjectionRejectsScopeBoundaryMismatch(t *testing.T) {
	s, cfg, members, calls := projectionFixture(t, 2)
	projectionApply(t, s, members[0], 1, false)
	original, err := s.cardStore.Load("om-projection")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		mutate func(*cardMemberScope)
	}{
		{"permission-domain", func(scope *cardMemberScope) { scope.PermissionDomain = "infra-eu" }},
		{"execution-domain", func(scope *cardMemberScope) { scope.ExecutionDomain = "another-executor" }},
		{"profile", func(scope *cardMemberScope) { scope.ProfileID = "formal" }},
		{"profile-version", func(scope *cardMemberScope) { scope.ProfileVersion = "another-version" }},
		{"owner", func(scope *cardMemberScope) { scope.CardOwnerID = "another-owner" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var state cardPersistentState
			if err := json.Unmarshal(raw, &state); err != nil {
				t.Fatal(err)
			}
			tc.mutate(state.MemberScope)
			if err := s.cardStore.Save(&state); err != nil {
				t.Fatal(err)
			}
			before, _ := os.ReadFile(s.cardStore.path("om-projection"))
			w, _ := projectionCall(t, s, cfg, "&projection=card-summary-v1", cfg.AnnotationToken)
			if w.Code != 403 || strings.Contains(w.Body.String(), "card_projection") {
				t.Fatalf("mismatched scope exposed: %d %s", w.Code, w.Body.String())
			}
			after, _ := os.ReadFile(s.cardStore.path("om-projection"))
			if string(before) != string(after) {
				t.Fatal("rejected GET changed state")
			}
		})
	}
	if calls.Load() != 1 {
		t.Fatal("rejected GET called patcher")
	}
}

func TestCardProjectionConfirmedSummaryWithLegacyScopeKeepsTotalsUnknown(t *testing.T) {
	s, cfg, members, _ := projectionFixture(t, 1)
	// Model an actually scope-less legacy binding BEFORE its next legal new
	// summary revision, rather than changing inputs to an already rendered card.
	state, err := s.cardStore.Load("om-projection")
	if err != nil {
		t.Fatal(err)
	}
	state.Version = legacyCardStateVersion
	state.MemberScope = nil
	if err := s.cardStore.Save(&state); err != nil {
		t.Fatal(err)
	}
	projectionApply(t, s, members[0], 1, false)
	w, out := projectionCall(t, s, cfg, "&projection=card-summary-v1", cfg.AnnotationToken)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	p := out["card_projection"].(map[string]interface{})
	if p["renderer_version"] != rendererCardSummary || p["counts_available"] != true || p["scope_known"] != false || p["total"] != nil || p["firing"] != nil || p["resolved"] != nil || p["received"] != float64(1) || p["pool"] != float64(1) {
		t.Fatal("legacy scope invented a total", p)
	}
	row := p["members"].([]interface{})[0].(map[string]interface{})
	if row["judgment_id"] != "judgment-confirmed-1" || row["result_state"] != "confirmed" || row["status"] != "unknown" {
		t.Fatal(row)
	}
}
