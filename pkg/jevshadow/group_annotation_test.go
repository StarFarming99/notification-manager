package jevshadow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/kubesphere/notification-manager/pkg/template"
)

func nativeGroupFixture(t *testing.T, n int) (*Service, Config, *FinalSnapshot, *atomic.Int32, *map[string]interface{}) {
	t.Helper()
	prototype, data := finalSnapshotFixture(n)
	cfg := prototype.config
	directory := t.TempDir()
	cfg.CardStateDir = directory + "/cards"
	cfg.ReceiptOutboxDir = directory + "/receipts"
	cfg.groupProfileEnabled = true
	s, err := New(log.NewNopLogger(), cfg, &http.Client{Transport: statusTestTransport{}})
	if err != nil {
		t.Fatal(err)
	}
	calls := &atomic.Int32{}
	last := &map[string]interface{}{}
	if err := s.captureDurable(data, "jev-shadow-feishu-uat", "oc_test", "om-native-group", persistentCardBase(), func(_ context.Context, _ string, card map[string]interface{}) error {
		calls.Add(1)
		*last = card
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	state, err := s.cardStore.Load("om-native-group")
	if err != nil || state.FinalSnapshot == nil {
		t.Fatalf("missing group snapshot %v", err)
	}
	t.Cleanup(s.Close)
	return s, cfg, state.FinalSnapshot, calls, last
}
func nativeGroupComponent(snapshot *FinalSnapshot, messageID string, revision, expected int) GroupAnnotationComponent {
	assessment := "group_assessment-fixture"
	c := GroupAnnotationComponent{SchemaVersion: "3", Capability: groupAnnotationCapability, Kind: "group_judgment", AnnotationRevision: revision, ExpectedAnnotationRevision: expected, ExpectedBaseRevision: 1, SnapshotID: snapshot.SnapshotID, MembersHash: snapshot.MembersHash, AssessmentID: &assessment, Scope: GroupAnnotationScope{PermissionDomain: snapshot.PermissionDomain, ExecutionDomain: snapshot.ExecutionDomain, ProfileID: snapshot.ProfileID, ProfileVersion: snapshot.ProfileVersion, CardOwnerID: snapshot.CardOwnerID, Receiver: snapshot.Receiver, SenderApp: snapshot.SenderApp, Destination: snapshot.Destination}, AsOf: "2026-10-15T01:02:00Z", MemberCount: snapshot.MemberCount, ActiveMemberCount: snapshot.MemberCount, GroupResult: &GroupRiskResult{Decision: "needs_action", PolicyAction: "oncall", Probabilities: map[string]float64{"all_can_wait": .05, "needs_action": .9, "needs_member_verification": .03, "insufficient_evidence": .02}, ExceptionMemberIDs: []string{snapshot.Members[len(snapshot.Members)-1].EventID}}, Versions: GroupAnnotationVersions{Template: "sre-group-v1", Compiler: "group-compiler-v1", Questions: "group-questions-v1", QuestionsHash: strings.Repeat("a", 64), Policy: "group-shadow-policy-v1", ConfigHash: strings.Repeat("b", 64)}, DetailURL: "https://aiops.example/apps/alert-center/groups/" + assessment + "?domain=" + snapshot.PermissionDomain, ReasonCode: "group_needs_action"}
	c.ExpectedBaseHash, _ = hashCanonical(persistentCardBase())
	for _, member := range snapshot.Members {
		c.LifecycleMembers = append(c.LifecycleMembers, GroupLifecycleMember{EventID: member.EventID, EpisodeID: member.EpisodeID, SentStatus: member.Status, CurrentStatus: "firing", Reason: "active_at_cutoff"})
	}
	return c
}
func nativeLifecycle(snapshot *FinalSnapshot, messageID string, revision, expected, life, expectedLife int) GroupAnnotationComponent {
	c := nativeGroupComponent(snapshot, messageID, revision, expected)
	c.Kind = "lifecycle"
	c.AssessmentID = nil
	c.GroupResult = nil
	c.LifecycleRevision = life
	c.ExpectedLifecycleRevision = expectedLife
	c.ActiveMemberCount = 0
	c.ResolvedMemberCount = snapshot.MemberCount
	c.DetailURL = "https://aiops.example/apps/alert-center/cards/" + cardBindingID(snapshot.SenderApp, messageID) + "?domain=" + snapshot.PermissionDomain
	c.ReasonCode = "late_firing_already_resolved"
	for i, member := range snapshot.Members {
		start, _ := parseGroupTime(member.StartsAt)
		end := start.Add(time.Minute)
		value := pythonISOTime(end)
		resolution := occurrenceV2(member.EpisodeID, &template.Alert{Status: "resolved", Labels: template.KV(member.Labels), Annotations: template.KV(member.Annotations), StartsAt: start, EndsAt: end})
		c.LifecycleMembers[i].CurrentStatus = "resolved"
		c.LifecycleMembers[i].ResolutionEventID = &resolution
		c.LifecycleMembers[i].ResolvedAt = &value
		c.LifecycleMembers[i].Reason = "trusted_episode_resolution"
	}
	return c
}
func nativeProjection(t *testing.T, s *Service, cfg Config) map[string]interface{} {
	t.Helper()
	request := httptest.NewRequest("GET", "/internal/jev/annotations/om-native-group?profile_id=test&profile_version=v1&sender_app="+cfg.SenderApp+"&annotation_revision=0&projection=group-card-v1", nil)
	request.Header.Set("Authorization", "Bearer "+cfg.AnnotationToken)
	response := httptest.NewRecorder()
	s.HandleAnnotationStatus(response, request, "om-native-group")
	if response.Code != 200 {
		t.Fatal(response.Code, response.Body.String())
	}
	var body map[string]interface{}
	_ = json.Unmarshal(response.Body.Bytes(), &body)
	return body["group_projection"].(map[string]interface{})
}
func nativeCardText(t *testing.T, card map[string]interface{}) string {
	t.Helper()
	elements := card["elements"].([]interface{})
	if len(elements) != 4 {
		t.Fatalf("not one block plus original base: %#v", elements)
	}
	text := elements[2].(map[string]interface{})["text"].(map[string]interface{})["content"].(string)
	if strings.Contains(text, "Member:") || strings.Contains(text, "已判断：人工处理") {
		t.Fatal("member votes rendered as group")
	}
	return text
}

func TestNativeGroupAnnotationRendersOneTrueGroupDistributionAndReadonlyProjection(t *testing.T) {
	s, cfg, snapshot, calls, last := nativeGroupFixture(t, 14)
	projection := nativeProjection(t, s, cfg)
	if projection["annotation_revision"] != float64(0) || projection["lifecycle_revision"] != float64(0) || projection["scope_known"] != true || projection["group_component"] != nil {
		t.Fatal("initial group state unavailable", projection)
	}
	if path := os.Getenv("JEV_GROUP_INITIAL_PROJECTION_OUTPUT"); path != "" {
		raw, _ := json.MarshalIndent(projection, "", "  ")
		_ = os.WriteFile(path, append(raw, '\n'), 0600)
	}
	c := nativeGroupComponent(snapshot, "om-native-group", 1, 0)
	raw, _ := json.Marshal(c)
	request := httptest.NewRequest("PUT", "/internal/jev/group-annotations/om-native-group", bytes.NewReader(raw))
	request.Header.Set("Authorization", "Bearer "+cfg.AnnotationToken)
	request.Header.Set("Idempotency-Key", "native-group-first-key")
	response := httptest.NewRecorder()
	s.HandleGroupAnnotation(response, request, "om-native-group")
	if response.Code != 200 {
		t.Fatal(response.Code, response.Body.String())
	}
	text := nativeCardText(t, *last)
	if !strings.Contains(text, "Jev 组判断：整组需要处理") || !strings.Contains(text, "覆盖14条成员") || strings.Count(text, "组概率") != 1 || strings.Contains(text, "人工处理14条") {
		t.Fatal(text)
	}
	state, _ := s.cardStore.Load("om-native-group")
	if len(state.Annotations) != 0 || state.GroupComponent == nil || state.ConfirmedRendererVersion != rendererGroupCard || calls.Load() != 1 {
		t.Fatal("group result copied into member annotations")
	}
	before, _ := os.ReadFile(s.cardStore.path("om-native-group"))
	projection = nativeProjection(t, s, cfg)
	after, _ := os.ReadFile(s.cardStore.path("om-native-group"))
	if string(before) != string(after) || calls.Load() != 1 || projection["group_component"] == nil || projection["renderer_version"] != rendererGroupCard {
		t.Fatal("projection changed state or omitted confirmed group")
	}
	if path := os.Getenv("JEV_GROUP_CONFIRMED_PROJECTION_OUTPUT"); path != "" {
		raw, _ := json.MarshalIndent(projection, "", "  ")
		_ = os.WriteFile(path, append(raw, '\n'), 0600)
	}
}
func TestNativeLifecycleCrossesCancelledGapAndFencesOldRisk(t *testing.T) {
	s, _, snapshot, calls, last := nativeGroupFixture(t, 14)
	// Reservation 1 was cancelled in Jev before reaching executor. Exact CAS0
	// with no pending evidence safely accepts status revision2/lifecycle1.
	lifecycle := nativeLifecycle(snapshot, "om-native-group", 2, 0, 1, 0)
	if _, err := s.applyGroupAnnotation(context.Background(), "om-native-group", "status-revision-two", lifecycle); err != nil {
		t.Fatal(err)
	}
	text := nativeCardText(t, *last)
	if !strings.Contains(text, "该次告警已恢复") || !strings.Contains(text, "迟到的 firing") || !strings.Contains(text, "未重新判断") || strings.Contains(text, "组概率") {
		t.Fatal(text)
	}
	stale := nativeGroupComponent(snapshot, "om-native-group", 3, 2)
	if _, err := s.applyGroupAnnotation(context.Background(), "om-native-group", "stale-risk-after-recovery", stale); !errors.Is(err, ErrRevision) {
		t.Fatal("old lifecycle risk accepted", err)
	}
	stale.ExpectedLifecycleRevision = 1
	stale.LifecycleRevision = 1
	if _, err := s.applyGroupAnnotation(context.Background(), "om-native-group", "resurrect-at-new-revision", stale); !errors.Is(err, ErrRevision) {
		t.Fatal("terminal members resurrected", err)
	}
	legacy := validV2Component()
	legacy.AnnotationRevision = 3
	legacy.MemberEventIDs = []string{snapshot.Members[0].EventID}
	if _, err := s.applyAnnotation(context.Background(), "om-native-group", "legacy-cannot-overwrite", legacy); !errors.Is(err, ErrRevision) {
		t.Fatal("old member component overwrote group", err)
	}
	state, _ := s.cardStore.Load("om-native-group")
	if state.AnnotationRevision != 2 || state.GroupLifecycleRevision != 1 || calls.Load() != 1 {
		t.Fatal("stale risk changed state")
	}
}

func TestNativeLifecycleStatusOnlyAssessmentUsesExactlyBoundGroupDetail(t *testing.T) {
	s, _, snapshot, calls, last := nativeGroupFixture(t, 2)
	c := nativeLifecycle(snapshot, "om-native-group", 1, 0, 1, 0)
	assessment := "group_status-only-fixture"
	c.AssessmentID = &assessment
	c.DetailURL = "https://aiops.example/apps/alert-center/groups/" + assessment + "?domain=" + snapshot.PermissionDomain
	wrong := c
	wrong.DetailURL = "https://aiops.example/apps/alert-center/groups/another-group?domain=" + snapshot.PermissionDomain
	if _, err := s.applyGroupAnnotation(context.Background(), "om-native-group", "wrong-status-subject", wrong); !errors.Is(err, ErrInvalidPayload) {
		t.Fatal("unbound group detail accepted", err)
	}
	if _, err := s.applyGroupAnnotation(context.Background(), "om-native-group", "status-only-group-detail", c); err != nil {
		t.Fatal(err)
	}
	text := nativeCardText(t, *last)
	if calls.Load() != 1 || strings.Contains(text, "组概率") || c.GroupResult != nil {
		t.Fatal("status-only assessment rendered a model result", text)
	}
	state, _ := s.cardStore.Load("om-native-group")
	if state.GroupComponent == nil || state.GroupComponent.AssessmentID == nil || *state.GroupComponent.AssessmentID != assessment {
		t.Fatal("status-only group subject not retained")
	}
}
func TestNativeGroupMixedLifecycleAllowsOnlyMatchingRiskEpoch(t *testing.T) {
	s, _, snapshot, _, last := nativeGroupFixture(t, 2)
	lifecycle := nativeLifecycle(snapshot, "om-native-group", 1, 0, 1, 0)
	lifecycle.ActiveMemberCount = 1
	lifecycle.ResolvedMemberCount = 1
	lifecycle.LifecycleMembers[1].CurrentStatus = "firing"
	lifecycle.LifecycleMembers[1].ResolutionEventID = nil
	lifecycle.LifecycleMembers[1].ResolvedAt = nil
	if _, err := s.applyGroupAnnotation(context.Background(), "om-native-group", "mixed-lifecycle-key", lifecycle); err != nil {
		t.Fatal(err)
	}
	risk := nativeGroupComponent(snapshot, "om-native-group", 2, 1)
	risk.ExpectedLifecycleRevision = 1
	risk.LifecycleRevision = 1
	risk.ActiveMemberCount = 1
	risk.ResolvedMemberCount = 1
	risk.LifecycleMembers = lifecycle.LifecycleMembers
	risk.GroupResult.ExceptionMemberIDs = []string{snapshot.Members[1].EventID}
	if _, err := s.applyGroupAnnotation(context.Background(), "om-native-group", "mixed-valid-risk-key", risk); err != nil {
		t.Fatal(err)
	}
	text := nativeCardText(t, *last)
	if !strings.Contains(text, "有效firing 1条 · 已恢复1条") {
		t.Fatal(text)
	}
}
func TestNativeGroupUnknownPatchPreservesFrozenIntentAndBlocksGapAcrossRestart(t *testing.T) {
	s, cfg, snapshot, _, _ := nativeGroupFixture(t, 2)
	binding, _ := s.getCardBinding("om-native-group")
	binding.patcher = func(context.Context, string, map[string]interface{}) error {
		return errors.New("connection lost after platform write")
	}
	risk := nativeGroupComponent(snapshot, "om-native-group", 1, 0)
	if _, err := s.applyGroupAnnotation(context.Background(), "om-native-group", "unknown-native-risk", risk); !errors.Is(err, ErrPatchStateUnknown) {
		t.Fatal(err)
	}
	state, _ := s.cardStore.Load("om-native-group")
	before, _ := groupPayloadHash(state.PendingGroupPatch)
	s.Close()
	restarted, err := New(log.NewNopLogger(), cfg, &http.Client{Transport: statusTestTransport{}})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	var calls atomic.Int32
	restarted.SetCardPatcherResolver(func(context.Context, string, string) (CardPatcher, error) {
		return func(context.Context, string, map[string]interface{}) error { calls.Add(1); return nil }, nil
	})
	status := nativeLifecycle(snapshot, "om-native-group", 3, 0, 1, 0)
	if _, err := restarted.applyGroupAnnotation(context.Background(), "om-native-group", "cannot-skip-unknown", status); !errors.Is(err, ErrPatchStateUnknown) {
		t.Fatal("gap cleared unknown evidence", err)
	}
	loaded, err := restarted.cardStore.Load("om-native-group")
	if err != nil {
		t.Fatal(err)
	}
	after, _ := groupPayloadHash(loaded.PendingGroupPatch)
	if before != after || loaded.AnnotationRevision != 0 || loaded.GroupComponent != nil || calls.Load() != 0 {
		t.Fatal("pending group rewritten/replayed")
	}
	projection := nativeProjection(t, restarted, cfg)
	if projection["pending"] != true || projection["patch_unknown"] != true || projection["platform_current"] != "unknown" || projection["group_component"] != nil {
		t.Fatal(projection)
	}
}
func TestNativeGroupCASIdempotencyAndUnknownMemberScopeReject(t *testing.T) {
	s, _, snapshot, calls, _ := nativeGroupFixture(t, 2)
	component := nativeGroupComponent(snapshot, "om-native-group", 1, 0)
	raw, _ := json.Marshal(component)
	cases := []func(*GroupAnnotationComponent){func(c *GroupAnnotationComponent) { c.Scope.Destination = "other" }, func(c *GroupAnnotationComponent) { c.SnapshotID = "other" }, func(c *GroupAnnotationComponent) { c.MembersHash = strings.Repeat("0", 64) }, func(c *GroupAnnotationComponent) { c.ExpectedBaseHash = strings.Repeat("0", 64) }, func(c *GroupAnnotationComponent) { c.GroupResult.ExceptionMemberIDs = []string{"nonexistent-event"} }, func(c *GroupAnnotationComponent) {
		c.DetailURL = "https://aiops.example/apps/alert-center/groups/another?domain=infra-us"
	}, func(c *GroupAnnotationComponent) { c.LifecycleMembers[0].EpisodeID = "another-episode" }}
	for index, mutate := range cases {
		var c GroupAnnotationComponent
		_ = json.Unmarshal(raw, &c)
		mutate(&c)
		if _, err := s.applyGroupAnnotation(context.Background(), "om-native-group", fmt.Sprintf("bad-native-key-%d", index), c); err == nil {
			t.Fatal("bad group accepted", index)
		}
	}
	var wait sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := s.applyGroupAnnotation(context.Background(), "om-native-group", "same-native-idempotency", component)
			errs <- err
		}()
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 {
		t.Fatal("concurrent idempotency patched more than once")
	}
}
func TestNativeGroupReceiptAndObservationShareExactSnapshotAndTriggerLocators(t *testing.T) {
	prototype, data := finalSnapshotFixture(14)
	receipt, err := prototype.buildReceipt(data, "jev-shadow-feishu-uat", "oc_test", "om-native-group", strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	if receipt.SchemaVersion != "2" || receipt.AssessmentMode != GroupAssessmentMode || receipt.Observation == nil || receipt.Observation.SchemaVersion != "2" || !equalGroupSnapshot(receipt.FinalSnapshot, receipt.Observation.FinalSnapshot) || receipt.Observation.ProfileVersion != "v1" {
		t.Fatal("receipt/observation group scope drift")
	}
	if receipt.Observation.Alerts[0].GeneratorURL != data.Alerts[0].GeneratorURL || receipt.FinalSnapshot.Members[0].GeneratorURL == "" {
		t.Fatal("trigger locator lost")
	}
	if path := os.Getenv("JEV_GROUP_RECEIPT_FIXTURE_OUTPUT"); path != "" {
		raw, _ := json.MarshalIndent(receipt, "", "  ")
		_ = os.WriteFile(path, append(raw, '\n'), 0600)
	}
	// Mode-off old schema does not acquire new fields merely because new code runs.
	prototype.config.groupProfileEnabled = false
	old, err := prototype.buildReceipt(data, "jev-shadow-feishu-uat", "oc_test", "om-old", strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	wire, _ := json.Marshal(old)
	if old.SchemaVersion != "1" || bytes.Contains(wire, []byte("final_snapshot")) || bytes.Contains(wire, []byte("generator_url")) || bytes.Contains(wire, []byte("assessment_mode")) {
		t.Fatal("old wire contract changed")
	}
}

func TestNativeGroupTLSPlatformPreservesOriginalButtonsAndSameMID(t *testing.T) {
	prototype, data := finalSnapshotFixture(14)
	cfg := prototype.config
	directory := t.TempDir()
	cfg.CardStateDir = directory + "/cards"
	cfg.ReceiptOutboxDir = directory + "/receipts"
	s, err := New(log.NewNopLogger(), cfg, &http.Client{Transport: statusTestTransport{}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	base, _, _ := groupedFixture(14)
	originalElements, _ := json.Marshal(base["elements"])
	var count atomic.Int32
	platform := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PATCH" || r.URL.Path != "/messages/om-native-group" {
			t.Errorf("wrong platform target %s %s", r.Method, r.URL.Path)
		}
		var card map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&card); err != nil {
			t.Error(err)
		}
		elements := card["elements"].([]interface{})
		prefix, _ := json.Marshal(elements[:2])
		if !bytes.Equal(prefix, originalElements) || len(elements) != 5 {
			t.Error("original descriptions/buttons changed or repeated group blocks")
		}
		count.Add(1)
		w.WriteHeader(200)
	}))
	defer platform.Close()
	patcher := func(ctx context.Context, mid string, card map[string]interface{}) error {
		raw, _ := json.Marshal(card)
		request, _ := http.NewRequestWithContext(ctx, "PATCH", platform.URL+"/messages/"+mid, bytes.NewReader(raw))
		response, err := platform.Client().Do(request)
		if err != nil {
			return err
		}
		response.Body.Close()
		if response.StatusCode != 200 {
			return errors.New("platform rejected")
		}
		return nil
	}
	if err := s.captureDurable(data, "jev-shadow-feishu-uat", "oc_test", "om-native-group", base, patcher); err != nil {
		t.Fatal(err)
	}
	state, _ := s.cardStore.Load("om-native-group")
	risk := nativeGroupComponent(state.FinalSnapshot, "om-native-group", 1, 0)
	risk.ExpectedBaseHash, _ = hashCanonical(base)
	if _, err := s.applyGroupAnnotation(context.Background(), "om-native-group", "tls-native-group-first", risk); err != nil {
		t.Fatal(err)
	}
	lifecycle := nativeLifecycle(state.FinalSnapshot, "om-native-group", 2, 1, 1, 0)
	lifecycle.ExpectedBaseHash = risk.ExpectedBaseHash
	if _, err := s.applyGroupAnnotation(context.Background(), "om-native-group", "tls-native-lifecycle-second", lifecycle); err != nil {
		t.Fatal(err)
	}
	if count.Load() != 2 {
		t.Fatal("platform updates lost")
	}
}

func TestNativeGroupRestartDoesNotWarmLargeSnapshotsAndStrictlyKeepsUnknownFiles(t *testing.T) {
	s, cfg, snapshot, _, _ := nativeGroupFixture(t, 1000)
	risk := nativeGroupComponent(snapshot, "om-native-group", 1, 0)
	if _, err := s.applyGroupAnnotation(context.Background(), "om-native-group", "thousand-member-group", risk); err != nil {
		t.Fatal(err)
	}
	statePath := s.cardStore.path("om-native-group")
	info, err := os.Stat(statePath)
	if err != nil {
		t.Fatal(err)
	}
	// Preserve a non-reader-compatible/corrupt file as evidence; no whole-volume
	// startup scan moves it to dead-letter just to initialize an empty cache.
	badPath := s.cardStore.path("om-unreadable")
	bad := []byte(`{"version":999,"unknown_future_field":"retain-original"}`)
	if err := os.WriteFile(badPath, bad, 0600); err != nil {
		t.Fatal(err)
	}
	s.Close()
	cfg.MaxCards = 2000
	restarted, err := New(log.NewNopLogger(), cfg, &http.Client{Transport: statusTestTransport{}})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if len(restarted.cards) != 0 || restarted.cardBindingCacheLimit() != 32 {
		t.Fatal("startup eagerly cached persisted groups")
	}
	if _, err := restarted.getCardBinding("om-unreadable"); err == nil {
		t.Fatal("unknown state accepted")
	}
	after, _ := os.ReadFile(badPath)
	if !bytes.Equal(bad, after) {
		t.Fatal("unknown/corrupt original file was rewritten or quarantined")
	}
	binding, err := restarted.getCardBinding("om-native-group")
	if err != nil {
		t.Fatal(err)
	}
	if binding.memberScope != nil || len(restarted.cards) != 1 {
		t.Fatal("large member snapshot retained in basic binding cache")
	}
	projection := nativeProjection(t, restarted, cfg)
	if projection["group_component"] == nil || projection["annotation_revision"] != float64(1) {
		t.Fatal("on-demand group recovery lost confirmation")
	}
	if path := os.Getenv("JEV_GROUP_RESOURCE_FIXTURE_OUTPUT"); path != "" {
		result := map[string]interface{}{"members": 1000, "state_file_bytes": info.Size(), "startup_binding_cache_count": 0, "basic_binding_cache_limit": 32, "group_snapshots_retained_in_basic_cache": false, "strict_unknown_original_preserved": true, "on_demand_recovery": "PASS"}
		raw, _ := json.MarshalIndent(result, "", "  ")
		_ = os.WriteFile(path, append(raw, '\n'), 0600)
	}
}
