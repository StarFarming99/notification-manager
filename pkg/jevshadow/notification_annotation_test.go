package jevshadow

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
)

func ordinaryNotificationComponent(snapshot *FinalSnapshot, mid string) GroupAnnotationComponent {
	c := nativeGroupComponent(snapshot, mid, 1, 0)
	c.Kind = "notification_judgment"
	c.GroupResult = nil
	c.DetailURL = "https://aiops.example/apps/alert-center/traces/" + *c.AssessmentID + "?domain=" + snapshot.PermissionDomain
	yes, no, threshold := true, false, .9
	c.NotificationResult = &NotificationRiskResult{Routing: &ChoiceAnswer{Choice: "pool", Probabilities: map[string]float64{"pool": .92, "oncall": .08}}, PoolReason: &ChoiceAnswer{Choice: "recurring", Probabilities: map[string]float64{"recurring": .8, "same_incident": .12, "non_urgent": .06, "unclear": .02}}, PolicyProposal: "pool", PolicyReasonCode: "pool_threshold_met", PoolMinProbability: &threshold, DecisionSource: "model_policy", ThresholdApplied: &yes, ThresholdSource: "default", ModelStatus: "available", ContextReduced: &no}
	return c
}

func silentLifecycle(snapshot *FinalSnapshot, mid string, life, previous int) GroupAnnotationComponent {
	c := nativeLifecycle(snapshot, mid, 0, 0, life, previous)
	c.Kind = "lifecycle_only"
	return c
}

func TestNotificationOrdinaryCardPreservesBothQuestionDistributionsAndOneTrace(t *testing.T) {
	for _, n := range []int{1, 14} {
		for _, route := range []string{"pool", "oncall"} {
			t.Run(route+string(rune(n)), func(t *testing.T) {
				s, _, snapshot, calls, last := nativeGroupFixture(t, n)
				c := ordinaryNotificationComponent(snapshot, "om-native-group")
				if route == "oncall" {
					c.NotificationResult.Routing = &ChoiceAnswer{Choice: "oncall", Probabilities: map[string]float64{"pool": .07, "oncall": .93}}
					c.NotificationResult.PolicyProposal = "oncall"
					c.NotificationResult.PolicyReasonCode = "pool_probability_below_threshold"
				}
				if _, err := s.applyGroupAnnotation(context.Background(), "om-native-group", "notification-ordinary-first", c); err != nil {
					t.Fatal(err)
				}
				text := nativeCardText(t, *last)
				for _, required := range []string{"Recurring 80% · Same incident 12%", "Non-urgent 6% · Unclear 2%", "Policy:", "Pool threshold 90%"} {
					if !strings.Contains(text, required) {
						t.Fatal(text)
					}
				}
				if strings.Contains(text, "Jev 组判断") || strings.Contains(text, "组概率") || calls.Load() != 1 {
					t.Fatal(text)
				}
				raw, _ := json.Marshal(*last)
				if strings.Count(string(raw), "/traces/") != 1 || strings.Contains(string(raw), "/groups/") {
					t.Fatal(string(raw))
				}
				if path := os.Getenv("JEV_NOTIFICATION_COMPONENT_OUTPUT"); path != "" && n == 14 && route == "oncall" {
					encoded, _ := json.MarshalIndent(c, "", "  ")
					if err := os.WriteFile(path, append(encoded, '\n'), 0600); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}

func TestNotificationUnavailableDoesNotInventProbabilities(t *testing.T) {
	s, _, snapshot, calls, last := nativeGroupFixture(t, 1)
	c := ordinaryNotificationComponent(snapshot, "om-native-group")
	no := false
	c.NotificationResult = &NotificationRiskResult{PolicyProposal: "original", PolicyReasonCode: "model_timeout", DecisionSource: "model_unavailable", ThresholdApplied: &no, ThresholdSource: "not_applicable", ModelStatus: "unavailable", ModelErrorCode: "model_timeout", ContextReduced: &no}
	if _, err := s.applyGroupAnnotation(context.Background(), "om-native-group", "notification-no-model", c); err != nil {
		t.Fatal(err)
	}
	text := nativeCardText(t, *last)
	if calls.Load() != 1 || strings.Contains(text, "100%") || strings.Contains(text, "Pool 0%") || !strings.Contains(text, "Original routing retained") {
		t.Fatal(text)
	}
	invalid := c
	invalid.AnnotationRevision = 2
	invalid.ExpectedAnnotationRevision = 1
	invalid.NotificationResult.Routing = &ChoiceAnswer{Choice: "oncall", Probabilities: map[string]float64{"pool": 0, "oncall": 1}}
	if _, err := s.applyGroupAnnotation(context.Background(), "om-native-group", "invalid-fabricated-result", invalid); !errors.Is(err, ErrInvalidPayload) {
		t.Fatal(err)
	}
}

func TestNotificationSilentFenceHTTPIsNotVisibleSuccessAndSurvivesRestart(t *testing.T) {
	s, cfg, snapshot, calls, _ := nativeGroupFixture(t, 2)
	c := silentLifecycle(snapshot, "om-native-group", 1, 0)
	raw, _ := json.Marshal(c)
	req := httptest.NewRequest(http.MethodPut, "/internal/jev/group-annotations/om-native-group", strings.NewReader(string(raw)))
	req.Header.Set("Authorization", "Bearer "+cfg.AnnotationToken)
	req.Header.Set("Idempotency-Key", "silent-fence-http-first")
	w := httptest.NewRecorder()
	s.HandleGroupAnnotation(w, req, "om-native-group")
	var ack map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &ack)
	if w.Code != 200 || ack["lifecycle_only"] != true || ack["visible_annotation_revision"] != float64(0) || calls.Load() != 0 {
		t.Fatal(w.Code, w.Body.String())
	}
	before, _ := os.ReadFile(s.cardStore.path("om-native-group"))
	if _, err := s.applyGroupAnnotation(context.Background(), "om-native-group", "silent-fence-http-first", c); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(s.cardStore.path("om-native-group"))
	if string(before) != string(after) {
		t.Fatal("fence changed visible ledger")
	}
	s.Close()
	restarted, err := New(log.NewNopLogger(), cfg, &http.Client{Transport: statusTestTransport{}})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	state, err := restarted.cardStore.Load("om-native-group")
	if err != nil {
		t.Fatal(err)
	}
	if state.AnnotationRevision != 0 || len(state.AppliedKeys) != 0 || state.PatchSendState != patchSendNone {
		t.Fatal("fence claimed visible success")
	}
	risk := ordinaryNotificationComponent(snapshot, "om-native-group")
	if _, err := restarted.applyGroupAnnotation(context.Background(), "om-native-group", "late-risk-after-restart", risk); !errors.Is(err, ErrRevision) {
		t.Fatal(err)
	}
	p := nativeProjection(t, restarted, cfg)
	if p["lifecycle_revision"] != float64(1) || p["lifecycle_fence"] == nil || p["group_component"] != nil {
		t.Fatal(p)
	}
	if path := os.Getenv("JEV_LIFECYCLE_FENCE_OUTPUT"); path != "" {
		encoded, _ := json.MarshalIndent(p, "", "  ")
		if err := os.WriteFile(path, append(encoded, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNotificationInflightSlowPatchDoesNotBlockSameMIDTerminalOrOtherScope(t *testing.T) {
	s, cfg, snapshot, _, _ := nativeGroupFixture(t, 1)
	started, release := make(chan struct{}), make(chan struct{})
	binding, _ := s.getCardBinding("om-native-group")
	binding.patcher = func(context.Context, string, map[string]interface{}) error {
		close(started)
		<-release
		return errors.New("platform outcome unknown")
	}
	done := make(chan error, 1)
	go func() {
		_, err := s.applyGroupAnnotation(context.Background(), "om-native-group", "slow-risk-inflight", ordinaryNotificationComponent(snapshot, "om-native-group"))
		done <- err
	}()
	<-started
	pending, err := s.cardStore.Load("om-native-group")
	if err != nil {
		t.Fatal(err)
	}
	hash, _ := groupPayloadHash(pending.PendingGroupPatch)
	begin := time.Now()
	if _, err := s.applyGroupAnnotation(context.Background(), "om-native-group", "terminal-during-http", silentLifecycle(snapshot, "om-native-group", 1, 0)); err != nil {
		t.Fatal(err)
	}
	if time.Since(begin) > 250*time.Millisecond {
		t.Fatal("terminal fence waited for platform HTTP")
	}
	// Independent lifecycle receipt activity shares neither the blocked binding
	// nor an external-HTTP-held lifecycle lock.
	other := *snapshot
	other.ProfileVersion = "other-generation"
	other.Destination = "oc_other"
	if err := s.cardStore.withLifecycleLock(func() error {
		return s.cardStore.persistTerminalMembers(&other, silentLifecycle(snapshot, "om-native-group", 1, 0).LifecycleMembers, "2026-10-15T01:02:00Z", "trusted_nm_receipt")
	}); err != nil {
		t.Fatal(err)
	}
	still, _ := s.cardStore.Load("om-native-group")
	otherHash, _ := groupPayloadHash(still.PendingGroupPatch)
	if hash != otherHash || still.PatchSendState != patchSendSending || still.AnnotationRevision != 0 {
		t.Fatal("terminal fact mutated in-flight intent")
	}
	close(release)
	if err := <-done; !errors.Is(err, ErrPatchStateUnknown) {
		t.Fatal(err)
	}
	unknown, _ := s.cardStore.Load("om-native-group")
	if unknown.PendingGroupPatch == nil || unknown.PatchSendState != patchSendUnknown {
		t.Fatal("unknown was cleared")
	}
	p := nativeProjection(t, s, cfg)
	if p["lifecycle_fence"] == nil || p["patch_unknown"] != true {
		t.Fatal(p)
	}
}

func TestNotificationFinalBoundaryRechecksFenceAfterPatcherResolution(t *testing.T) {
	s, _, snapshot, _, _ := nativeGroupFixture(t, 1)
	binding, _ := s.getCardBinding("om-native-group")
	binding.patcher = nil
	s.mu.Lock()
	delete(s.patchers, cardPatcherKey(binding.receiver, binding.destination))
	s.mu.Unlock()
	var calls atomic.Int32
	s.SetCardPatcherResolver(func(context.Context, string, string) (CardPatcher, error) {
		if _, err := s.applyGroupAnnotation(context.Background(), "om-native-group", "fence-before-final-gate", silentLifecycle(snapshot, "om-native-group", 1, 0)); err != nil {
			t.Fatal(err)
		}
		return func(context.Context, string, map[string]interface{}) error { calls.Add(1); return nil }, nil
	})
	if _, err := s.applyGroupAnnotation(context.Background(), "om-native-group", "old-risk-after-resolver", ordinaryNotificationComponent(snapshot, "om-native-group")); !errors.Is(err, ErrRevision) {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("late risk reached platform")
	}
}

func TestNotificationTrustedResolvedReceiptFencesOldMIDAndNewEpisodeStillWorks(t *testing.T) {
	for _, sameMID := range []bool{false, true} {
		t.Run(map[bool]string{false: "new-mid", true: "same-mid-contract"}[sameMID], func(t *testing.T) {
			s, _, snapshot, calls, _ := nativeGroupFixture(t, 1)
			_, resolved := finalSnapshotFixture(1)
			resolved.Alerts[0].Status = "resolved"
			resolved.Alerts[0].EndsAt = resolved.Alerts[0].StartsAt.Add(time.Minute)
			mid := "om-recovered-new"
			if sameMID {
				mid = "om-native-group"
			}
			base := persistentCardBase()
			base["resolved"] = true
			err := s.captureDurable(resolved, "jev-shadow-feishu-uat", "oc_test", mid, base, func(context.Context, string, map[string]interface{}) error { calls.Add(1); return nil })
			if (!sameMID && err != nil) || (sameMID && !errors.Is(err, ErrRevision)) {
				t.Fatal(err)
			}
			if _, err := s.applyGroupAnnotation(context.Background(), "om-native-group", "late-risk-from-old-episode", ordinaryNotificationComponent(snapshot, "om-native-group")); !errors.Is(err, ErrRevision) {
				t.Fatal(err)
			}
			_, fresh := finalSnapshotFixture(1)
			fresh.Alerts[0].StartsAt = fresh.Alerts[0].StartsAt.Add(2 * time.Hour)
			if err := s.captureDurable(fresh, "jev-shadow-feishu-uat", "oc_test", "om-new-episode", persistentCardBase(), func(context.Context, string, map[string]interface{}) error { calls.Add(1); return nil }); err != nil {
				t.Fatal(err)
			}
			state, _ := s.cardStore.Load("om-new-episode")
			if state.FinalSnapshot.Members[0].EpisodeID == snapshot.Members[0].EpisodeID {
				t.Fatal("fixture did not create new episode")
			}
			if _, err := s.applyGroupAnnotation(context.Background(), "om-new-episode", "valid-next-episode", ordinaryNotificationComponent(state.FinalSnapshot, "om-new-episode")); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 1 {
				t.Fatal("old risk sent, or new episode incorrectly fenced")
			}
		})
	}
}

func TestNotificationFenceSubprocessHelper(t *testing.T) {
	directory := os.Getenv("JEV_FENCE_SUBPROCESS_CARD_DIR")
	if directory == "" {
		t.Skip("subprocess contract helper")
	}
	store, err := newCardStateStore(log.NewNopLogger(), directory)
	if err != nil {
		t.Fatal(err)
	}
	state, err := store.Load("om-native-group")
	if err != nil {
		t.Fatal(err)
	}
	s := &Service{cardStore: store}
	if _, err := s.applyGroupAnnotation(context.Background(), "om-native-group", "cross-process-terminal", silentLifecycle(state.FinalSnapshot, "om-native-group", 1, 0)); err != nil {
		t.Fatal(err)
	}
}

func TestNotificationCrossProcessFenceDuringPlatformHTTPRetainsUnknown(t *testing.T) {
	s, cfg, snapshot, _, _ := nativeGroupFixture(t, 1)
	started, release := make(chan struct{}), make(chan struct{})
	binding, _ := s.getCardBinding("om-native-group")
	binding.patcher = func(context.Context, string, map[string]interface{}) error {
		close(started)
		<-release
		return errors.New("platform outcome unknown")
	}
	done := make(chan error, 1)
	go func() {
		_, err := s.applyGroupAnnotation(context.Background(), "om-native-group", "cross-process-old-risk", ordinaryNotificationComponent(snapshot, "om-native-group"))
		done <- err
	}()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestNotificationFenceSubprocessHelper$")
	cmd.Env = append(os.Environ(), "JEV_FENCE_SUBPROCESS_CARD_DIR="+cfg.CardStateDir)
	output, err := cmd.CombinedOutput()
	close(release)
	if err != nil {
		t.Fatal(err, string(output))
	}
	if err := <-done; !errors.Is(err, ErrPatchStateUnknown) {
		t.Fatal(err)
	}
	projection := nativeProjection(t, s, cfg)
	if projection["lifecycle_revision"] != float64(1) || projection["patch_unknown"] != true || projection["annotation_revision"] != float64(0) {
		t.Fatal(projection)
	}
}

func TestNotificationSilentFenceRejectsWrongScopeAndEpisode(t *testing.T) {
	for _, mutate := range []func(*GroupAnnotationComponent){func(c *GroupAnnotationComponent) { c.Scope.ProfileVersion = "other" }, func(c *GroupAnnotationComponent) { c.Scope.Destination = "oc_other" }, func(c *GroupAnnotationComponent) { c.LifecycleMembers[0].EpisodeID = "other-episode" }} {
		s, _, snapshot, calls, _ := nativeGroupFixture(t, 1)
		c := silentLifecycle(snapshot, "om-native-group", 1, 0)
		mutate(&c)
		if _, err := s.applyGroupAnnotation(context.Background(), "om-native-group", "wrong-scope-lifecycle", c); !errors.Is(err, ErrRevision) {
			t.Fatal(err)
		}
		if calls.Load() != 0 {
			t.Fatal("invalid fence wrote platform")
		}
	}
}

// Optional real-data replay bridge. It runs only the actual consumer validator
// and pure renderer against local fixtures; it cannot invoke a platform sender.
func TestNotificationReplayContractFiles(t *testing.T) {
	path := os.Getenv("JEV_NOTIFICATION_REPLAY_INPUT")
	if path == "" {
		t.Skip("local replay bridge requires frozen input")
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	type sample struct {
		SampleID      string                    `json:"sample_id"`
		Component     *GroupAnnotationComponent `json:"component"`
		BaseCard      map[string]interface{}    `json:"base_card"`
		FinalSnapshot *FinalSnapshot            `json:"final_snapshot"`
		MessageID     string                    `json:"message_id"`
		SkipReason    string                    `json:"skip_reason"`
	}
	var samples []sample
	decoder := json.NewDecoder(io.LimitReader(file, 128<<20))
	if err := decoder.Decode(&samples); err != nil {
		t.Fatal(err)
	}
	if len(samples) < 1 || len(samples) > 1000 {
		t.Fatal("invalid local replay sample count")
	}
	results := make([]map[string]interface{}, 0, len(samples))
	failed := 0
	for _, s := range samples {
		out := map[string]interface{}{"sample_id": s.SampleID, "validation": "NOT_RUN_no_final_snapshot", "patch_count": 0, "visible_jev_blocks": 0, "card": s.BaseCard}
		if s.Component == nil {
			out["validation"] = "SKIPPED_no_annotation"
			out["skip_reason"] = s.SkipReason
			results = append(results, out)
			continue
		}
		if s.FinalSnapshot != nil && s.MessageID != "" {
			if err := validateFinalSnapshot(s.FinalSnapshot); err != nil {
				out["validation"] = "FAIL"
				out["error"] = err.Error()
				failed++
				results = append(results, out)
				continue
			}
			if err := s.Component.validate(s.FinalSnapshot, s.MessageID, s.BaseCard); err != nil {
				out["validation"] = "FAIL"
				out["error"] = err.Error()
				failed++
				results = append(results, out)
				continue
			}
			out["validation"] = "PASS"
		}
		if s.Component.Kind == "lifecycle_only" || s.Component.Kind == "lifecycle" {
			out["visible_jev_blocks"] = 0
			results = append(results, out)
			continue
		}
		frozen, err := immutableGroupRender(s.BaseCard, s.FinalSnapshot, *s.Component)
		if err == nil {
			var card map[string]interface{}
			card, err = renderGroupCard(frozen)
			if err == nil {
				out["card"] = card
				out["visible_jev_blocks"] = 1
			}
		}
		if err != nil {
			out["validation"] = "FAIL"
			out["error"] = err.Error()
			failed++
		}
		results = append(results, out)
	}
	output := os.Getenv("JEV_NOTIFICATION_REPLAY_OUTPUT")
	if output == "" {
		t.Fatal("local replay output path required")
	}
	raw, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(output, append(raw, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	if failed > 0 {
		t.Fatalf("%d of %d frozen samples failed actual consumer validation/render", failed, len(samples))
	}
}

func TestNotificationLegacyLifecycleExactGroupLinkBecomesSilentWithoutDiscardingValidation(t *testing.T) {
	s, cfg, snapshot, calls, _ := nativeGroupFixture(t, 1)
	c := nativeLifecycle(snapshot, "om-native-group", 1, 0, 1, 0)
	assessment := "ga1_legacy-pending"
	c.AssessmentID = &assessment
	c.DetailURL = "https://aiops.example/apps/alert-center/groups/" + assessment + "?domain=" + snapshot.PermissionDomain
	bad := c
	bad.AnnotationRevision = 0
	if _, err := s.applyGroupAnnotation(context.Background(), "om-native-group", "invalid-legacy-revision", bad); !errors.Is(err, ErrInvalidPayload) {
		t.Fatal(err)
	}
	if _, err := s.applyGroupAnnotation(context.Background(), "om-native-group", "legacy-old-link-control", c); err != nil {
		t.Fatal(err)
	}
	projection := nativeProjection(t, s, cfg)
	if calls.Load() != 0 || projection["annotation_revision"] != float64(0) || projection["lifecycle_revision"] != float64(1) {
		t.Fatal(projection)
	}
	fence := projection["lifecycle_fence"].(map[string]interface{})
	if fence["detail_url"] != "https://aiops.example/apps/alert-center/traces/"+assessment+"?domain="+snapshot.PermissionDomain {
		t.Fatal(fence)
	}
}
