package jevshadow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const rendererGroupCard = "group-card-v1"
const groupAnnotationCapability = "group-judgment-v1"
const maxGroupAnnotationBytes = 1 << 20

var groupDecisions = []string{"all_can_wait", "needs_action", "needs_member_verification", "insufficient_evidence"}

type GroupAnnotationScope struct {
	PermissionDomain string `json:"permission_domain"`
	ExecutionDomain  string `json:"execution_domain"`
	ProfileID        string `json:"profile_id"`
	ProfileVersion   string `json:"profile_version"`
	CardOwnerID      string `json:"card_owner_id"`
	Receiver         string `json:"receiver"`
	SenderApp        string `json:"sender_app"`
	Destination      string `json:"destination"`
}
type GroupAnnotationVersions struct {
	Template      string `json:"template"`
	Compiler      string `json:"compiler"`
	Questions     string `json:"questions"`
	QuestionsHash string `json:"questions_hash"`
	Policy        string `json:"policy"`
	ConfigHash    string `json:"config_hash"`
}
type GroupRiskResult struct {
	Decision           string             `json:"decision"`
	PolicyAction       string             `json:"policy_action"`
	Probabilities      map[string]float64 `json:"probabilities"`
	ExceptionMemberIDs []string           `json:"exception_member_ids"`
}

// Group probabilities preserve TypeSafe's explicit two-decimal response. They
// are never renormalized: only 0.99/1.00/1.01 totals whose rounding intervals can
// contain one are accepted. Keep the legacy component validator unchanged.
func validateGroupDistribution(result *GroupRiskResult) error {
	if result == nil || len(result.Probabilities) != len(groupDecisions) {
		return ErrInvalidPayload
	}
	chosen, ok := result.Probabilities[result.Decision]
	if !ok || !validProbability(chosen) {
		return ErrInvalidPayload
	}
	totalCents := 0
	lower, upper := 0.0, 0.0
	for _, choice := range groupDecisions {
		value, ok := result.Probabilities[choice]
		if !ok || !validProbability(value) || chosen < value || math.Abs(value*100-math.Round(value*100)) > 1e-9 {
			return ErrInvalidPayload
		}
		totalCents += int(math.Round(value * 100))
		lower += math.Max(0, value-0.005)
		upper += math.Min(1, value+0.005)
	}
	if totalCents < 99 || totalCents > 101 || lower > 1+1e-9 || upper < 1-1e-9 {
		return ErrInvalidPayload
	}
	return nil
}

type GroupLifecycleMember struct {
	EventID           string  `json:"event_id"`
	EpisodeID         string  `json:"episode_id"`
	SentStatus        string  `json:"sent_status"`
	CurrentStatus     string  `json:"current_status"`
	ResolutionEventID *string `json:"resolution_event_id"`
	ResolvedAt        *string `json:"resolved_at"`
	Reason            string  `json:"reason"`
}
type GroupAnnotationComponent struct {
	SchemaVersion              string                  `json:"schema_version"`
	Capability                 string                  `json:"capability"`
	Kind                       string                  `json:"kind"`
	AnnotationRevision         int                     `json:"annotation_revision"`
	ExpectedAnnotationRevision int                     `json:"expected_annotation_revision"`
	ExpectedBaseRevision       int                     `json:"expected_base_revision"`
	ExpectedBaseHash           string                  `json:"expected_base_hash"`
	SnapshotID                 string                  `json:"snapshot_id"`
	MembersHash                string                  `json:"members_hash"`
	AssessmentID               *string                 `json:"assessment_id"`
	LifecycleRevision          int                     `json:"lifecycle_revision"`
	ExpectedLifecycleRevision  int                     `json:"expected_lifecycle_revision"`
	Scope                      GroupAnnotationScope    `json:"scope"`
	AsOf                       string                  `json:"as_of"`
	MemberCount                int                     `json:"member_count"`
	ActiveMemberCount          int                     `json:"active_member_count"`
	ResolvedMemberCount        int                     `json:"resolved_member_count"`
	GroupResult                *GroupRiskResult        `json:"group_result"`
	Versions                   GroupAnnotationVersions `json:"versions"`
	LifecycleMembers           []GroupLifecycleMember  `json:"lifecycle_members"`
	DetailURL                  string                  `json:"detail_url"`
	ReasonCode                 string                  `json:"reason_code"`
}

type groupRenderSnapshot struct {
	BaseCard      map[string]interface{}   `json:"base_card"`
	FinalSnapshot *FinalSnapshot           `json:"final_snapshot"`
	Component     GroupAnnotationComponent `json:"component"`
}
type groupCardPatchIntent struct {
	RendererVersion  string                   `json:"renderer_version"`
	IdempotencyKey   string                   `json:"idempotency_key"`
	PayloadHash      string                   `json:"payload_hash"`
	RequestID        string                   `json:"request_id"`
	Component        GroupAnnotationComponent `json:"component"`
	RenderSnapshot   groupRenderSnapshot      `json:"render_snapshot"`
	RenderedCard     map[string]interface{}   `json:"rendered_card"`
	RenderedCardHash string                   `json:"rendered_card_hash"`
	StartedAt        time.Time                `json:"started_at"`
}

func decodeGroupAnnotation(reader io.Reader) (GroupAnnotationComponent, error) {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	var component GroupAnnotationComponent
	if err := decoder.Decode(&component); err != nil {
		return component, ErrInvalidPayload
	}
	if decoder.Decode(new(interface{})) != io.EOF {
		return component, ErrInvalidPayload
	}
	return component, nil
}
func parseGroupTime(value string) (time.Time, error) {
	v, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return v, err
	}
	_, offset := v.Zone()
	if offset != 0 {
		return v, errors.New("UTC timestamp required")
	}
	return v, nil
}
func (c GroupAnnotationComponent) validate(snapshot *FinalSnapshot, messageID string, base map[string]interface{}) error {
	if snapshot == nil || c.SchemaVersion != "3" || c.Capability != groupAnnotationCapability || (c.Kind != "group_judgment" && c.Kind != "lifecycle") || c.AnnotationRevision < 1 || c.ExpectedAnnotationRevision < 0 || c.AnnotationRevision <= c.ExpectedAnnotationRevision || c.ExpectedBaseRevision < 1 || c.LifecycleRevision < 0 || c.ExpectedLifecycleRevision < 0 {
		return ErrInvalidPayload
	}
	baseHash, err := hashCanonical(base)
	if err != nil || c.ExpectedBaseHash != baseHash || c.SnapshotID != snapshot.SnapshotID || c.MembersHash != snapshot.MembersHash {
		return ErrRevision
	}
	if c.Scope.PermissionDomain != snapshot.PermissionDomain || c.Scope.ExecutionDomain != snapshot.ExecutionDomain || c.Scope.ProfileID != snapshot.ProfileID || c.Scope.ProfileVersion != snapshot.ProfileVersion || c.Scope.CardOwnerID != snapshot.CardOwnerID || c.Scope.Receiver != snapshot.Receiver || c.Scope.SenderApp != snapshot.SenderApp || c.Scope.Destination != snapshot.Destination {
		return ErrRevision
	}
	if c.MemberCount != snapshot.MemberCount || len(c.LifecycleMembers) != snapshot.MemberCount || len(c.LifecycleMembers) > MaxFinalSnapshotMembers {
		return ErrInvalidPayload
	}
	if _, err := parseGroupTime(c.AsOf); err != nil || len(c.ReasonCode) < 1 || len(c.ReasonCode) > 200 {
		return ErrInvalidPayload
	}
	if len(c.DetailURL) > 2000 {
		return ErrInvalidPayload
	}
	parsed, urlErr := url.Parse(c.DetailURL)
	if urlErr != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
		return ErrInvalidPayload
	}
	query, queryErr := url.ParseQuery(parsed.RawQuery)
	if queryErr != nil || len(query) != 1 || len(query["domain"]) != 1 || query.Get("domain") != snapshot.PermissionDomain {
		return ErrInvalidPayload
	}
	expectedPath := "/apps/alert-center/cards/" + cardBindingID(snapshot.SenderApp, messageID)
	// A status-only assessment is still an exact group subject, even though
	// it has no model result. Never accept an unrelated arbitrary group URL.
	if c.AssessmentID != nil {
		if !cardBindingPathID.MatchString(*c.AssessmentID) {
			return ErrInvalidPayload
		}
		expectedPath = "/apps/alert-center/groups/" + *c.AssessmentID
	}
	if parsed.Path != expectedPath {
		return ErrInvalidPayload
	}
	members := map[string]FinalSnapshotMember{}
	for _, member := range snapshot.Members {
		members[member.EventID] = member
	}
	seen := map[string]bool{}
	active, resolved := 0, 0
	activeIDs := map[string]bool{}
	for _, member := range c.LifecycleMembers {
		original, ok := members[member.EventID]
		if !ok || seen[member.EventID] || member.EpisodeID != original.EpisodeID || member.SentStatus != original.Status || len(member.Reason) > 1000 {
			return ErrRevision
		}
		seen[member.EventID] = true
		if original.Status == "resolved" && member.CurrentStatus != "resolved" {
			return ErrRevision
		}
		switch member.CurrentStatus {
		case "firing":
			active++
			activeIDs[member.EventID] = true
			if member.ResolutionEventID != nil || member.ResolvedAt != nil {
				return ErrInvalidPayload
			}
		case "resolved":
			resolved++
			if member.ResolutionEventID == nil || !strings.HasPrefix(*member.ResolutionEventID, "occ2_") || member.ResolvedAt == nil {
				return ErrInvalidPayload
			}
			end, err := parseGroupTime(*member.ResolvedAt)
			start, startErr := parseGroupTime(original.StartsAt)
			asOf, _ := parseGroupTime(c.AsOf)
			if err != nil || startErr != nil || end.Before(start) || end.After(asOf) || (original.Status == "firing" && *member.ResolutionEventID == member.EventID) {
				return ErrInvalidPayload
			}
		case "unknown":
			if member.ResolutionEventID != nil || member.ResolvedAt != nil {
				return ErrInvalidPayload
			}
		default:
			return ErrInvalidPayload
		}
	}
	if c.ActiveMemberCount != active || c.ResolvedMemberCount != resolved {
		return ErrInvalidPayload
	}
	if c.Kind == "lifecycle" {
		if c.GroupResult != nil || c.LifecycleRevision <= c.ExpectedLifecycleRevision {
			return ErrInvalidPayload
		}
	} else {
		if c.GroupResult == nil || c.AssessmentID == nil || strings.TrimSpace(*c.AssessmentID) == "" || len(*c.AssessmentID) > 200 || active < 1 || c.LifecycleRevision != c.ExpectedLifecycleRevision {
			return ErrInvalidPayload
		}
		result := c.GroupResult
		if err := validateGroupDistribution(result); err != nil {
			return err
		}
		if result.PolicyAction != "pool_candidate" && result.PolicyAction != "oncall" && result.PolicyAction != "original" {
			return ErrInvalidPayload
		}
		if result.PolicyAction == "pool_candidate" && (result.Decision != "all_can_wait" || active+resolved != c.MemberCount || len(result.ExceptionMemberIDs) > 0) {
			return ErrInvalidPayload
		}
		exceptions := map[string]bool{}
		for _, id := range result.ExceptionMemberIDs {
			if !activeIDs[id] || exceptions[id] {
				return ErrInvalidPayload
			}
			exceptions[id] = true
		}
		if c.Versions.Template == "" || c.Versions.Compiler == "" || c.Versions.Questions == "" || len(c.Versions.QuestionsHash) != 64 || c.Versions.Policy == "" || len(c.Versions.ConfigHash) != 64 {
			return ErrInvalidPayload
		}
	}
	return nil
}

func (s *Service) HandleGroupAnnotation(w http.ResponseWriter, r *http.Request, messageID string) {
	selected := s
	if len(s.scopeServices) > 0 {
		child, err := s.messageScope(messageID)
		if err != nil {
			w.WriteHeader(404)
			return
		}
		selected = child
	}
	if !selected.Enabled() || selected.cardStore == nil {
		w.WriteHeader(503)
		return
	}
	if !selected.authorized(r.Header.Get("Authorization")) {
		w.WriteHeader(401)
		return
	}
	if r.Method != http.MethodPut {
		w.WriteHeader(405)
		return
	}
	if !selected.config.groupProfileEnabled || !selected.config.annotationProfileEnabled || (selected.config.FormalWriterDisabled && selected.config.expectedProfileID == "formal") {
		w.WriteHeader(503)
		return
	}
	component, err := decodeGroupAnnotation(http.MaxBytesReader(w, r.Body, maxGroupAnnotationBytes))
	if err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if len(key) < 8 || len(key) > 256 {
		w.WriteHeader(400)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	id, err := selected.applyGroupAnnotation(ctx, messageID, key, component)
	if err != nil {
		switch {
		case errors.Is(err, ErrCardNotFound):
			w.WriteHeader(404)
		case errors.Is(err, ErrInvalidPayload):
			writeJSON(w, 400, map[string]string{"error": err.Error()})
		case errors.Is(err, ErrRevision), errors.Is(err, ErrPatchStateUnknown):
			writeJSON(w, 409, map[string]string{"error": err.Error()})
		case errors.Is(err, ErrCardWriterBusy):
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(429)
		case errors.Is(err, ErrPatchWriterPaused):
			w.WriteHeader(503)
		default:
			var deferred *patchBudgetDeferred
			if errors.As(err, &deferred) {
				w.Header().Set("Retry-After", fmt.Sprint(deferred.retrySeconds()))
				w.WriteHeader(429)
			} else {
				writeJSON(w, 502, map[string]string{"error": "group card update failed"})
			}
		}
		return
	}
	writeJSON(w, 200, map[string]string{"request_id": id})
}

func immutableGroupRender(base map[string]interface{}, snapshot *FinalSnapshot, component GroupAnnotationComponent) (groupRenderSnapshot, error) {
	raw, err := json.Marshal(groupRenderSnapshot{BaseCard: base, FinalSnapshot: snapshot, Component: component})
	if err != nil {
		return groupRenderSnapshot{}, err
	}
	var result groupRenderSnapshot
	err = json.Unmarshal(raw, &result)
	return result, err
}
func groupPayloadHash(value interface{}) (string, error) {
	raw, err := groupCanonicalJSON(value)
	if err != nil {
		return "", err
	}
	return rawSHA256(raw), nil
}

func (s *Service) applyGroupAnnotation(ctx context.Context, messageID, key string, component GroupAnnotationComponent) (string, error) {
	binding, err := s.getCardBinding(messageID)
	if err != nil {
		return "", err
	}
	binding.mu.Lock()
	defer binding.mu.Unlock()
	var resultID string
	err = s.cardStore.WithMessageLock(messageID, func() error {
		state, err := s.cardStore.Load(messageID)
		if err != nil {
			return err
		}
		if state.FinalSnapshot == nil {
			return ErrRevision
		}
		if state.PatchSendState == patchSendSending && (state.PendingPatch != nil || state.PendingGroupPatch != nil) {
			state.PatchSendState = patchSendUnknown
			state.LastPatchError = "writer stopped before the persisted patch intent was confirmed"
			if err := s.cardStore.Save(&state); err != nil {
				return err
			}
			return ErrPatchStateUnknown
		}
		if state.PendingPatch != nil || state.PendingGroupPatch != nil || state.PatchSendState == patchSendUnknown {
			return ErrPatchStateUnknown
		}
		if err := component.validate(state.FinalSnapshot, messageID, state.BaseCard); err != nil {
			return err
		}
		payloadHash, err := groupPayloadHash(component)
		if err != nil {
			return ErrInvalidPayload
		}
		if applied, ok := state.AppliedKeys[key]; ok {
			if applied.PayloadHash != payloadHash {
				return ErrRevision
			}
			resultID = applied.RequestID
			return nil
		}
		if component.ExpectedBaseRevision != state.BaseRevision || component.ExpectedAnnotationRevision != state.AnnotationRevision || component.AnnotationRevision <= state.AnnotationRevision || component.ExpectedLifecycleRevision != state.GroupLifecycleRevision {
			return ErrRevision
		}
		if component.Kind == "group_judgment" && component.LifecycleRevision != state.GroupLifecycleRevision {
			return ErrRevision
		}
		if component.Kind == "lifecycle" && component.LifecycleRevision <= state.GroupLifecycleRevision {
			return ErrRevision
		}
		if state.GroupComponent != nil {
			resolved := map[string]bool{}
			for _, member := range state.GroupComponent.LifecycleMembers {
				if member.CurrentStatus == "resolved" {
					resolved[member.EventID] = true
				}
			}
			for _, member := range component.LifecycleMembers {
				if resolved[member.EventID] && member.CurrentStatus != "resolved" {
					return ErrRevision
				}
			}
		}
		patcher, err := s.resolveCardPatcher(ctx, state.Receiver, state.Destination, binding.patcher)
		if err != nil {
			return err
		}
		frozen, err := immutableGroupRender(state.BaseCard, state.FinalSnapshot, component)
		if err != nil {
			return err
		}
		rendered, err := renderGroupCard(frozen)
		if err != nil {
			return err
		}
		renderedHash, err := hashCanonical(rendered)
		if err != nil {
			return err
		}
		resultID = requestID(messageID, key)
		return s.writerGate.withAttempt(ctx, messageID, func() error {
			state.PatchSendState = patchSendSending
			state.LastPatchError = ""
			state.PendingGroupPatch = &groupCardPatchIntent{RendererVersion: rendererGroupCard, IdempotencyKey: key, PayloadHash: payloadHash, RequestID: resultID, Component: component, RenderSnapshot: frozen, RenderedCard: rendered, RenderedCardHash: renderedHash, StartedAt: time.Now().UTC()}
			if err := s.cardStore.Save(&state); err != nil {
				return fmt.Errorf("%w: persist group intent: %v", ErrCardWriterUnavailable, err)
			}
			if patchErr := patcher(ctx, messageID, rendered); patchErr != nil {
				state.LastPatchError = boundedCardStateError(patchErr)
				state.PatchSendState = classifyPatchFailure(patchErr)
				if state.PatchSendState == patchSendFailed {
					state.PendingGroupPatch = nil
				}
				if err := s.cardStore.Save(&state); err != nil {
					return fmt.Errorf("%w: persist group failure: %v", ErrPatchStateUnknown, err)
				}
				if state.PatchSendState == patchSendUnknown {
					return ErrPatchStateUnknown
				}
				return patchErr
			}
			state.GroupComponent = &component
			state.GroupLifecycleRevision = component.LifecycleRevision
			state.ConfirmedRendererVersion = rendererGroupCard
			state.AnnotationRevision = component.AnnotationRevision
			state.AppliedKeys[key] = persistedAppliedRequest{PayloadHash: payloadHash, RequestID: resultID}
			state.PatchSendState = patchSendConfirmed
			state.PendingGroupPatch = nil
			state.LastPatchError = ""
			if err := s.cardStore.Save(&state); err != nil {
				return fmt.Errorf("%w: persist group confirmation: %v", ErrPatchStateUnknown, err)
			}
			syncBindingFromCardState(binding, state, patcher)
			return nil
		})
	})
	if err != nil {
		return "", err
	}
	return resultID, nil
}

func renderGroupCard(snapshot groupRenderSnapshot) (map[string]interface{}, error) {
	card, err := cloneMap(snapshot.BaseCard)
	if err != nil {
		return nil, err
	}
	elements, ok := card["elements"].([]interface{})
	if !ok {
		return nil, ErrInvalidPayload
	}
	c := snapshot.Component
	lines := []string{}
	if c.Kind == "lifecycle" {
		if c.ResolvedMemberCount == c.MemberCount {
			late := false
			for _, member := range c.LifecycleMembers {
				if member.SentStatus == "firing" {
					late = true
					break
				}
			}
			explanation := "原告警已恢复，未重新进行风险判断。"
			if late {
				explanation = "这是迟到的 firing 通知，未重新判断。"
			}
			lines = append(lines, "**Jev：该次告警已恢复**", explanation)
		} else {
			lines = append(lines, "**Jev：告警状态更新，保持原通知**")
		}
		lines = append(lines, fmt.Sprintf("本卡%d条成员 · 当前有效firing %d条 · 已确认恢复%d条 · 状态待核对%d条", c.MemberCount, c.ActiveMemberCount, c.ResolvedMemberCount, c.MemberCount-c.ActiveMemberCount-c.ResolvedMemberCount))
		// Keep card size independent of group length. Complete recovery evidence is
		// retained in the immutable component and detail, not N repeated blocks.
		latest := ""
		for _, member := range c.LifecycleMembers {
			if member.ResolvedAt != nil && *member.ResolvedAt > latest {
				latest = *member.ResolvedAt
			}
		}
		if latest != "" {
			lines = append(lines, "恢复依据截止："+latest)
		}
	} else {
		labels := map[string]string{"all_can_wait": "整组建议等待（仅旁路观察）", "needs_action": "整组需要处理", "needs_member_verification": "部分成员需要进一步核实，保持原通知", "insufficient_evidence": "证据不足，保持原通知"}
		conclusion := labels[c.GroupResult.Decision]
		if c.GroupResult.PolicyAction == "original" && c.GroupResult.Decision != "needs_member_verification" {
			conclusion = "证据不足，保持原通知"
		} else if c.GroupResult.PolicyAction == "oncall" {
			conclusion = "整组需要处理"
		}
		lines = append(lines, "**Jev 组判断："+conclusion+"**", fmt.Sprintf("覆盖%d条成员 · 有效firing %d条 · 已恢复%d条 · 待核对%d条 · 异常成员%d条", c.MemberCount, c.ActiveMemberCount, c.ResolvedMemberCount, c.MemberCount-c.ActiveMemberCount-c.ResolvedMemberCount, len(c.GroupResult.ExceptionMemberIDs)))
		lines = append(lines, fmt.Sprintf("组概率：可等待 %s · 需处理 %s · 待核实 %s · 证据不足 %s", formatProbability(c.GroupResult.Probabilities["all_can_wait"]), formatProbability(c.GroupResult.Probabilities["needs_action"]), formatProbability(c.GroupResult.Probabilities["needs_member_verification"]), formatProbability(c.GroupResult.Probabilities["insufficient_evidence"])))
		total := 0.0
		for _, choice := range groupDecisions {
			total += c.GroupResult.Probabilities[choice]
		}
		if math.Abs(total-1) > 1e-9 {
			lines = append(lines, fmt.Sprintf("概率为两位小数舍入值，合计%.0f%%，保持原始数值。", total*100))
		}
		lines = append(lines, "配置："+c.Versions.Template+" / "+c.Versions.Policy)
	}
	lines = append(lines, "证据截止："+c.AsOf, "仍按原告警流程通知")
	elements = append(elements, map[string]interface{}{"tag": "hr"}, map[string]interface{}{"tag": "div", "text": map[string]interface{}{"tag": "lark_md", "content": strings.Join(lines, "\n")}}, map[string]interface{}{"tag": "action", "actions": []interface{}{map[string]interface{}{"tag": "button", "type": "default", "text": map[string]interface{}{"tag": "plain_text", "content": "查看本卡全部判断与状态"}, "url": c.DetailURL}}})
	card["elements"] = elements
	return card, nil
}

// Compare immutable render inputs without consulting current state or DB.
func equalGroupSnapshot(a, b interface{}) bool {
	x, err := groupCanonicalJSON(a)
	if err != nil {
		return false
	}
	y, err := groupCanonicalJSON(b)
	return err == nil && bytes.Equal(x, y)
}

func validateGroupCardState(state cardPersistentState) error {
	if state.FinalSnapshot == nil {
		if state.GroupComponent != nil || state.PendingGroupPatch != nil || state.GroupLifecycleRevision != 0 || state.ConfirmedRendererVersion == rendererGroupCard {
			return errors.New("group card state has no immutable final snapshot")
		}
		return nil
	}
	if err := validateFinalSnapshot(state.FinalSnapshot); err != nil {
		return err
	}
	snapshot := state.FinalSnapshot
	if scope := state.MemberScope; scope != nil && (scope.PermissionDomain != snapshot.PermissionDomain || scope.ProfileID != snapshot.ProfileID || scope.ProfileVersion != snapshot.ProfileVersion || scope.CardOwnerID != snapshot.CardOwnerID || (scope.ExecutionDomain != "" && scope.ExecutionDomain != snapshot.ExecutionDomain)) {
		return errors.New("snapshot and canonical receipt scopes disagree")
	}
	if snapshot.SenderApp != state.SenderApp || snapshot.Receiver != state.Receiver || snapshot.Destination != state.Destination {
		return errors.New("final snapshot escaped card writer scope")
	}
	if state.GroupLifecycleRevision < 0 {
		return errors.New("invalid group lifecycle revision")
	}
	if state.PendingPatch != nil && state.PendingGroupPatch != nil {
		return errors.New("two card patch intents are forbidden")
	}
	if state.GroupComponent != nil {
		if err := state.GroupComponent.validate(snapshot, state.MessageID, state.BaseCard); err != nil {
			return fmt.Errorf("invalid confirmed group component: %w", err)
		}
		if state.GroupComponent.AnnotationRevision != state.AnnotationRevision || state.GroupComponent.ExpectedBaseRevision != state.BaseRevision || state.GroupComponent.LifecycleRevision != state.GroupLifecycleRevision || state.ConfirmedRendererVersion != rendererGroupCard {
			return errors.New("confirmed group revision or renderer mismatch")
		}
	} else if state.ConfirmedRendererVersion == rendererGroupCard {
		return errors.New("group renderer has no confirmed component")
	}
	if intent := state.PendingGroupPatch; intent != nil {
		if intent.RendererVersion != rendererGroupCard || (state.PatchSendState != patchSendSending && state.PatchSendState != patchSendUnknown) || intent.StartedAt.IsZero() || len(intent.IdempotencyKey) < 8 || len(intent.IdempotencyKey) > 256 || intent.RequestID != requestID(state.MessageID, intent.IdempotencyKey) {
			return errors.New("invalid group patch intent identity")
		}
		if err := intent.Component.validate(snapshot, state.MessageID, state.BaseCard); err != nil {
			return err
		}
		if intent.Component.ExpectedAnnotationRevision != state.AnnotationRevision || intent.Component.AnnotationRevision <= state.AnnotationRevision || intent.Component.ExpectedLifecycleRevision != state.GroupLifecycleRevision {
			return errors.New("group pending CAS mismatch")
		}
		if intent.Component.Kind == "group_judgment" && intent.Component.LifecycleRevision != state.GroupLifecycleRevision {
			return errors.New("stale risk pending lifecycle")
		}
		hash, err := groupPayloadHash(intent.Component)
		if err != nil || hash != intent.PayloadHash {
			return errors.New("group pending component hash mismatch")
		}
		renderedHash, err := hashCanonical(intent.RenderedCard)
		if err != nil || renderedHash != intent.RenderedCardHash {
			return errors.New("group rendered card hash mismatch")
		}
		expected, err := immutableGroupRender(state.BaseCard, snapshot, intent.Component)
		if err != nil || !equalGroupSnapshot(expected, intent.RenderSnapshot) {
			return errors.New("group pending frozen input mismatch")
		}
		rebuilt, err := renderGroupCard(intent.RenderSnapshot)
		if err != nil {
			return err
		}
		hash, err = hashCanonical(rebuilt)
		if err != nil || hash != intent.RenderedCardHash {
			return errors.New("group pending legacy/renderer reconstruction mismatch")
		}
	}
	return nil
}
