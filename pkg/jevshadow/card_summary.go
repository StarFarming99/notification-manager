package jevshadow

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/kubesphere/notification-manager/pkg/template"
)

const (
	rendererLegacyMember = "legacy-member-v1"
	rendererCardSummary  = "card-summary-v1"
)

// This is executor-local durable state, not a new receipt/annotation wire DTO.
// It is captured from the exact successfully delivered Alert set, using the
// same canonical identity functions as the receipt. It never parses card text.
type cardMemberScope struct {
	ExpectedCardBindingID string            `json:"expected_card_binding_id"`
	PermissionDomain      string            `json:"permission_domain"`
	ProfileID             string            `json:"profile_id"`
	ProfileVersion        string            `json:"profile_version"`
	CardOwnerID           string            `json:"card_owner_id"`
	SenderApp             string            `json:"sender_app"`
	Receiver              string            `json:"receiver"`
	Destination           string            `json:"destination"`
	BaseRevision          int               `json:"base_revision"`
	BaseCardHash          string            `json:"base_card_hash"`
	Complete              bool              `json:"complete"`
	Members               []cardScopeMember `json:"members"`
}

type cardScopeMember struct {
	EventID     string    `json:"event_id"`
	EpisodeID   string    `json:"episode_id"`
	Fingerprint string    `json:"fingerprint"`
	StartsAt    time.Time `json:"starts_at"`
	Status      string    `json:"status"`
}

// Every input to a pending full-card PATCH is frozen with that intent. Restart
// reconstruction must not consult a mutable card binding, database or clock.
type cardRenderSnapshot struct {
	ExpectedCardBindingID string                         `json:"expected_card_binding_id,omitempty"`
	BaseCard              map[string]interface{}         `json:"base_card"`
	Annotations           map[string]AnnotationComponent `json:"annotations"`
	Scope                 *cardMemberScope               `json:"scope,omitempty"`
	DetailURL             string                         `json:"detail_url,omitempty"`
}

func (s *Service) cardScopeFromReceipt(data *template.Data, receipt deliveryReceipt) (*cardMemberScope, error) {
	// Older noncanonical receipts have no authoritative occurrence event IDs.
	// Do not guess their event mapping from fingerprints or display text.
	if s.config.Environment != "production" {
		return nil, nil
	}
	canonical, err := s.canonicalData(data)
	if err != nil {
		return nil, err
	}
	actual := make(map[string]cardScopeMember, len(canonical.Alerts))
	for _, alert := range canonical.Alerts {
		episode, err := episodeV2(s.config.LogicalSource, s.config.Environment, s.config.PermissionDomain, alert.ID, alert.StartsAt)
		if err != nil {
			return nil, err
		}
		id := occurrenceV2(episode, alert)
		actual[id] = cardScopeMember{EventID: id, EpisodeID: episode, Fingerprint: alert.ID, StartsAt: alert.StartsAt.UTC(), Status: alert.Status}
	}
	scope := &cardMemberScope{ExpectedCardBindingID: cardBindingID(receipt.SenderApp, receipt.MessageID), PermissionDomain: receipt.PermissionDomain, ProfileID: receipt.ProfileID, ProfileVersion: receipt.ProfileVersion,
		CardOwnerID: receipt.CardOwnerID, SenderApp: receipt.SenderApp, Receiver: "", Destination: receipt.Destination,
		BaseRevision: receipt.BaseCardRevision, BaseCardHash: receipt.BaseCardSnapshotHash, Complete: true, Members: make([]cardScopeMember, 0, len(receipt.Members))}
	seen := make(map[string]bool)
	for _, member := range receipt.Members {
		value, ok := actual[member.EventID]
		if !ok || value.Fingerprint != member.Fingerprint || !value.StartsAt.Equal(member.StartsAt) {
			return nil, errors.New("receipt member identity cannot be bound")
		}
		if !seen[value.EventID] {
			scope.Members = append(scope.Members, value)
			seen[value.EventID] = true
		}
		if value.Status != "firing" && value.Status != "resolved" {
			scope.Complete = false
		}
	}
	sort.Slice(scope.Members, func(i, j int) bool { return scope.Members[i].EventID < scope.Members[j].EventID })
	if len(scope.Members) == 0 {
		scope.Complete = false
	}
	return scope, nil
}

func validateCardScope(scope *cardMemberScope, state cardPersistentState) error {
	baseHash, err := hashCanonical(state.BaseCard)
	if err != nil || scope.ExpectedCardBindingID != cardBindingID(state.SenderApp, state.MessageID) || scope.BaseCardHash != baseHash || scope.BaseRevision != state.BaseRevision ||
		scope.SenderApp != state.SenderApp || scope.Destination != state.Destination || scope.Receiver != state.Receiver || scope.PermissionDomain == "" {
		return errors.New("card receipt scope does not match base binding")
	}
	seen := make(map[string]bool)
	for _, member := range scope.Members {
		if member.EventID == "" || member.EpisodeID == "" || member.Fingerprint == "" || member.StartsAt.IsZero() || seen[member.EventID] {
			return errors.New("invalid card receipt member snapshot")
		}
		if scope.Complete && member.Status != "firing" && member.Status != "resolved" {
			return errors.New("complete card scope contains unknown member state")
		}
		seen[member.EventID] = true
	}
	if scope.Complete && len(scope.Members) == 0 {
		return errors.New("complete card scope is empty")
	}
	return nil
}

func newCardRenderSnapshot(base map[string]interface{}, annotations map[string]AnnotationComponent, scope *cardMemberScope, identity ...string) cardRenderSnapshot {
	// JSON round-trip also removes caller-owned slices, maps and pointers before
	// the intent is persisted or handed to an external platform patcher.
	encoded, _ := canonicalJSON(cardRenderSnapshot{BaseCard: base, Annotations: annotations, Scope: scope})
	var result cardRenderSnapshot
	_ = json.Unmarshal(encoded, &result)
	if scope != nil {
		result.ExpectedCardBindingID = scope.ExpectedCardBindingID
	}
	if len(identity) == 2 {
		result.ExpectedCardBindingID = cardBindingID(identity[0], identity[1])
	}
	result.DetailURL = cardDetailURL(result.Annotations, result.Scope, result.ExpectedCardBindingID)
	return result
}

var cardBindingPathID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,256}$`)

func cardDetailURL(annotations map[string]AnnotationComponent, scope *cardMemberScope, expectedBindingID string) string {
	if expectedBindingID == "" {
		return ""
	}
	selected := ""
	for _, component := range annotations {
		raw := component.detailURL()
		if raw == nil {
			continue
		}
		parsed, err := url.Parse(*raw)
		if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
			continue
		}
		const prefix = "/apps/alert-center/cards/"
		if !strings.HasPrefix(parsed.Path, prefix) || !cardBindingPathID.MatchString(strings.TrimPrefix(parsed.Path, prefix)) || strings.TrimPrefix(parsed.Path, prefix) != expectedBindingID {
			continue
		}
		if parsed.Query().Get("domain") == "" || (scope != nil && parsed.Query().Get("domain") != scope.PermissionDomain) {
			continue
		}
		if selected != "" && selected != *raw {
			return ""
		}
		selected = *raw
	}
	return selected
}

type cardSummaryCounts struct {
	Total, Firing, Resolved, Received, Oncall, Pool, Missing, Unavailable, Protected, Conflicts int
	ScopeKnown                                                                                  bool
	Single                                                                                      *AnnotationComponent
}

func summarizeCard(snapshot cardRenderSnapshot) cardSummaryCounts {
	counts := cardSummaryCounts{ScopeKnown: snapshot.Scope != nil && snapshot.Scope.Complete}
	candidates := make(map[string][]AnnotationComponent)
	authoritative := make(map[string]bool)
	if snapshot.Scope != nil {
		for _, member := range snapshot.Scope.Members {
			authoritative[member.EventID] = true
		}
	}
	for _, component := range snapshot.Annotations {
		if snapshot.Scope != nil {
			inside, outside := false, false
			for _, id := range component.MemberEventIDs {
				if authoritative[id] {
					inside = true
				} else {
					outside = true
				}
			}
			if inside && outside {
				counts.ScopeKnown = false
			}
		}
		for _, memberID := range component.MemberEventIDs {
			candidates[memberID] = append(candidates[memberID], component)
		}
	}
	members := []cardScopeMember{}
	if snapshot.Scope != nil {
		members = snapshot.Scope.Members
	} else {
		ids := make([]string, 0, len(candidates))
		for id := range candidates {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			members = append(members, cardScopeMember{EventID: id, Status: "firing"})
		}
	}
	// An explicit resolved observation wins within the same actual episode. A
	// stale firing occurrence may remain stored but cannot become active again.
	resolvedEpisodes := make(map[string]bool)
	for _, member := range members {
		if member.Status == "resolved" && member.EpisodeID != "" {
			resolvedEpisodes[member.EpisodeID] = true
		}
	}
	for _, member := range members {
		counts.Total++
		if member.Status == "resolved" || resolvedEpisodes[member.EpisodeID] {
			counts.Resolved++
			continue
		}
		if member.Status != "firing" {
			counts.Missing++
			continue
		}
		counts.Firing++
		values := candidates[member.EventID]
		if len(values) == 0 {
			counts.Missing++
			continue
		}
		// Different overlapping component scopes are ambiguous. Keep every original
		// component, but do not count an arbitrary last answer as full coverage.
		if len(values) != 1 {
			counts.Conflicts++
			counts.Missing++
			for _, candidate := range values {
				policy := candidate.PolicyProposal
				if candidate.SchemaVersion == "1" {
					policy = candidate.Route
				}
				if policy == "oncall" {
					counts.Oncall++
					break
				}
			}
			continue
		}
		component := values[0]
		counts.Received++
		if counts.Total == 1 {
			copy := component
			counts.Single = &copy
		}
		if protectedReason(component.PolicyReasonCode) || component.Category == "protected" {
			counts.Protected++
		}
		policy := component.PolicyProposal
		if component.SchemaVersion == "1" {
			policy = component.Route
		}
		switch policy {
		case "oncall":
			counts.Oncall++
		case "pool":
			if !protectedReason(component.PolicyReasonCode) && component.Category != "protected" {
				counts.Pool++
			}
		}
		if policy != "pool" && policy != "oncall" || component.ModelStatus == "unavailable" {
			counts.Unavailable++
		}
	}
	if counts.Total != 1 || counts.Resolved != 0 {
		counts.Single = nil
	}
	return counts
}

func renderCardSummary(snapshot cardRenderSnapshot) (map[string]interface{}, error) {
	card, err := cloneMap(snapshot.BaseCard)
	if err != nil {
		return nil, err
	}
	elements, ok := card["elements"].([]interface{})
	if !ok {
		return nil, errors.New("base card has no elements array")
	}
	counts := summarizeCard(snapshot)
	title := "判断未完整，保持原通知"
	switch {
	case counts.ScopeKnown && counts.Total > 0 && counts.Resolved == counts.Total:
		title = "本卡告警已恢复（以原告警状态为准）"
	case counts.Oncall > 0:
		title = "建议人工处理"
	case counts.ScopeKnown && counts.Firing > 0 && counts.Pool == counts.Firing && counts.Missing == 0 && counts.Unavailable == 0 && counts.Protected == 0 && counts.Conflicts == 0:
		title = "全部有效告警建议入池（覆盖完整）"
	}
	lines := []string{"**Jev：" + title + "**"}
	if counts.ScopeKnown {
		lines = append(lines, fmt.Sprintf("本卡%d条告警 · 已收到%d条判断 · 其余%d条尚无有效结果", counts.Total, counts.Received, counts.Missing))
		if counts.Resolved > 0 {
			lines = append(lines, fmt.Sprintf("已恢复%d条（无需等待判断） · firing %d条", counts.Resolved, counts.Firing))
		}
	} else {
		lines = append(lines, fmt.Sprintf("已收到%d条判断，总范围未知", counts.Received))
	}
	lines = append(lines, fmt.Sprintf("已判断：人工处理%d条 / 建议入池%d条", counts.Oncall, counts.Pool))
	if counts.Unavailable > 0 || counts.Protected > 0 || counts.Conflicts > 0 {
		lines = append(lines, fmt.Sprintf("不可用%d条 / 受保护%d条 / 冲突%d条", counts.Unavailable, counts.Protected, counts.Conflicts))
	}
	if counts.Single != nil {
		lines = append(lines, counts.Single.markdown())
	}
	lines = append(lines, "仍按原告警流程通知")
	if snapshot.DetailURL == "" {
		lines = append(lines, "本卡详情暂不可用")
	}
	elements = append(elements, map[string]interface{}{"tag": "hr"}, map[string]interface{}{"tag": "div", "text": map[string]interface{}{"tag": "lark_md", "content": strings.Join(lines, "\n")}})
	if snapshot.DetailURL != "" {
		elements = append(elements, map[string]interface{}{"tag": "action", "actions": []interface{}{map[string]interface{}{"tag": "button", "text": map[string]interface{}{"tag": "plain_text", "content": "查看本卡全部判断"}, "type": "default", "url": snapshot.DetailURL}}})
	}
	card["elements"] = elements
	return card, nil
}

// Matches Jev identity.digest_id("card", sender_app, message_id), with no
// display labels, alertname or group key participating in the binding.
func cardBindingID(senderApp, messageID string) string {
	encoded, _ := canonicalJSON([]string{senderApp, messageID})
	digest := sha256.Sum256(encoded)
	return "card_" + hex.EncodeToString(digest[:])[:32]
}
