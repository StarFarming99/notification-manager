package jevshadow

import (
	"errors"
	"net/http"
	"sort"
	"strconv"
)

const maxCardProjectionMembers = 1000

func cardProjectionPage(r *http.Request) (int, int, error) {
	offset, limit := 0, maxCardProjectionMembers
	if raw := r.URL.Query().Get("member_offset"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v < 0 {
			return 0, 0, errors.New("invalid card member offset")
		}
		offset = v
	}
	if raw := r.URL.Query().Get("member_limit"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v < 1 || v > maxCardProjectionMembers {
			return 0, 0, errors.New("card member limit must be between 1 and 1000")
		}
		limit = v
	}
	return offset, limit, nil
}

// confirmedCardProjection is an authenticated, opt-in view of the one loaded
// durable state. It does not read the platform or include the pending intent's
// component. Even after an unknown PATCH it describes only the last confirmed
// executor rendering, never claims what the platform currently displays.
func (s *Service) confirmedCardProjection(state cardPersistentState, offset, limit int) map[string]interface{} {
	snapshot := newCardRenderSnapshot(state.BaseCard, state.Annotations, state.MemberScope, state.SenderApp, state.MessageID)
	counts := summarizeCard(snapshot)
	known := state.ConfirmedRendererVersion == rendererCardSummary && state.AnnotationRevision > 0
	renderer := "unknown"
	reason := "renderer_unrecorded"
	if known {
		renderer = rendererCardSummary
		reason = ""
	} else if state.AnnotationRevision == 0 {
		reason = "no_confirmed_annotation"
	}
	pending := state.PendingPatch != nil || state.PendingGroupPatch != nil
	unknownPatch := state.PatchSendState == patchSendSending || state.PatchSendState == patchSendUnknown
	platformCurrent := "not-read"
	if pending || unknownPatch {
		platformCurrent = "unknown"
	}
	var pendingRevision interface{}
	if state.PendingPatch != nil {
		pendingRevision = state.PendingPatch.AnnotationRevision
	} else if state.PendingGroupPatch != nil {
		pendingRevision = state.PendingGroupPatch.Component.AnnotationRevision
	}
	baseHash, _ := hashCanonical(state.BaseCard)
	projection := map[string]interface{}{
		"schema_version": rendererCardSummary, "perspective": "last-confirmed-executor-state", "platform_current": platformCurrent,
		"permission_domain": s.config.PermissionDomain, "execution_domain": s.config.ExecutionDomain, "profile_id": s.config.expectedProfileID,
		"profile_version": s.config.expectedProfileVersion, "card_owner_id": s.config.CardOwnerID, "sender_app": state.SenderApp,
		"destination": state.Destination, "message_id": state.MessageID, "card_binding_id": cardBindingID(state.SenderApp, state.MessageID),
		"base_card_hash": baseHash, "base_revision": state.BaseRevision, "annotation_revision": state.AnnotationRevision, "renderer_version": renderer,
		"scope_known": known && counts.ScopeKnown, "counts_available": known, "unknown_reason": reason,
		"pending": pending, "patch_unknown": unknownPatch, "pending_annotation_revision": pendingRevision,
		"total": nil, "firing": nil, "resolved": nil, "received": nil, "oncall": nil, "pool": nil, "missing": nil, "unavailable": nil, "protected": nil, "conflicts": nil,
	}
	if known {
		if counts.ScopeKnown {
			projection["total"] = counts.Total
			projection["firing"] = counts.Firing
			projection["resolved"] = counts.Resolved
		}
		projection["received"] = counts.Received
		projection["oncall"] = counts.Oncall
		projection["pool"] = counts.Pool
		projection["missing"] = counts.Missing
		projection["unavailable"] = counts.Unavailable
		projection["protected"] = counts.Protected
		projection["conflicts"] = counts.Conflicts
	}
	candidates := make(map[string][]AnnotationComponent)
	for _, component := range state.Annotations {
		for _, id := range component.MemberEventIDs {
			candidates[id] = append(candidates[id], component)
		}
	}
	members := []cardScopeMember{}
	if state.MemberScope != nil {
		members = state.MemberScope.Members
	} else {
		for id := range candidates {
			members = append(members, cardScopeMember{EventID: id, Status: "unknown"})
		}
	}
	// A private copy prevents a GET from sorting a loaded scope in place.
	members = append([]cardScopeMember(nil), members...)
	sort.Slice(members, func(i, j int) bool { return members[i].EventID < members[j].EventID })
	resolvedEpisodes := make(map[string]bool)
	for _, member := range members {
		if member.Status == "resolved" && member.EpisodeID != "" {
			resolvedEpisodes[member.EpisodeID] = true
		}
	}
	total := len(members)
	start := offset
	if start > total {
		start = total
	}
	end := start + limit
	if end > total {
		end = total
	}
	rows := make([]map[string]interface{}, 0, end-start)
	for _, member := range members[start:end] {
		row := map[string]interface{}{"event_id": member.EventID, "status": member.Status, "judgment_id": nil, "annotation_revision": nil, "final_policy": nil, "result_state": "unknown"}
		values := candidates[member.EventID]
		switch {
		case !known:
		case member.Status == "resolved" || (member.EpisodeID != "" && resolvedEpisodes[member.EpisodeID]):
			row["status"] = "resolved"
			row["result_state"] = "resolved"
		case len(values) == 0:
			row["result_state"] = "missing"
		case len(values) > 1:
			row["result_state"] = "conflict"
		default:
			component := values[0]
			row["result_state"] = "confirmed"
			row["annotation_revision"] = component.AnnotationRevision
			if component.SchemaVersion == "2" {
				if component.JudgmentID != "" {
					row["judgment_id"] = component.JudgmentID
				}
				row["final_policy"] = component.PolicyProposal
			} else {
				if len(component.JudgmentRefs) == 1 {
					row["judgment_id"] = component.JudgmentRefs[0]
				}
				if component.Route != "" {
					row["final_policy"] = component.Route
				}
			}
		}
		rows = append(rows, row)
	}
	var next interface{}
	if end < total {
		next = end
	}
	projection["members"] = rows
	projection["member_offset"] = offset
	projection["member_limit"] = limit
	projection["member_total"] = total
	projection["next_offset"] = next
	return projection
}
