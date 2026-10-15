package jevshadow

import (
	"net/http"
	"strconv"
	"strings"
)

// HandleAnnotationStatus is a read-only projection of the durable receipt.
// It uses the same per-profile annotation credential, authenticates before
// reading a message, and never invokes a patcher or a callback consumer.
func (s *Service) HandleAnnotationStatus(w http.ResponseWriter, r *http.Request, messageID string) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	id, version, app := r.URL.Query().Get("profile_id"), r.URL.Query().Get("profile_version"), r.URL.Query().Get("sender_app")
	revision, err := strconv.Atoi(r.URL.Query().Get("annotation_revision"))
	if id == "" || version == "" || app == "" || err != nil || revision < 0 || len(messageID) > 256 || strings.TrimSpace(messageID) == "" {
		writeJSON(w, 400, map[string]string{"error": "exact profile, sender, message and non-negative revision required"})
		return
	}
	selected := s
	if len(s.scopeServices) > 0 {
		selected = s.scopeServices[scopeKey(id, version)]
	}
	if selected == nil || !selected.authorized(r.Header.Get("Authorization")) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	if selected.config.expectedProfileID != id || selected.config.expectedProfileVersion != version || selected.config.SenderApp != app {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	if !selected.Enabled() || selected.cardStore == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	state, err := selected.cardStore.Load(messageID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "durable message receipt unavailable"})
		return
	}
	if state.SenderApp != app || !selected.ShouldCapture(state.Receiver, state.Destination) {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	payloadHash, requestID := "", ""
	for _, component := range state.Annotations {
		if component.AnnotationRevision != revision {
			continue
		}
		hash, e := hashCanonical(component)
		if e != nil {
			w.WriteHeader(503)
			return
		}
		for _, receipt := range state.AppliedKeys {
			if receipt.PayloadHash == hash {
				payloadHash = hash
				requestID = receipt.RequestID
				break
			}
		}
	}
	baseHash, err := hashCanonical(state.BaseCard)
	if err != nil {
		w.WriteHeader(503)
		return
	}
	projection := r.URL.Query().Get("projection")
	if projection != "" && projection != rendererCardSummary {
		writeJSON(w, 400, map[string]string{"error": "unsupported annotation projection"})
		return
	}
	var projectionData map[string]interface{}
	if projection == rendererCardSummary {
		if scope := state.MemberScope; scope != nil && (scope.PermissionDomain != selected.config.PermissionDomain || scope.ProfileID != id || scope.ProfileVersion != version || scope.CardOwnerID != selected.config.CardOwnerID || (scope.ExecutionDomain != "" && scope.ExecutionDomain != selected.config.ExecutionDomain)) {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		offset, limit, parseErr := cardProjectionPage(r)
		if parseErr != nil {
			writeJSON(w, 400, map[string]string{"error": parseErr.Error()})
			return
		}
		projectionData = selected.confirmedCardProjection(state, offset, limit)
	}
	out := map[string]interface{}{
		"profile_id": id, "profile_version": version, "card_owner_id": selected.config.CardOwnerID,
		"sender_app": app, "message_id": messageID, "base_revision": state.BaseRevision,
		"requested_annotation_revision": revision, "annotation_revision": state.AnnotationRevision,
		"base_send_state": state.BaseSendState, "patch_send_state": state.PatchSendState,
		"base_card_hash": baseHash, "annotation_payload_hash": payloadHash, "request_id": requestID,
		"confirmed":  revision > 0 && payloadHash != "" && requestID != "" && state.AnnotationRevision >= revision,
		"updated_at": state.UpdatedAt,
	}
	if projectionData != nil {
		out["card_projection"] = projectionData
	}
	writeJSON(w, 200, out)
}
