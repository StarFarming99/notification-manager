package jevshadow

func (s *Service) groupCardProjection(state cardPersistentState) map[string]interface{} {
	pending := state.PendingPatch != nil || state.PendingGroupPatch != nil
	unknown := state.PatchSendState == patchSendSending || state.PatchSendState == patchSendUnknown
	current := "not-read"
	if pending || unknown {
		current = "unknown"
	}
	renderer := "unknown"
	var component interface{}
	var payloadHash, request string
	if state.ConfirmedRendererVersion == rendererGroupCard && state.GroupComponent != nil {
		renderer = rendererGroupCard
		component = state.GroupComponent
		payloadHash, _ = groupPayloadHash(state.GroupComponent)
		for _, applied := range state.AppliedKeys {
			if applied.PayloadHash == payloadHash {
				request = applied.RequestID
				break
			}
		}
	}
	baseHash, _ := hashCanonical(state.BaseCard)
	projection := map[string]interface{}{"schema_version": rendererGroupCard, "perspective": "last-confirmed-executor-state", "platform_current": current, "renderer_version": renderer, "permission_domain": s.config.PermissionDomain, "execution_domain": s.config.ExecutionDomain, "profile_id": s.config.expectedProfileID, "profile_version": s.config.expectedProfileVersion, "card_owner_id": s.config.CardOwnerID, "receiver": state.Receiver, "sender_app": state.SenderApp, "destination": state.Destination, "message_id": state.MessageID, "card_binding_id": cardBindingID(state.SenderApp, state.MessageID), "base_revision": state.BaseRevision, "base_card_hash": baseHash, "annotation_revision": state.AnnotationRevision, "lifecycle_revision": state.GroupLifecycleRevision, "group_component": component, "group_payload_hash": payloadHash, "request_id": request, "scope_known": state.FinalSnapshot != nil, "pending": pending, "patch_unknown": unknown, "pending_annotation_revision": nil, "snapshot_id": nil, "members_hash": nil, "logical_group_id": nil}
	scope := GroupAnnotationScope{PermissionDomain: s.config.PermissionDomain, ExecutionDomain: s.config.ExecutionDomain, ProfileID: s.config.expectedProfileID, ProfileVersion: s.config.expectedProfileVersion, CardOwnerID: s.config.CardOwnerID, Receiver: state.Receiver, SenderApp: state.SenderApp, Destination: state.Destination}
	projection["scope"] = scope
	if state.FinalSnapshot != nil {
		projection["snapshot_id"] = state.FinalSnapshot.SnapshotID
		projection["members_hash"] = state.FinalSnapshot.MembersHash
		projection["logical_group_id"] = state.FinalSnapshot.LogicalGroupID
		projection["member_count"] = state.FinalSnapshot.MemberCount
	}
	if state.PendingGroupPatch != nil {
		projection["pending_annotation_revision"] = state.PendingGroupPatch.Component.AnnotationRevision
	} else if state.PendingPatch != nil {
		projection["pending_annotation_revision"] = state.PendingPatch.AnnotationRevision
	}
	return projection
}
