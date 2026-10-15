package notification_spool

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"sort"
	"strings"

	bolt "go.etcd.io/bbolt"
)

// Feishu renders carry JSON as a string within FrozenNotification. Equivalent
// object key order/spacing must not invalidate a byte-hash-verified AM retry.
// Strings, arrays, numbers and credential/config values remain exact. Other
// channels retain their original byte identity.
func canonicalFeishuTarget(t Target) Target {
	if t.Channel != "feishu" || t.ContentRevision != Hash(t.Payload) {
		return t
	}
	raw, err := uniqueJSON(t.Payload)
	if err != nil {
		return t
	}
	payload, ok := raw.(map[string]interface{})
	if !ok {
		return t
	}
	content, ok := payload["content"].(string)
	if !ok {
		return t
	}
	decoded, err := uniqueJSON([]byte(content))
	if err != nil {
		return t
	}
	if _, ok := decoded.(map[string]interface{}); !ok {
		return t
	}
	normalized, err := json.Marshal(decoded)
	if err != nil {
		return t
	}
	payload["content"] = string(normalized)
	normalized, err = json.Marshal(payload)
	if err != nil {
		return t
	}
	t.Payload = normalized
	t.ContentRevision = Hash(normalized)
	return t
}

// Duplicate object keys are deliberately unsupported: silently selecting one
// occurrence could equate two messages that a platform interprets differently.
func uniqueJSON(raw []byte) (interface{}, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	v, err := readUniqueJSON(d)
	if err != nil {
		return nil, err
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, errors.New("JSON contains trailing tokens")
	}
	return v, nil
}
func readUniqueJSON(d *json.Decoder) (interface{}, error) {
	tok, err := d.Token()
	if err != nil {
		return nil, err
	}
	delimiter, ok := tok.(json.Delim)
	if !ok {
		return tok, nil
	}
	switch delimiter {
	case '{':
		out := map[string]interface{}{}
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return nil, err
			}
			name, ok := key.(string)
			if !ok {
				return nil, errors.New("invalid JSON object key")
			}
			if _, exists := out[name]; exists {
				return nil, errors.New("duplicate JSON object key")
			}
			v, err := readUniqueJSON(d)
			if err != nil {
				return nil, err
			}
			out[name] = v
		}
		end, err := d.Token()
		if err != nil || end != json.Delim('}') {
			return nil, errors.New("invalid JSON object")
		}
		return out, nil
	case '[':
		out := []interface{}{}
		for d.More() {
			v, err := readUniqueJSON(d)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		end, err := d.Token()
		if err != nil || end != json.Delim(']') {
			return nil, errors.New("invalid JSON array")
		}
		return out, nil
	default:
		return nil, errors.New("unexpected JSON delimiter")
	}
}

// recordAMObservation records what this NM actually observed. Native AM webhook
// payloads do not identify the notification generation across a content return.
// A changed current payload followed by an older payload is therefore accepted
// rather than silently tied to an older in-flight/unknown attempt.
func recordAMObservation(tx *bolt.Tx, in *Intake) error {
	meta, intakes, dedupe := tx.Bucket([]byte("meta")), tx.Bucket([]byte("intakes")), tx.Bucket([]byte("dedupe"))
	var seq uint64
	if raw := meta.Get([]byte("am-reception-sequence")); raw != nil {
		if err := json.Unmarshal(raw, &seq); err != nil {
			return err
		}
	}
	if seq == ^uint64(0) {
		return errors.New("AM reception sequence exhausted")
	}
	in.ReceptionSequence = seq + 1
	in.IdentityOrigin = "local-observation-v1"
	st, err := releaseRead(tx)
	if err != nil {
		return err
	}
	in.ReceptionInstance = st.InstanceID
	if in.ReceptionInstance == "" {
		in.ReceptionInstance = "process:" + string(meta.Get([]byte("owner")))
	}
	if previous := dedupe.Get([]byte(in.DedupeKey)); previous != nil {
		var current Intake
		if err := json.Unmarshal(intakes.Get(previous), &current); err != nil {
			return err
		}
		in.DeliveryOrder = current.DeliveryOrder
		if in.DeliveryOrder < current.Generation {
			in.DeliveryOrder = current.Generation
		}
	}
	in.DeliveryOrder++
	historical := dedupe.Get([]byte(in.DedupeKey + ":intent:" + in.RequestHash))
	if historical == nil {
		if split := strings.LastIndex(in.DedupeKey, ":am-group:"); split >= 0 {
			historical = dedupe.Get([]byte(in.DedupeKey[:split] + ":am:" + in.RequestHash))
		}
	}
	if historical != nil {
		var prior Intake
		if err := json.Unmarshal(intakes.Get(historical), &prior); err != nil {
			return err
		}
		if prior.RequestHash != in.RequestHash {
			return errors.New("AM historical index content conflict")
		}
		if current := dedupe.Get([]byte(in.DedupeKey)); current == nil || string(current) != prior.ID {
			in.AmbiguityReason = "content_return_or_delayed_peer"
			in.PossibleDuplicateOf = []string{prior.ID}
		} else if incomplete, err := intakeIncomplete(tx, prior); err != nil {
			return err
		} else if incomplete {
			in.AmbiguityReason = "repeat_window_elapsed_with_unsettled_predecessor"
			in.PossibleDuplicateOf = []string{prior.ID}
		}
	}
	if err := put(meta, "am-reception-sequence", in.ReceptionSequence); err != nil {
		return err
	}
	if in.AmbiguityReason != "" {
		return put(tx.Bucket([]byte("audit")), "am-observation:"+in.ID, map[string]interface{}{
			"action": "accept_ambiguous_am_candidate", "intake_id": in.ID,
			"group": in.DedupeKey, "request_hash": in.RequestHash,
			"reception_sequence": in.ReceptionSequence, "reception_instance": in.ReceptionInstance,
			"ambiguity_reason": in.AmbiguityReason, "possible_duplicate_of": in.PossibleDuplicateOf,
			"accepted_at": in.AcceptedAt,
		})
	}
	return nil
}

// markLegacyAMIntakes adds only a provenance classification. It does not invent
// a generation, request identity, timestamp or reception sequence for old rows.
// Old pending targets remain pending; unknown attempts remain quarantined.
func markLegacyAMIntakes(tx *bolt.Tx) error {
	legacy := map[string]bool{}
	if err := tx.Bucket([]byte("dedupe")).ForEach(func(k, v []byte) error {
		key := string(k)
		if strings.Contains(key, ":am:") || strings.Contains(key, ":am-group:") {
			legacy[string(v)] = true
		}
		return nil
	}); err != nil {
		return err
	}
	b := tx.Bucket([]byte("intakes"))
	for id := range legacy {
		var in Intake
		if err := json.Unmarshal(b.Get([]byte(id)), &in); err != nil {
			return err
		}
		if in.IdentityOrigin != "" || (in.AMGroup && in.Generation > 0) {
			continue
		}
		in.IdentityOrigin = "legacy-unsequenced"
		if err := put(b, id, in); err != nil {
			return err
		}
	}
	return nil
}

func appendPossibleDuplicate(in *Intake, id, reason string) {
	if in.AmbiguityReason == "" {
		in.AmbiguityReason = reason
	}
	for _, prior := range in.PossibleDuplicateOf {
		if prior == id {
			return
		}
	}
	if id != "" {
		in.PossibleDuplicateOf = append(in.PossibleDuplicateOf, id)
	}
}

// reconcileAMHistories retains unresolved cross-instance observations rather
// than rejecting them. The delivery order is the controlled source-before-local
// replay order, not a fabricated cross-peer reception chronology. Exact stable
// API identities and unambiguous AM tail overlap retain normal de-duplication.
func reconcileAMHistories(tx *bolt.Tx, m ReleaseManifest, baseIDs map[string]bool, localIntakes, localTargets, localDedupe map[string][]byte, sourceIDs map[string]bool) error {
	if err := validateReleaseDedupe(m); err != nil {
		return err
	}
	for key, sourceID := range m.Buckets["dedupe"] {
		if strings.Contains(key, ":intent:") {
			continue
		}
		localID := localDedupe[key]
		if localID == nil || string(localID) == string(sourceID) {
			continue
		}
		var src, loc Intake
		if json.Unmarshal(m.Buckets["intakes"][string(sourceID)], &src) != nil || json.Unmarshal(localIntakes[string(localID)], &loc) != nil {
			return errors.New("invalid dedupe reference")
		}
		if sourceIDs[loc.ID] {
			continue
		}
		isAM := src.AMGroup || loc.AMGroup || strings.Contains(key, ":am:") || strings.Contains(key, ":am-group:")
		if !isAM {
			if err := confirmAlias(loc, src, localTargets, m.Buckets["targets"]); err != nil {
				return err
			}
			continue
		}
		var a, b []Intake
		sourceOccurrences, localOccurrences := map[string]int{}, map[string]int{}
		for _, in := range orderedIntakes(m.Buckets["intakes"], key) {
			sourceOccurrences[in.RequestHash]++
			if !baseIDs[in.ID] {
				a = append(a, in)
			}
		}
		for _, in := range orderedIntakes(localIntakes, key) {
			localOccurrences[in.RequestHash]++
			if !baseIDs[in.ID] && !sourceIDs[in.ID] {
				b = append(b, in)
			}
		}
		// Old exact hash indexes do not establish a group generation. Carry them
		// as history, leaving newly accepted local records independent.
		if len(b) == 0 {
			continue
		}
		overlap, alternatives := 0, 0
		if len(baseIDs) > 0 && src.AMGroup && loc.AMGroup && src.DedupeKey == key && loc.DedupeKey == key {
			for n := 1; n <= len(a) && n <= len(b); n++ {
				matches := true
				for j := 0; j < n; j++ {
					prior, next := a[len(a)-n+j], b[j]
					if !sameBoundary(prior, next) || prior.AmbiguityReason != "" || next.AmbiguityReason != "" || sourceOccurrences[prior.RequestHash] != 1 || localOccurrences[next.RequestHash] != 1 {
						matches = false
						break
					}
				}
				if matches {
					overlap = n
					alternatives++
				}
			}
		}
		if alternatives != 1 {
			overlap = 0
		}
		nextOrder := src.DeliveryOrder
		if nextOrder < src.Generation {
			nextOrder = src.Generation
		}
		for j := range b {
			aliased := false
			if j < overlap {
				prior := a[len(a)-overlap+j]
				// confirmAlias can update several targets: a failed match must not
				// leak partial successful states into an accepted ambiguous plan.
				candidateTargets := map[string][]byte{}
				for _, id := range b[j].TargetIDs {
					candidateTargets[id] = localTargets[id]
				}
				if err := confirmAlias(b[j], prior, candidateTargets, m.Buckets["targets"]); err == nil {
					for id, raw := range candidateTargets {
						localTargets[id] = raw
					}
					aliased = true
				}
			}
			if !aliased && len(a) > 0 {
				for _, prior := range a {
					if prior.RequestHash == b[j].RequestHash {
						appendPossibleDuplicate(&b[j], prior.ID, "cross_instance_generation_unproven")
					}
				}
				// Even distinct contents lack cross-peer reception chronology.
				if overlap == 0 {
					appendPossibleDuplicate(&b[j], "", "cross_instance_order_unproven")
				}
			}
			nextOrder++
			b[j].DeliveryOrder = nextOrder
			raw, err := json.Marshal(b[j])
			if err != nil {
				return err
			}
			localIntakes[b[j].ID] = raw
			if b[j].AmbiguityReason != "" {
				if err := put(tx.Bucket([]byte("audit")), "am-handoff:"+m.OperationID+":"+b[j].ID, map[string]interface{}{
					"action": "retain_ambiguous_am_candidate", "operation_id": m.OperationID,
					"intake_id": b[j].ID, "ambiguity_reason": b[j].AmbiguityReason,
					"possible_duplicate_of": b[j].PossibleDuplicateOf,
					"delivery_order":        b[j].DeliveryOrder, "source_before_local": true,
				}); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Cancellation returns never-sent successor buffers to the still-active
// predecessor. Its accepted/sent local history stays authoritative; only the
// returned rows receive a new local scheduling position after that history.
func reorderCancelledBuffer(tx *bolt.Tx, sourceIDs map[string]bool) error {
	maxOrder := map[string]uint64{}
	var returned []Intake
	b := tx.Bucket([]byte("intakes"))
	if err := b.ForEach(func(_, raw []byte) error {
		var in Intake
		if err := json.Unmarshal(raw, &in); err != nil {
			return err
		}
		if !in.AMGroup || in.DedupeKey == "" || in.IdentityOrigin == "legacy-unsequenced" {
			return nil
		}
		if sourceIDs[in.ID] {
			returned = append(returned, in)
			return nil
		}
		order := in.DeliveryOrder
		if order < in.Generation {
			order = in.Generation
		}
		if order > maxOrder[in.DedupeKey] {
			maxOrder[in.DedupeKey] = order
		}
		return nil
	}); err != nil {
		return err
	}
	sort.Slice(returned, func(a, b int) bool {
		if returned[a].ReceptionSequence != 0 && returned[b].ReceptionSequence != 0 && returned[a].ReceptionInstance == returned[b].ReceptionInstance {
			return returned[a].ReceptionSequence < returned[b].ReceptionSequence
		}
		if !returned[a].AcceptedAt.Equal(returned[b].AcceptedAt) {
			return returned[a].AcceptedAt.Before(returned[b].AcceptedAt)
		}
		return returned[a].ID < returned[b].ID
	})
	for _, in := range returned {
		maxOrder[in.DedupeKey]++
		in.DeliveryOrder = maxOrder[in.DedupeKey]
		if err := put(b, in.ID, in); err != nil {
			return err
		}
	}
	return refreshAMCurrentDedupe(tx)
}

// Missing generation metadata is a supported legacy fact; dangling or
// contradictory indexes are corrupt snapshots, not ambiguity to fail open.
func validateReleaseDedupe(m ReleaseManifest) error {
	for key, id := range m.Buckets["dedupe"] {
		var in Intake
		if len(id) == 0 || json.Unmarshal(m.Buckets["intakes"][string(id)], &in) != nil || in.ID != string(id) {
			return errors.New("invalid release dedupe reference")
		}
		if split := strings.LastIndex(key, ":intent:"); split >= 0 {
			if in.RequestHash != key[split+len(":intent:"):] || (in.DedupeKey != "" && in.DedupeKey != key[:split]) {
				return errors.New("AM intent index contradicts frozen intake")
			}
		} else if in.AMGroup && in.DedupeKey != "" && in.DedupeKey != key {
			return errors.New("AM current index contradicts observed generation")
		}
	}
	return nil
}
