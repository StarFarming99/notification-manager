package notification_spool

import (
	"container/heap"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

const terminalSupersededOutcome = "terminal_superseded"

type releaseLifecycleTarget struct {
	target Target
	alerts map[string]string
}

// lifecycleFacts is deliberately strict. A mixed or incomplete frozen render
// cannot establish that an entire original notification has already recovered.
func lifecycleFacts(t Target) map[string]string {
	var payload struct {
		Data struct {
			Alerts []struct {
				ID       string            `json:"id"`
				Labels   map[string]string `json:"labels"`
				StartsAt time.Time         `json:"startsAt"`
				Status   string            `json:"status"`
			} `json:"alerts"`
		} `json:"data"`
	}
	if json.Unmarshal(t.Payload, &payload) != nil || len(payload.Data.Alerts) == 0 {
		return nil
	}
	out := map[string]string{}
	for _, alert := range payload.Data.Alerts {
		if alert.StartsAt.IsZero() || (alert.Status != "firing" && alert.Status != "resolved") || (alert.ID == "" && len(alert.Labels) == 0) {
			return nil
		}
		identity := alert.ID
		if len(alert.Labels) > 0 {
			raw, _ := json.Marshal(alert.Labels)
			identity = Hash(raw)
		}
		raw, _ := json.Marshal([]string{t.ProfileID, t.ProfileVersion, t.CardOwnerID, t.Receiver, t.Channel, t.Destination, t.OriginalDestination, identity, alert.StartsAt.UTC().Format(time.RFC3339Nano)})
		key := string(raw)
		if previous, ok := out[key]; ok && previous != alert.Status {
			return nil
		}
		out[key] = alert.Status
	}
	return out
}
func allLifecycleStatus(facts map[string]string, status string) bool {
	if len(facts) == 0 {
		return false
	}
	for _, value := range facts {
		if value != status {
			return false
		}
	}
	return true
}
func unstartedLifecycleTarget(t Target) bool {
	return (t.State == Pending || t.State == Retryable) && t.Attempts == 0
}

// reconcileLifecycleOrder preserves original facts while imposing the proven
// firing-before-resolved relationship for the exact same lifecycle. It runs
// under the import transaction before queue/index reconstruction. It does not
// infer order from AM peer clocks or turn a superseded card into platform success.
func reconcileLifecycleOrder(tx *bolt.Tx) error {
	if err := rebuildDeliveredLifecycleIndex(tx); err != nil {
		return err
	}
	intakes, targets := tx.Bucket([]byte("intakes")), tx.Bucket([]byte("targets"))
	groups := map[string][]Intake{}
	eligible := map[string]bool{}
	rows := map[string][]releaseLifecycleTarget{}
	deliveredResolution := map[string][]string{}
	if err := intakes.ForEach(func(_, raw []byte) error {
		var in Intake
		if err := json.Unmarshal(raw, &in); err != nil {
			return err
		}
		if !in.AMGroup || in.DedupeKey == "" || in.IdentityOrigin == "legacy-unsequenced" {
			return nil
		}
		groups[in.DedupeKey] = append(groups[in.DedupeKey], in)
		eligible[in.ID] = len(in.TargetIDs) > 0
		for _, id := range in.TargetIDs {
			var t Target
			if err := json.Unmarshal(targets.Get([]byte(id)), &t); err != nil {
				return err
			}
			facts := lifecycleFacts(t)
			rows[in.ID] = append(rows[in.ID], releaseLifecycleTarget{t, facts})
			if !unstartedLifecycleTarget(t) {
				eligible[in.ID] = false
			}
			if t.State == Delivered && t.Evidence != "" && allLifecycleStatus(facts, "resolved") {
				for identity := range facts {
					deliveredResolution[identity] = append(deliveredResolution[identity], t.ID)
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	for intakeID, items := range rows {
		for n, item := range items {
			if !unstartedLifecycleTarget(item.target) || !allLifecycleStatus(item.alerts, "firing") {
				continue
			}
			proofs := map[string]bool{}
			complete := true
			for identity := range item.alerts {
				if len(deliveredResolution[identity]) == 0 {
					complete = false
					continue
				}
				for _, id := range deliveredResolution[identity] {
					proofs[id] = true
				}
			}
			if !complete {
				if len(proofs) > 0 {
					if err := recordLifecyclePartial(tx, item.target, "mixed_target_incomplete_resolution"); err != nil {
						return err
					}
				}
				continue
			}
			ids := make([]string, 0, len(proofs))
			for id := range proofs {
				ids = append(ids, id)
			}
			sort.Strings(ids)
			t := item.target
			t.State = DeadLetter
			t.Outcome = terminalSupersededOutcome
			t.Evidence = "delivered-resolutions:" + Hash([]byte(joinProofIDs(ids)))
			t.UpdatedAt = time.Now().UTC()
			if err := put(targets, t.ID, t); err != nil {
				return err
			}
			if err := put(tx.Bucket([]byte("audit")), "terminal-superseded:"+t.ID, map[string]interface{}{"action": terminalSupersededOutcome, "target_id": t.ID, "intake_id": intakeID, "resolved_receipt_target_ids": ids, "original_state": item.target.State, "original_attempts": item.target.Attempts, "original_attempt_id": item.target.AttemptID, "platform_sent": false, "at": t.UpdatedAt}); err != nil {
				return err
			}
			rows[intakeID][n].target = t
			eligible[intakeID] = false
		}
	}
	for intakeID := range rows {
		if err := supersedeBlockedLifecycleHistory(tx, intakeID, false); err != nil {
			return err
		}
	}
	for group, history := range groups {
		sort.Slice(history, func(i, j int) bool {
			if groupOrder(history[i]) != groupOrder(history[j]) {
				return groupOrder(history[i]) < groupOrder(history[j])
			}
			if !history[i].AcceptedAt.Equal(history[j].AcceptedAt) {
				return history[i].AcceptedAt.Before(history[j].AcceptedAt)
			}
			return history[i].ID < history[j].ID
		})
		var movable []Intake
		for _, in := range history {
			if eligible[in.ID] {
				movable = append(movable, in)
			}
		}
		edges := map[string]map[string]bool{}
		indegree := map[string]int{}
		for _, in := range movable {
			indegree[in.ID] = 0
		}
		firingByLife, resolvedByLife := map[string]map[string]bool{}, map[string]map[string]bool{}
		for _, in := range movable {
			for _, item := range rows[in.ID] {
				for identity, status := range item.alerts {
					index := firingByLife
					if status == "resolved" {
						index = resolvedByLife
					}
					if index[identity] == nil {
						index[identity] = map[string]bool{}
					}
					index[identity][in.ID] = true
				}
			}
		}
		position := map[string]int{}
		for n, in := range movable {
			position[in.ID] = n
		}
		addEdge := func(from, to string) {
			if from == to {
				return
			}
			if edges[from] == nil {
				edges[from] = map[string]bool{}
			}
			if !edges[from][to] {
				edges[from][to] = true
				indegree[to]++
			}
		}
		orderedIDs := func(ids map[string]bool) []string {
			out := make([]string, 0, len(ids))
			for id := range ids {
				out = append(out, id)
			}
			sort.Slice(out, func(i, j int) bool { return position[out[i]] < position[out[j]] })
			return out
		}
		for life, firings := range firingByLife {
			fires, resolutions := orderedIDs(firings), orderedIDs(resolvedByLife[life])
			if len(resolutions) == 0 {
				continue
			}
			for n := 1; n < len(fires); n++ {
				addEdge(fires[n-1], fires[n])
			}
			for n := 1; n < len(resolutions); n++ {
				addEdge(resolutions[n-1], resolutions[n])
			}
			if len(fires) > 0 {
				addEdge(fires[len(fires)-1], resolutions[0])
			}
		}
		var ordered []Intake
		ready := &lifecyclePriority{}
		heap.Init(ready)
		for n, in := range movable {
			if indegree[in.ID] == 0 {
				heap.Push(ready, n)
			}
		}
		for ready.Len() > 0 {
			n := heap.Pop(ready).(int)
			in := movable[n]
			ordered = append(ordered, in)
			for next := range edges[in.ID] {
				indegree[next]--
				if indegree[next] == 0 {
					heap.Push(ready, position[next])
				}
			}
		}

		if len(ordered) != len(movable) {
			if err := put(tx.Bucket([]byte("audit")), "lifecycle-partial:"+Hash([]byte(group)), map[string]interface{}{"action": "lifecycle_order_partial", "group": group, "reason": "mixed_group_cycle_cannot_split_frozen_card", "candidates_retained": true, "at": time.Now().UTC()}); err != nil {
				return err
			}
			ordered = movable
		}
		next := 0
		for n, in := range history {
			if eligible[in.ID] {
				in = ordered[next]
				next++
			}
			in.DeliveryOrder = uint64(n + 1)
			if err := put(intakes, in.ID, in); err != nil {
				return err
			}
		}
	}
	return refreshAMCurrentDedupe(tx)
}
func joinProofIDs(ids []string) string { raw, _ := json.Marshal(ids); return string(raw) }

// Refresh only observed AM group current pointers. Historical intent indexes
// retain the exact originally accepted rows, including ambiguous/superseded ones.
func refreshAMCurrentDedupe(tx *bolt.Tx) error {
	current := map[string]Intake{}
	if err := tx.Bucket([]byte("intakes")).ForEach(func(_, raw []byte) error {
		var in Intake
		if err := json.Unmarshal(raw, &in); err != nil {
			return err
		}
		if !in.AMGroup || in.DedupeKey == "" || in.IdentityOrigin == "legacy-unsequenced" {
			return nil
		}
		superseded := len(in.TargetIDs) > 0
		for _, id := range in.TargetIDs {
			var t Target
			if err := json.Unmarshal(tx.Bucket([]byte("targets")).Get([]byte(id)), &t); err != nil {
				return err
			}
			if t.State != DeadLetter || t.Outcome != terminalSupersededOutcome {
				superseded = false
			}
		}
		if superseded {
			return nil
		}
		prior, ok := current[in.DedupeKey]
		if !ok || groupOrder(in) > groupOrder(prior) || (groupOrder(in) == groupOrder(prior) && in.AcceptedAt.After(prior.AcceptedAt)) {
			current[in.DedupeKey] = in
		}
		return nil
	}); err != nil {
		return err
	}
	for key, in := range current {
		if in.ID == "" {
			return errors.New("empty observed AM current identity")
		}
		if err := tx.Bucket([]byte("dedupe")).Put([]byte(key), []byte(in.ID)); err != nil {
			return err
		}
	}
	return nil
}

// Stable Kahn ordering is O((rows + constraints) log rows), rather than a
// quadratic repeated scan for the next eligible intake.
type lifecyclePriority []int

func (h lifecyclePriority) Len() int            { return len(h) }
func (h lifecyclePriority) Less(i, j int) bool  { return h[i] < h[j] }
func (h lifecyclePriority) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *lifecyclePriority) Push(v interface{}) { *h = append(*h, v.(int)) }
func (h *lifecyclePriority) Pop() interface{} {
	old := *h
	n := len(old)
	v := old[n-1]
	*h = old[:n-1]
	return v
}

func recordDeliveredLifecycle(tx *bolt.Tx, t Target) error {
	if t.State != Delivered || t.Evidence == "" {
		return nil
	}
	var in Intake
	if err := json.Unmarshal(tx.Bucket([]byte("intakes")).Get([]byte(t.IntakeID)), &in); err != nil {
		return err
	}
	if !in.AMGroup || in.IdentityOrigin == "legacy-unsequenced" {
		return nil
	}
	facts := lifecycleFacts(t)
	if !allLifecycleStatus(facts, "resolved") {
		return nil
	}
	for life := range facts {
		if err := tx.Bucket([]byte("meta")).Put([]byte("lifecycle-resolved:"+Hash([]byte(life))), []byte(t.ID)); err != nil {
			return err
		}
	}
	return nil
}
func rebuildDeliveredLifecycleIndex(tx *bolt.Tx) error {
	meta := tx.Bucket([]byte("meta"))
	var keys [][]byte
	if err := meta.ForEach(func(k, _ []byte) error {
		if strings.HasPrefix(string(k), "lifecycle-resolved:") {
			keys = append(keys, append([]byte(nil), k...))
		}
		return nil
	}); err != nil {
		return err
	}
	for _, key := range keys {
		if err := meta.Delete(key); err != nil {
			return err
		}
	}
	return tx.Bucket([]byte("targets")).ForEach(func(_, raw []byte) error {
		var t Target
		if err := json.Unmarshal(raw, &t); err != nil {
			return err
		}
		return recordDeliveredLifecycle(tx, t)
	})
}

// suppressTerminalTarget is a claim-time O(alert count) index probe. It leaves
// unknown/attempted/legacy targets untouched. On true it atomically persists an
// explicit non-sent terminal outcome, removes queue membership and updates the
// existing active counters; the caller must continue to the next claim row.
func suppressTerminalTarget(tx *bolt.Tx, t *Target) (bool, error) {
	if !unstartedLifecycleTarget(*t) {
		return false, nil
	}
	var in Intake
	if err := json.Unmarshal(tx.Bucket([]byte("intakes")).Get([]byte(t.IntakeID)), &in); err != nil {
		return false, err
	}
	if !in.AMGroup || in.IdentityOrigin == "legacy-unsequenced" {
		return false, nil
	}
	facts := lifecycleFacts(*t)
	if !allLifecycleStatus(facts, "firing") {
		return false, nil
	}
	meta := tx.Bucket([]byte("meta"))
	proofs := map[string]bool{}
	complete := true
	// Several alerts commonly share one resolved card. Parse each referenced
	// proof once per claim rather than reparsing that whole card per alert.
	proofFacts := map[string]map[string]string{}
	for life := range facts {
		key := []byte("lifecycle-resolved:" + Hash([]byte(life)))
		id := meta.Get(key)
		if id == nil {
			complete = false
			continue
		}
		proofID := string(id)
		resolvedFacts, loaded := proofFacts[proofID]
		if !loaded {
			var resolved Target
			if json.Unmarshal(tx.Bucket([]byte("targets")).Get(id), &resolved) == nil && resolved.State == Delivered && resolved.Evidence != "" {
				candidate := lifecycleFacts(resolved)
				if allLifecycleStatus(candidate, "resolved") {
					resolvedFacts = candidate
				}
			}
			proofFacts[proofID] = resolvedFacts
		}
		if resolvedFacts[life] != "resolved" {
			if err := meta.Delete(key); err != nil {
				return false, err
			}
			complete = false
			continue
		}
		proofs[proofID] = true
	}

	if !complete {
		if len(proofs) > 0 {
			if err := recordLifecyclePartial(tx, *t, "mixed_target_incomplete_resolution"); err != nil {
				return false, err
			}
		}
		return false, nil
	}

	ids := make([]string, 0, len(proofs))
	for id := range proofs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	previous := *t
	t.State = DeadLetter
	t.Outcome = terminalSupersededOutcome
	t.Evidence = "delivered-resolutions:" + Hash([]byte(joinProofIDs(ids)))
	t.UpdatedAt = time.Now().UTC()
	if err := put(tx.Bucket([]byte("audit")), "terminal-superseded:"+t.ID, map[string]interface{}{"action": terminalSupersededOutcome, "target_id": t.ID, "intake_id": t.IntakeID, "resolved_receipt_target_ids": ids, "original_state": previous.State, "original_attempts": previous.Attempts, "original_attempt_id": previous.AttemptID, "platform_sent": false, "at": t.UpdatedAt}); err != nil {
		return false, err
	}
	if err := deleteReady(tx, previous); err != nil {
		return false, err
	}
	if err := tx.Bucket([]byte("blocked")).Delete([]byte(t.ID)); err != nil {
		return false, err
	}
	if err := changeProfileActiveCount(tx, t.ProfileID, -1); err != nil {
		return false, err
	}
	if err := changeActiveCount(tx, -1); err != nil {
		return false, err
	}
	if err := put(tx.Bucket([]byte("targets")), t.ID, *t); err != nil {
		return false, err
	}
	if err := supersedeBlockedLifecycleHistory(tx, t.IntakeID, true); err != nil {
		return false, err
	}
	if err := restoreTerminalCurrentPointer(tx, *t, ids); err != nil {
		return false, err
	}
	return true, nil
}

func recordLifecyclePartial(tx *bolt.Tx, t Target, reason string) error {
	if tx.Bucket([]byte("audit")).Get([]byte("lifecycle-partial-target:"+t.ID)) != nil {
		return nil
	}
	return put(tx.Bucket([]byte("audit")), "lifecycle-partial-target:"+t.ID, map[string]interface{}{"action": "lifecycle_order_partial", "target_id": t.ID, "intake_id": t.IntakeID, "reason": reason, "candidate_retained": true, "at": time.Now().UTC()})
}

// A dependent history card cannot become eligible when every referenced
// original was proved terminal without sending. Retain it explicitly instead
// of leaving an immortal blocked pending target occupying capacity.
func supersedeBlockedLifecycleHistory(tx *bolt.Tx, intakeID string, runtime bool) error {
	var in Intake
	if err := json.Unmarshal(tx.Bucket([]byte("intakes")).Get([]byte(intakeID)), &in); err != nil {
		return err
	}
	b := tx.Bucket([]byte("targets"))
	parents := map[string]Target{}
	var children []Target
	for _, id := range in.TargetIDs {
		var t Target
		if err := json.Unmarshal(b.Get([]byte(id)), &t); err != nil {
			return err
		}
		if len(t.DependencyRevisions) == 0 {
			parents[t.ContentRevision] = t
		} else {
			children = append(children, t)
		}
	}
	for _, t := range children {
		if !unstartedLifecycleTarget(t) {
			continue
		}
		var ids []string
		all := true
		for _, revision := range t.DependencyRevisions {
			parent, ok := parents[revision]
			if !ok || parent.State != DeadLetter || parent.Outcome != terminalSupersededOutcome {
				all = false
				break
			}
			ids = append(ids, parent.ID)
		}
		if !all || len(ids) == 0 {
			continue
		}
		sort.Strings(ids)
		original := t
		t.State = DeadLetter
		t.Outcome = terminalSupersededOutcome
		t.Evidence = "terminal-originals:" + Hash([]byte(joinProofIDs(ids)))
		t.UpdatedAt = time.Now().UTC()
		if err := put(tx.Bucket([]byte("audit")), "terminal-superseded:"+t.ID, map[string]interface{}{"action": terminalSupersededOutcome, "reason": "all_original_dependencies_terminal", "target_id": t.ID, "intake_id": in.ID, "terminal_original_target_ids": ids, "platform_sent": false, "at": t.UpdatedAt}); err != nil {
			return err
		}
		if runtime {
			if err := deleteReady(tx, original); err != nil {
				return err
			}
			if err := tx.Bucket([]byte("blocked")).Delete([]byte(t.ID)); err != nil {
				return err
			}
			if err := changeActiveCount(tx, -1); err != nil {
				return err
			}
			if err := changeProfileActiveCount(tx, t.ProfileID, -1); err != nil {
				return err
			}
		}
		if err := put(b, t.ID, t); err != nil {
			return err
		}
	}
	return nil
}

// Only a fully terminal current candidate and a single exact same-group
// resolution intake permit a local pointer restoration. Never scan old history
// or guess a current request from several independent recovery messages.
func restoreTerminalCurrentPointer(tx *bolt.Tx, t Target, proofIDs []string) error {
	var in Intake
	if err := json.Unmarshal(tx.Bucket([]byte("intakes")).Get([]byte(t.IntakeID)), &in); err != nil {
		return err
	}
	dedupe := tx.Bucket([]byte("dedupe"))
	if string(dedupe.Get([]byte(in.DedupeKey))) != in.ID {
		return nil
	}
	for _, id := range in.TargetIDs {
		var target Target
		if err := json.Unmarshal(tx.Bucket([]byte("targets")).Get([]byte(id)), &target); err != nil {
			return err
		}
		if target.State != DeadLetter || target.Outcome != terminalSupersededOutcome {
			return nil
		}
	}
	resolvedIntake := ""
	for _, id := range proofIDs {
		var target Target
		if err := json.Unmarshal(tx.Bucket([]byte("targets")).Get([]byte(id)), &target); err != nil {
			return err
		}
		if resolvedIntake != "" && resolvedIntake != target.IntakeID {
			return recordLifecyclePartial(tx, t, "multiple_resolution_intakes_current_pointer_unproven")
		}
		resolvedIntake = target.IntakeID
	}
	if resolvedIntake == "" {
		return nil
	}
	var resolved Intake
	if err := json.Unmarshal(tx.Bucket([]byte("intakes")).Get([]byte(resolvedIntake)), &resolved); err != nil {
		return err
	}
	if !resolved.AMGroup || resolved.DedupeKey != in.DedupeKey || resolved.IdentityOrigin == "legacy-unsequenced" {
		return nil
	}
	if err := dedupe.Put([]byte(in.DedupeKey), []byte(resolved.ID)); err != nil {
		return err
	}
	return put(tx.Bucket([]byte("audit")), "lifecycle-current-restored:"+in.ID, map[string]interface{}{"action": "restore_recovered_am_current", "terminal_intake_id": in.ID, "resolved_intake_id": resolved.ID, "historical_intent_indexes_retained": true, "at": time.Now().UTC()})
}
