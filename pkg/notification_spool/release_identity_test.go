package notification_spool

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func renderedFeishuTarget(content, credential string) Target {
	payload, _ := json.Marshal(map[string]interface{}{"version": 1, "content": content, "credential_references": []map[string]string{{"name": credential, "key": "app-secret"}}, "data": map[string]string{"receiver": "critical"}})
	return Target{ID: "target", IntakeID: "intake", Receiver: "critical", Channel: "feishu", Destination: "chat:test", ContentRevision: Hash(payload), Payload: payload, State: Pending}
}
func aliasRendered(t *testing.T, a, b Target) error {
	t.Helper()
	a.ID = "a"
	a.IntakeID = "source"
	a.State = Delivered
	a.Attempts = 1
	a.Evidence = "exact-mock-receipt"
	b.ID = "b"
	b.IntakeID = "local"
	aRaw, _ := json.Marshal(a)
	bRaw, _ := json.Marshal(b)
	return confirmAlias(Intake{ID: "local", RequestHash: "same-original-request", TargetIDs: []string{"b"}}, Intake{ID: "source", RequestHash: "same-original-request", TargetIDs: []string{"a"}}, map[string][]byte{"b": bRaw}, map[string][]byte{"a": aRaw})
}
func TestReleaseFeishuCanonicalObjectOrderPreservesExactReceiptIdentity(t *testing.T) {
	a := renderedFeishuTarget(`{"elements":[{"tag":"button","value":{"op":"ack","id":"same"}}],"n":9007199254740993}`, "same-reference")
	b := renderedFeishuTarget(` { "n":9007199254740993, "elements": [ { "value": {"id":"same","op":"ack"}, "tag":"button" } ] } `, "same-reference")
	if a.ContentRevision == b.ContentRevision {
		t.Fatal("fixture did not vary actual frozen payload hash")
	}
	if err := aliasRendered(t, a, b); err != nil {
		t.Fatal("semantic JSON object order mismatch rejected proven same receipt", err)
	}
	if a.ContentRevision != Hash(a.Payload) || b.ContentRevision != Hash(b.Payload) {
		t.Fatal("comparison mutated original frozen bytes")
	}
}
func TestReleaseFeishuCanonicalizationNeverIgnoresMeaningfulChanges(t *testing.T) {
	base := `{"elements":[{"tag":"button","value":{"op":"ack","id":"same"}}],"array":["first","second"],"n":9007199254740993,"text":"Confirm"}`
	cases := map[string]Target{
		"button-value":         renderedFeishuTarget(strings.Replace(base, `"op":"ack"`, `"op":"suppress"`, 1), "same-reference"),
		"exact-number":         renderedFeishuTarget(strings.Replace(base, "9007199254740993", "9007199254740992", 1), "same-reference"),
		"array-order":          renderedFeishuTarget(strings.Replace(base, `["first","second"]`, `["second","first"]`, 1), "same-reference"),
		"literal-string-space": renderedFeishuTarget(strings.Replace(base, `"Confirm"`, `" Confirm"`, 1), "same-reference"),
		"credential-reference": renderedFeishuTarget(base, "different-reference"),
		"duplicate-key":        renderedFeishuTarget(strings.Replace(base, `"text":"Confirm"`, `"text":"Confirm","text":"Different"`, 1), "same-reference"),
	}
	for name, changed := range cases {
		t.Run(name, func(t *testing.T) {
			if err := aliasRendered(t, renderedFeishuTarget(base, "same-reference"), changed); err == nil {
				t.Fatal("different target acquired predecessor receipt")
			}
		})
	}
	a := renderedFeishuTarget(base, "same-reference")
	b := a
	b.ContentRevision = "not-the-frozen-payload-hash"
	if err := aliasRendered(t, a, b); err == nil {
		t.Fatal("unverified content revision ignored")
	}
	a = renderedFeishuTarget(`{"a":1,"b":2}`, "same-reference")
	b = renderedFeishuTarget(`{"b":2,"a":1}`, "same-reference")
	a.Channel = "webhook"
	b.Channel = "webhook"
	if err := aliasRendered(t, a, b); err == nil {
		t.Fatal("non-Feishu channel was silently normalized")
	}
}
func TestReleaseCanonicalHistoryDependenciesRequireTheSameExactParents(t *testing.T) {
	src := renderedFeishuTarget(`{"a":1,"b":2}`, "same-reference")
	local := renderedFeishuTarget(`{"b":2,"a":1}`, "same-reference")
	src.ID = "source-parent"
	local.ID = "local-parent"
	src.IntakeID = "source"
	local.IntakeID = "local"
	src.State = Delivered
	src.Evidence = "parent-receipt"
	srcChild := renderedFeishuTarget(`{"text":"history"}`, "same-reference")
	localChild := srcChild
	srcChild.ID = "source-child"
	localChild.ID = "local-child"
	srcChild.IntakeID = "source"
	localChild.IntakeID = "local"
	srcChild.State = Delivered
	srcChild.Evidence = "history-receipt"
	srcChild.DependencyRevisions = []string{src.ContentRevision}
	localChild.DependencyRevisions = []string{local.ContentRevision}
	sr := map[string][]byte{}
	lr := map[string][]byte{}
	for _, v := range []Target{src, srcChild} {
		sr[v.ID], _ = json.Marshal(v)
	}
	for _, v := range []Target{local, localChild} {
		lr[v.ID], _ = json.Marshal(v)
	}
	si := Intake{ID: "source", RequestHash: "same", TargetIDs: []string{src.ID, srcChild.ID}}
	li := Intake{ID: "local", RequestHash: "same", TargetIDs: []string{local.ID, localChild.ID}}
	if err := confirmAlias(li, si, lr, sr); err != nil {
		t.Fatal("proven same history parent identity was lost", err)
	}
	localChild.DependencyRevisions = []string{"missing-parent"}
	localChild.UpdatedAt = time.Now()
	lr[localChild.ID], _ = json.Marshal(localChild)
	if err := confirmAlias(li, si, lr, sr); err == nil {
		t.Fatal("missing dependency parent accepted")
	}
}

func TestAMReturnPreservesOriginalUnknownAndIndependentReceiveFactsAcrossRestart(t *testing.T) {
	s, path := releaseFixture(t, "old", "active")
	a, _ := submitGroup(t, s, "group", "same-start:A", time.Hour)
	c, err := s.Claim(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(c, Unknown, "response_lost", "unknown-platform-result", time.Now()); err != nil {
		t.Fatal(err)
	}
	middle, _ := submitGroup(t, s, "group", "same-start:B", time.Hour)
	returned, dup := submitGroup(t, s, "group", "same-start:A", time.Hour)
	if dup || returned.ID == a.ID || returned.ReceptionSequence <= middle.ReceptionSequence || returned.ReceptionInstance != "old" || returned.AmbiguityReason != "content_return_or_delayed_peer" || returned.Generation != 3 || len(returned.PossibleDuplicateOf) != 1 || returned.PossibleDuplicateOf[0] != a.ID {
		t.Fatal("receive facts missing", returned)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.ConfigureRelease("old", "active", releaseTestKey); err != nil {
		t.Fatal(err)
	}
	current, dup := submitGroup(t, s, "group", "same-start:A", time.Hour)
	if !dup || current.ID != returned.ID || current.ReceptionSequence != returned.ReceptionSequence || current.Generation != returned.Generation {
		t.Fatal("current certain duplicate not preserved", current)
	}
	_, targets, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range targets {
		if row.IntakeID == a.ID && (row.State != Unknown || row.Attempts != 1 || row.AttemptID != c.AttemptID || row.Evidence != "unknown-platform-result") {
			t.Fatal("old unknown was replayed or rewritten", row)
		}
	}
	for _, wanted := range []string{middle.ID, returned.ID} {
		if c := finishOne(t, s); c.Target.IntakeID != wanted {
			t.Fatal("candidate order lost", c.Target.IntakeID, wanted)
		}
	}
}

func TestAMExpiredRepeatWithUnknownCreatesNewCandidateWithoutRetryingUnknown(t *testing.T) {
	s, _ := releaseFixture(t, "old", "active")
	defer s.Close()
	first, _ := submitGroup(t, s, "group", "same-start:A", time.Millisecond)
	c, err := s.Claim(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(c, Unknown, "response_lost", "first-attempt", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := s.update(func(tx *bolt.Tx) error {
		first.AcceptedAt = time.Now().Add(-time.Hour)
		return put(tx.Bucket([]byte("intakes")), first.ID, first)
	}); err != nil {
		t.Fatal(err)
	}
	repeat, dup := submitGroup(t, s, "group", "same-start:A", time.Millisecond)
	if dup || repeat.ID == first.ID || repeat.AmbiguityReason != "repeat_window_elapsed_with_unsettled_predecessor" || repeat.PossibleDuplicateOf[0] != first.ID {
		t.Fatal("unknown permanently swallowed repeat", repeat)
	}
	if c := finishOne(t, s); c.Target.IntakeID != repeat.ID {
		t.Fatal("repeat did not send independently")
	}
	_, rows, _ := s.Snapshot()
	for _, row := range rows {
		if row.IntakeID == first.ID && (row.State != Unknown || row.AttemptID != c.AttemptID || row.Attempts != 1) {
			t.Fatal("unknown automatically retried", row)
		}
	}
}

func TestLegacyPlainRestartKeepsPendingUnknownAndAllowsSubsequentNewProtocolHandoff(t *testing.T) {
	s, path, _ := profileFixture(t)
	snap, _ := s.ProfileSnapshot("test")
	h1, h2 := Hash([]byte("legacy-unknown")), Hash([]byte("legacy-pending"))
	unknown, _, err := s.SubmitProfile("test:v1:am:"+h1, h1, []Target{profileTarget(snap)}, "", snap, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := s.Claim(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(claim, Unknown, "legacy_response_loss", "legacy-proof", time.Now()); err != nil {
		t.Fatal(err)
	}
	pending, _, err := s.SubmitProfile("test:v1:am:"+h2, h2, []Target{profileTarget(snap)}, "", snap, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.update(func(tx *bolt.Tx) error {
		for _, in := range []Intake{unknown, pending} {
			in.Generation = 0
			in.IdentityOrigin = ""
			in.ReceptionSequence = 0
			in.ReceptionInstance = ""
			if err := put(tx.Bucket([]byte("intakes")), in.ID, in); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.ConfigureRelease("old", "active", releaseTestKey); err != nil {
		t.Fatal(err)
	}
	intakes, rows, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(intakes) != 2 || len(rows) != 2 {
		t.Fatal("legacy facts deleted")
	}
	for _, in := range intakes {
		if in.IdentityOrigin != "legacy-unsequenced" || in.AMGroup || in.Generation != 0 || in.ReceptionSequence != 0 || in.ReceptionInstance != "" {
			t.Fatal("legacy generation metadata invented", in)
		}
	}
	for _, row := range rows {
		if row.IntakeID == unknown.ID && (row.State != Unknown || row.Attempts != 1 || row.AttemptID != claim.AttemptID || row.Evidence != "legacy-proof") {
			t.Fatal("legacy unknown facts rewritten", row)
		}
	}
	if c := finishOne(t, s); c.Target.IntakeID != pending.ID {
		t.Fatal("legacy pending lost", c.Target.IntakeID)
	}
	b, _ := releaseFixture(t, "new", "standby")
	defer b.Close()
	beginPair(t, s, b)
	preseed(t, s, b)
	fresh, _ := submitGroup(t, b, "new-group", "new-start:critical", time.Hour)
	finalHandoff(t, s, b)
	if c := finishOne(t, b); c.Target.IntakeID != fresh.ID {
		t.Fatal("legacy facts blocked future bluegreen")
	}
	_, rows, err = b.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.IntakeID == unknown.ID && (row.State != Unknown || row.Attempts != 1 || row.AttemptID != claim.AttemptID) {
			t.Fatal("handoff changed unknown", row)
		}
	}
}

func TestCancelledBufferReorderDoesNotRewriteAuthoritativeGeneration(t *testing.T) {
	s, _ := releaseFixture(t, "old", "active")
	defer s.Close()
	first, _ := submitGroup(t, s, "group", "old-first", time.Hour)
	second, _ := submitGroup(t, s, "group", "old-second", time.Hour)
	returned, _ := submitGroup(t, s, "group", "buffer-return", time.Hour)
	if err := s.update(func(tx *bolt.Tx) error {
		returned.Generation = 1
		returned.DeliveryOrder = 1
		if err := put(tx.Bucket([]byte("intakes")), returned.ID, returned); err != nil {
			return err
		}
		return reorderCancelledBuffer(tx, map[string]bool{returned.ID: true})
	}); err != nil {
		t.Fatal(err)
	}
	intakes, _, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, in := range intakes {
		switch in.ID {
		case first.ID:
			if in.Generation != first.Generation || in.DeliveryOrder != first.DeliveryOrder {
				t.Fatal("old first changed")
			}
		case second.ID:
			if in.Generation != second.Generation || in.DeliveryOrder != second.DeliveryOrder {
				t.Fatal("old second changed")
			}
		case returned.ID:
			if in.Generation != 1 || in.DeliveryOrder != second.DeliveryOrder+1 {
				t.Fatal("returned buffer raw facts or queue order wrong", in)
			}
		}
	}
}

func TestAMCrossPeerContentReturnKeepsPossibleBoundaryDuplicatesWithoutLoss(t *testing.T) {
	a, _ := releaseFixture(t, "old", "active")
	defer a.Close()
	b, _ := releaseFixture(t, "new", "standby")
	defer b.Close()
	base, _ := submitGroup(t, a, "same-start-group", "A", time.Hour)
	finishOne(t, a)
	beginPair(t, a, b)
	preseed(t, a, b)
	oldB, _ := submitGroup(t, a, "same-start-group", "B", time.Hour)
	finishOne(t, a)
	oldA, _ := submitGroup(t, a, "same-start-group", "A", time.Hour)
	finishOne(t, a)
	bufferB, _ := submitGroup(t, b, "same-start-group", "B", time.Hour)
	bufferA, _ := submitGroup(t, b, "same-start-group", "A", time.Hour)
	finalHandoff(t, a, b)
	for _, wanted := range []string{bufferB.ID, bufferA.ID} {
		if c := finishOne(t, b); c.Target.IntakeID != wanted {
			t.Fatal("ambiguous candidate omitted or reordered", c.Target.IntakeID, wanted)
		}
	}
	if _, err := b.Claim(time.Now()); !errors.Is(err, ErrEmpty) {
		t.Fatal("unexpected extra candidate", err)
	}
	intakes, targets, err := b.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(intakes) != 5 || len(targets) != 5 {
		t.Fatal("accepted tasks lost", len(intakes), len(targets))
	}
	for _, in := range intakes {
		if in.ID == bufferA.ID && (in.Generation != bufferA.Generation || in.ReceptionSequence != bufferA.ReceptionSequence || in.ReceptionInstance != "new" || len(in.PossibleDuplicateOf) < 1 || in.AmbiguityReason == "") {
			t.Fatal("return provenance was overwritten", in)
		}
	}
	for _, target := range targets {
		if target.State != Delivered || target.Attempts != 1 {
			t.Fatal("missing receipt or blind retry", target)
		}
	}
	t.Logf("accepted=5 delivered=5 missing=0 potential_boundary_duplicates=2 base=%s old_B=%s old_A=%s buffer_B=%s buffer_A=%s", base.ID, oldB.ID, oldA.ID, bufferB.ID, bufferA.ID)
}

func TestReleaseLegacyClassificationDoesNotPermitCorruptIndexes(t *testing.T) {
	a, _ := releaseFixture(t, "old", "active")
	defer a.Close()
	b, _ := releaseFixture(t, "new", "standby")
	defer b.Close()
	submitGroup(t, a, "test:v1:am-group:g", "A", time.Hour)
	beginPair(t, a, b)
	for _, mutation := range []string{"dangling-reference", "hash-contradiction", "group-contradiction"} {
		t.Run(mutation, func(t *testing.T) {
			m := exportRelease(t, a, false)
			if mutation == "dangling-reference" {
				m.Buckets["dedupe"]["test:v1:am-group:g"] = []byte("not-present")
			}
			if mutation == "hash-contradiction" {
				for key, id := range m.Buckets["dedupe"] {
					if strings.Contains(key, ":intent:") {
						delete(m.Buckets["dedupe"], key)
						m.Buckets["dedupe"]["test:v1:am-group:g:intent:wrong"] = id
					}
				}
			}
			if mutation == "group-contradiction" {
				for id, raw := range m.Buckets["intakes"] {
					var in Intake
					if err := json.Unmarshal(raw, &in); err != nil {
						t.Fatal(err)
					}
					in.DedupeKey = "different-group"
					m.Buckets["intakes"][id], _ = json.Marshal(in)
				}
			}
			m.MAC = ""
			m.SnapshotChecksum = manifestChecksum(m)
			m.MAC = a.releaseMAC(m)
			before, _ := b.ReleaseStatus()
			if _, err := b.ImportRelease(m, commandFor(t, b, "import")); err == nil {
				t.Fatal("corrupt snapshot accepted")
			}
			after, _ := b.ReleaseStatus()
			if before.Revision != after.Revision {
				t.Fatal("failed snapshot partially committed")
			}
		})
	}
}

func TestReleaseUncertainAliasCannotPartiallyAcquireSuccessfulReceipt(t *testing.T) {
	s, _ := releaseFixture(t, "new", "standby")
	defer s.Close()
	now := time.Now().UTC()
	for range 20 {
		source, local := Intake{ID: "source", DedupeKey: "group", AMGroup: true, Generation: 1, WindowMillis: time.Hour.Milliseconds(), RequestHash: "A", AcceptedAt: now, TargetIDs: []string{"source-one", "source-two"}}, Intake{ID: "local", DedupeKey: "group", AMGroup: true, Generation: 1, WindowMillis: time.Hour.Milliseconds(), RequestHash: "A", AcceptedAt: now, TargetIDs: []string{"local-one", "local-two"}}
		st := map[string][]byte{}
		lt := map[string][]byte{}
		for n, name := range []string{"one", "two"} {
			a, b := renderedFeishuTarget(`{"text":"`+name+`"}`, "same-reference"), renderedFeishuTarget(`{"text":"`+name+`"}`, "same-reference")
			a.ID = "source-" + name
			a.IntakeID = source.ID
			a.State = Delivered
			a.Evidence = "confirmed"
			if n == 1 {
				a.State = Unknown
				a.Evidence = "uncertain"
			}
			b.ID = "local-" + name
			b.IntakeID = local.ID
			st[a.ID], _ = json.Marshal(a)
			lt[b.ID], _ = json.Marshal(b)
		}
		sr, _ := json.Marshal(source)
		lr, _ := json.Marshal(local)
		m := ReleaseManifest{OperationID: "test", Buckets: map[string]map[string][]byte{"intakes": {source.ID: sr}, "targets": st, "dedupe": {"group": []byte(source.ID)}}}
		if err := s.update(func(tx *bolt.Tx) error {
			return reconcileAMHistories(tx, m, map[string]bool{"frozen-base": true}, map[string][]byte{local.ID: lr}, lt, map[string][]byte{"group": []byte(local.ID)}, map[string]bool{source.ID: true})
		}); err != nil {
			t.Fatal(err)
		}
		for _, raw := range lt {
			var target Target
			if err := json.Unmarshal(raw, &target); err != nil {
				t.Fatal(err)
			}
			if target.State != Pending || target.Evidence != "" || target.Attempts != 0 {
				t.Fatal("uncertain multi-target alias partially acquired receipt", target)
			}
		}
	}
}
