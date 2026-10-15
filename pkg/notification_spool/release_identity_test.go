package notification_spool

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
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
