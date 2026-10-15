package jevshadow

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/kubesphere/notification-manager/pkg/template"
	"github.com/kubesphere/notification-manager/pkg/utils"
)

func finalSnapshotFixture(n int) (*Service, *template.Data) {
	cfg := groupedRuntimeConfig("http://mock.invalid/receipts", "")
	cfg.expectedProfileID = "test"
	cfg.expectedProfileVersion = "v1"
	cfg.groupProfileEnabled = true
	data := groupedRuntimeData(n)
	data.ProfileID = "test"
	data.ProfileVersion = "v1"
	data.CardOwnerID = cfg.CardOwnerID
	data.OriginalDestination = "chat:oc_production_fixture"
	data.GroupLabels = template.KV{"alertname": "KubePodNotReady", "cluster": "fixture-cluster"}
	data.NMDispatch = &template.NMDispatchReference{SchemaVersion: "nm-target-v1", AssessmentMode: GroupAssessmentMode, DispatchID: "fixture-persisted-intake", TargetID: "fixture-persisted-target"}
	for i, alert := range data.Alerts {
		alert.Labels["cluster"] = "fixture-cluster"
		alert.GeneratorURL = "http://vmalert.fixture:8080/vmalert/alert?group_id=1&alert_id=" + string(rune('a'+i%20))
		alert.AMInputRefs = []template.AMInputReference{{SchemaVersion: "am-input-v1", ReferenceKind: "raw_payload_sha256", RequestSHA256: rawSHA256([]byte("fixture-original-AM-body")), Receiver: "original-am-receiver", GroupKey: `{}/{cluster="fixture-cluster",alertname="KubePodNotReady"}`, GroupLabels: data.GroupLabels.Clone(), ExternalURL: "http://alertmanager.fixture:9093"}}
	}
	data.CompactAMInputReferences()
	return &Service{config: cfg}, data
}

func TestFinalSnapshotPreservesExactScopeTriggerAndDuplicateEntryMappings(t *testing.T) {
	service, data := finalSnapshotFixture(14)
	// Identical canonical occurrence appears twice after an upstream append. Its
	// distinct original entry remains counted without creating a fake member.
	data.Alerts = append(data.Alerts, data.Alerts[0].Clone())
	data.AMInputRefs[0].EntryIndexes = append(data.AMInputRefs[0].EntryIndexes, 14)
	snapshot, err := service.buildFinalSnapshot(data, "jev-shadow-feishu-uat", "oc_test")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.MemberCount != 14 || snapshot.RawEntryCount != 15 || len(snapshot.AMInputRefs) != 1 || len(snapshot.AMInputRefs[0].EntryIndexes) != 15 || snapshot.DispatchID != data.NMDispatch.DispatchID || snapshot.TargetID != data.NMDispatch.TargetID {
		t.Fatal("scope/entries lost")
	}
	found := false
	for _, member := range snapshot.Members {
		if len(member.RawEntryIndexes) == 2 {
			found = true
			if member.GeneratorURL != data.Alerts[0].GeneratorURL || member.RawEntryIndexes[0] != 0 || member.RawEntryIndexes[1] != 14 {
				t.Fatal(member)
			}
		}
	}
	if !found {
		t.Fatal("duplicate raw entry not preserved")
	}
	if snapshot.Receiver == snapshot.ObservationReceiver || snapshot.AMInputRefs[0].GroupKey == "" {
		t.Fatal("AM versus target receiver/group identities conflated")
	}
	if path := os.Getenv("JEV_FINAL_SNAPSHOT_FIXTURE_OUTPUT"); path != "" {
		raw, _ := json.MarshalIndent(snapshot, "", "  ")
		if err := os.WriteFile(path, append(raw, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
}
func TestFinalSnapshotSupports1001012001000AndRejectsOverBudgetWithoutTruncation(t *testing.T) {
	for _, n := range []int{100, 101, 200, 1000} {
		service, data := finalSnapshotFixture(n)
		snapshot, err := service.buildFinalSnapshot(data, "jev-shadow-feishu-uat", "oc_test")
		if err != nil || snapshot.MemberCount != n || len(snapshot.Members) != n {
			t.Fatalf("n=%d %v", n, err)
		}
	}
	service, data := finalSnapshotFixture(1001)
	if _, err := service.buildFinalSnapshot(data, "jev-shadow-feishu-uat", "oc_test"); err == nil {
		t.Fatal("over member budget silently truncated")
	}
	service, data = finalSnapshotFixture(14)
	data.Alerts[0].Annotations["large"] = strings.Repeat("x", MaxFinalSnapshotBytes)
	if _, err := service.buildFinalSnapshot(data, "jev-shadow-feishu-uat", "oc_test"); err == nil {
		t.Fatal("over byte budget silently truncated")
	}
}
func TestFinalSnapshotHashScopeMemberAndOriginalLocatorTamperingRejected(t *testing.T) {
	service, data := finalSnapshotFixture(2)
	snapshot, err := service.buildFinalSnapshot(data, "jev-shadow-feishu-uat", "oc_test")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(snapshot)
	cases := []func(*FinalSnapshot){func(s *FinalSnapshot) { s.Destination = "other" }, func(s *FinalSnapshot) { s.MembersHash = strings.Repeat("0", 64) }, func(s *FinalSnapshot) { s.Members[0].GeneratorURL = "http://another" }, func(s *FinalSnapshot) { s.Members[0].RawEntryIndexes = []int{100} }, func(s *FinalSnapshot) { s.AMInputRefs[0].GroupKey = "different" }}
	for _, mutate := range cases {
		var copy FinalSnapshot
		_ = json.Unmarshal(raw, &copy)
		mutate(&copy)
		if validateFinalSnapshot(&copy) == nil {
			t.Fatal("snapshot tampering accepted")
		}
	}
}
func TestFinalSnapshotCanonicalJSONMatchesPythonUTF8AndDoesNotMutateLegacyHash(t *testing.T) {
	raw, err := groupCanonicalJSON(struct {
		B string `json:"b"`
		A string `json:"a"`
	}{B: "line\u2028split", A: "<你好>\\u2028"})
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "{\"a\":\"<你好>\\\\u2028\",\"b\":\"line\u2028split\"}" {
		t.Fatalf("canonical ordering/UTF8 mismatch: %s", raw)
	}
}
func TestAMInputMetadataDoesNotChangeOriginalAlertIdentityOrGeneratorSemantics(t *testing.T) {
	original := persistentCardTestData().Alerts[0]
	original.GeneratorURL = "http://vmalert/alert?group_id=1&alert_id=2"
	before := utils.Hash(original)
	with := original.Clone()
	with.AMInputRefs = []template.AMInputReference{{SchemaVersion: "am-input-v1", ReferenceKind: "raw_payload_sha256", RequestSHA256: strings.Repeat("a", 64), Receiver: "one", GroupKey: "one", GroupLabels: template.KV{"cluster": "one"}}}
	if utils.Hash(with) != before {
		t.Fatal("AM metadata changed old Alert.ID")
	}
	with.AMInputRefs[0].RequestSHA256 = strings.Repeat("b", 64)
	with.AMInputRefs[0].GroupKey = "other"
	if utils.Hash(with) != before {
		t.Fatal("different AM batch metadata changed original dedupe identity")
	}
	with.GeneratorURL = "http://vmalert/alert?group_id=1&alert_id=3"
	if utils.Hash(with) == before {
		t.Fatal("old GeneratorURL identity participation unexpectedly changed")
	}
	copied := original.Clone()
	copied.StartsAt = copied.StartsAt.Add(time.Second)
	if utils.Hash(copied) == before {
		t.Fatal("old startsAt identity unexpectedly changed")
	}
}
