package notify

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/kubesphere/notification-manager/pkg/internal"
	webhooktype "github.com/kubesphere/notification-manager/pkg/internal/webhook"
	"github.com/kubesphere/notification-manager/pkg/template"
)

func TestGroupMetadataCompactsExactPostAggregationEntriesWithoutChangingRender(t *testing.T) {
	ctl := testController(t, `{{define "frozen"}}original:{{(index .Alerts 0).Labels.alertname}}{{end}}`)
	receiver := &webhooktype.Receiver{Common: &internal.Common{Name: "webhook-test", Type: "webhook", Template: internal.Template{TmplName: "frozen"}}, URL: "http://mock.invalid/webhook"}
	data := &template.Data{GroupLabels: template.KV{"alertname": "KubePodNotReady"}}
	ref := template.AMInputReference{SchemaVersion: "am-input-v1", ReferenceKind: "raw_payload_sha256", RequestSHA256: strings.Repeat("a", 64), Receiver: "am-native", GroupKey: "original-AM-group-key", GroupLabels: data.GroupLabels.Clone(), ExternalURL: "http://am.fixture"}
	for i := 0; i < 1000; i++ {
		data.Alerts = append(data.Alerts, &template.Alert{ID: "fixture", Labels: template.KV{"alertname": "KubePodNotReady"}, GeneratorURL: "http://vmalert/alert?group_id=1&alert_id=2", AMInputRefs: []template.AMInputReference{ref.Clone()}})
	}
	targets, err := Freeze(log.NewNopLogger(), ctl, map[internal.Receiver][]*template.Data{receiver: {data}})
	if err != nil {
		t.Fatal(err)
	}
	var frozen FrozenNotification
	_ = json.Unmarshal(targets[0].Payload, &frozen)
	if frozen.Content != "original:KubePodNotReady" || len(frozen.Data.Alerts) != 1000 || len(frozen.Data.AMInputRefs) != 1 || len(frozen.Data.AMInputRefs[0].EntryIndexes) != 1000 || frozen.Data.Alerts[999].GeneratorURL != data.Alerts[999].GeneratorURL {
		t.Fatal("frozen original render/members/metadata changed")
	}
	if len(frozen.Data.Alerts[0].AMInputRefs) != 0 || len(data.Alerts[0].AMInputRefs) != 1 {
		t.Fatal("metadata duplicated or original input mutated")
	}
	for i, index := range frozen.Data.AMInputRefs[0].EntryIndexes {
		if i != index {
			t.Fatal("entry mapping lost", i, index)
		}
	}
}
func TestOversizedOptionalGroupMetadataCannotIntroduceBaseNotificationRejection(t *testing.T) {
	ctl := testController(t, `{{define "frozen"}}original-base{{end}}`)
	receiver := &webhooktype.Receiver{Common: &internal.Common{Name: "webhook-test", Type: "webhook", Template: internal.Template{TmplName: "frozen"}}, URL: "http://mock.invalid/webhook"}
	data := &template.Data{Alerts: template.Alerts{{ID: "fixture", Labels: template.KV{"alertname": "fixture"}, AMInputRefs: []template.AMInputReference{{SchemaVersion: "am-input-v1", ReferenceKind: "raw_payload_sha256", RequestSHA256: strings.Repeat("a", 64), GroupKey: strings.Repeat("x", 2<<20)}}}}}
	targets, err := Freeze(log.NewNopLogger(), ctl, map[internal.Receiver][]*template.Data{receiver: {data}})
	if err != nil {
		t.Fatal("optional metadata rejected original notification", err)
	}
	var frozen FrozenNotification
	_ = json.Unmarshal(targets[0].Payload, &frozen)
	if frozen.Content != "original-base" || len(frozen.Data.Alerts) != 1 || len(frozen.Data.AMInputRefs) != 0 || frozen.Data.GroupCaptureIssue != "am_input_metadata_byte_budget" {
		t.Fatal("oversize was truncated silently or changed base")
	}
}
