package feishu

import (
	"encoding/json"
	"testing"

	"github.com/kubesphere/notification-manager/pkg/constants"
	"github.com/kubesphere/notification-manager/pkg/internal"
	feishutype "github.com/kubesphere/notification-manager/pkg/internal/feishu"
	"github.com/kubesphere/notification-manager/pkg/template"
)

func TestDurableRenderPreservesOriginalCardConfigAndButtons(t *testing.T) {
	for _, original := range []string{`{"config":{"wide_screen_mode":true,"enable_forward":true},"elements":[{"tag":"action","actions":[{"tag":"button","value":{"action":"acknowledge"}}]}]}`, `{"config":{"wide_screen_mode":false,"enable_forward":false,"custom":"keep"},"elements":[]}`} {
		tmpl, err := template.New("", nil)
		if err != nil {
			t.Fatal(err)
		}
		tmpl, err = tmpl.ParserText(`{{define "fixture"}}` + original + `{{end}}`)
		if err != nil {
			t.Fatal(err)
		}
		n := &Notifier{tmpl: tmpl, receiver: &feishutype.Receiver{Common: &internal.Common{Template: internal.Template{TmplName: "fixture", TmplType: constants.Interactive}}}}
		frozen, err := n.RenderForDurable(&template.Data{})
		if err != nil {
			t.Fatal(err)
		}
		var old, new map[string]interface{}
		if err := json.Unmarshal([]byte(original), &old); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(frozen), &new); err != nil {
			t.Fatal(err)
		}
		config := new["config"].(map[string]interface{})
		if config["update_multi"] != true {
			t.Fatal("shared-card semantics absent", frozen)
		}
		for key, value := range old["config"].(map[string]interface{}) {
			if config[key] != value {
				t.Fatal("explicit original card config changed", key, value, config)
			}
		}
		rawOld, _ := json.Marshal(old["elements"])
		rawNew, _ := json.Marshal(new["elements"])
		if string(rawOld) != string(rawNew) {
			t.Fatal("original buttons changed", frozen)
		}
		changed, err := n.RenderForDurable(&template.Data{FrozenContent: &frozen})
		if err != nil || changed != frozen {
			t.Fatal("frozen content re-rendered", err)
		}
	}
}
