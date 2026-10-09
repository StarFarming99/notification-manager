package template

import (
	"encoding/json"
	"testing"
)

func TestAlertGeneratorURLSurvivesWebhookRoundTripAndClone(t *testing.T) {
	const generatorURL = "http://vmalert.example/vmalert/alert?group_id=group-a&alert_id=alert-a"
	payload := []byte(`{
		"alerts":[{
			"id":"alert-a",
			"status":"firing",
			"labels":{"alertname":"NodeMemoryHigh"},
			"annotations":{},
			"startsAt":"2026-09-29T09:21:00Z",
			"generatorURL":"` + generatorURL + `"
		}],
		"groupLabels":{"alertname":"NodeMemoryHigh"}
	}`)

	var data Data
	if err := json.Unmarshal(payload, &data); err != nil {
		t.Fatalf("unmarshal webhook payload: %v", err)
	}
	if got := data.Alerts[0].GeneratorURL; got != generatorURL {
		t.Fatalf("generatorURL lost during unmarshal: got %q", got)
	}
	if got := data.Alerts[0].Clone().GeneratorURL; got != generatorURL {
		t.Fatalf("generatorURL lost during clone: got %q", got)
	}
	encoded, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("marshal webhook payload: %v", err)
	}
	var roundTrip Data
	if err := json.Unmarshal(encoded, &roundTrip); err != nil {
		t.Fatalf("unmarshal serialized payload: %v", err)
	}
	if got := roundTrip.Alerts[0].GeneratorURL; got != generatorURL {
		t.Fatalf("generatorURL lost during serialization: got %q", got)
	}
}
