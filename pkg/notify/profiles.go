package notify

import (
	"encoding/json"
	"errors"

	"github.com/kubesphere/notification-manager/pkg/internal"
	feishutype "github.com/kubesphere/notification-manager/pkg/internal/feishu"
	spool "github.com/kubesphere/notification-manager/pkg/notification_spool"
	"github.com/kubesphere/notification-manager/pkg/template"
)

// ApplyProfile runs after the original route/filter/aggregation pipeline. The
// test lane copies exactly the original scoped chat intent, and no other
// channels or destinations. The shared Receiver CR is never mutated.
func ApplyProfile(input map[internal.Receiver][]*template.Data, snap spool.ProfileSnapshot) map[internal.Receiver][]*template.Data {
	output := map[internal.Receiver][]*template.Data{}
	for receiver, groups := range input {
		r := receiver.Clone()
		if snap.ID == "test" {
			f, ok := r.(*feishutype.Receiver)
			if !ok || f.Name != snap.Receiver {
				continue
			}
			found := false
			for _, chat := range f.ChatIDs {
				if chat == snap.SourceChatID {
					found = true
				}
			}
			if !found {
				continue
			}
			f.ChatIDs = []string{snap.TestChatID}
			f.User = nil
			f.Department = nil
			f.ChatBot = nil
		}
		for _, group := range groups {
			data := group.Clone()
			if snap.ID == "test" {
				data.Alerts = nil
				for _, alert := range group.Alerts {
					if alert.Labels["severity"] == "critical" {
						data.Alerts = append(data.Alerts, alert.Clone())
					}
				}
				if len(data.Alerts) == 0 {
					continue
				}
				data.Format()
			}
			data.ProfileID = snap.ID
			data.ProfileVersion = snap.Version
			data.CardOwnerID = snap.CardOwnerID
			if snap.ID == "test" {
				data.OriginalDestination = "chat:" + snap.SourceChatID
			}
			output[r] = append(output[r], data)
		}
	}
	return output
}

// BindFrozenProfile includes provenance in the frozen content identity. Both
// automatic workers and recovery validate this registered immutable version.
func BindFrozenProfile(targets []spool.Target, snap spool.ProfileSnapshot) ([]spool.Target, error) {
	for i := range targets {
		t := &targets[i]
		t.ProfileID = snap.ID
		t.ProfileVersion = snap.Version
		t.CardOwnerID = snap.CardOwnerID
		t.OriginalDestination = t.Destination
		if snap.ID == "test" {
			t.OriginalDestination = "chat:" + snap.SourceChatID
		}
		var frozen FrozenNotification
		if err := json.Unmarshal(t.Payload, &frozen); err != nil {
			return nil, err
		}
		if frozen.Data == nil {
			return nil, errors.New("missing frozen data")
		}
		frozen.Data.ProfileID = snap.ID
		frozen.Data.ProfileVersion = snap.Version
		frozen.Data.CardOwnerID = snap.CardOwnerID
		frozen.Data.OriginalDestination = t.OriginalDestination
		payload, err := json.Marshal(frozen)
		if err != nil {
			return nil, err
		}
		t.Payload = payload
		t.ContentRevision = spool.Hash(payload)
	}
	return targets, nil
}
