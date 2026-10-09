package feishu

import (
	"context"
	"testing"

	"github.com/go-kit/kit/log"
	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher/callback"

	"github.com/kubesphere/notification-manager/pkg/jevshadow"
)

func TestHandleJevCardActionRejectsUntrustedIdentity(t *testing.T) {
	response := handleJevCardAction(context.Background(), log.NewNopLogger(), &jevshadow.Service{}, "cli_uat", &callback.CardActionTriggerEvent{})
	if response == nil || response.Toast == nil || response.Toast.Type != "error" {
		t.Fatalf("expected error toast, got %#v", response)
	}
}

func TestHandleJevCardActionRejectsOtherActionsBeforeRelay(t *testing.T) {
	response := handleJevCardAction(context.Background(), log.NewNopLogger(), &jevshadow.Service{}, "cli_uat", &callback.CardActionTriggerEvent{
		EventV2Base: &larkevent.EventV2Base{Header: &larkevent.EventHeader{EventID: "event-1", AppID: "cli_uat"}},
		Event: &callback.CardActionTriggerRequest{
			Operator: &callback.Operator{OpenID: "ou_actor"},
			Context:  &callback.Context{OpenChatID: "oc_uat", OpenMessageID: "om_message"},
			Action: &callback.CallBackAction{
				Tag:   "button",
				Value: map[string]interface{}{"action": "other"},
			},
		},
	})
	if response == nil || response.Toast == nil || response.Toast.Content != "不是 Jev 反馈操作" {
		t.Fatalf("unexpected response %#v", response)
	}
}
