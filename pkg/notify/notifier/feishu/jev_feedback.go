package feishu

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher/callback"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"

	"github.com/kubesphere/notification-manager/pkg/jevshadow"
)

const jevFeedbackTimeout = 2 * time.Second

func StartJevFeedbackListener(
	ctx context.Context,
	logger log.Logger,
	shadow *jevshadow.Service,
	appID string,
	appSecret string,
) error {
	if shadow == nil || !shadow.FeedbackEnabled() {
		return nil
	}
	handler := dispatcher.NewEventDispatcher("", "").OnP2CardActionTrigger(
		func(callbackContext context.Context, event *callback.CardActionTriggerEvent) (*callback.CardActionTriggerResponse, error) {
			return handleJevCardAction(callbackContext, logger, shadow, appID, event), nil
		},
	)
	client := larkws.NewClient(
		appID,
		appSecret,
		larkws.WithEventHandler(handler),
		larkws.WithLogLevel(larkcore.LogLevelInfo),
	)
	_ = level.Info(logger).Log("msg", "Jev Feishu feedback listener starting")
	return client.Start(ctx)
}

func handleJevCardAction(
	ctx context.Context,
	logger log.Logger,
	shadow *jevshadow.Service,
	expectedAppID string,
	event *callback.CardActionTriggerEvent,
) *callback.CardActionTriggerResponse {
	errorToast := func(content string) *callback.CardActionTriggerResponse {
		return &callback.CardActionTriggerResponse{Toast: &callback.Toast{Type: "error", Content: content}}
	}
	if event == nil || event.Event == nil || event.Event.Operator == nil ||
		event.Event.Context == nil || event.Event.Action == nil || event.EventV2Base == nil ||
		event.EventV2Base.Header == nil {
		return errorToast("反馈事件缺少可信身份，请重试")
	}
	if event.EventV2Base.Header.AppID != expectedAppID {
		return errorToast("反馈事件不属于当前告警应用")
	}
	action := event.Event.Action
	if action.Tag != "button" || stringValue(action.Value, "action") != "jev_feedback" ||
		stringValue(action.Value, "dimension") != "overall" {
		return errorToast("不是 Jev 反馈操作")
	}
	feedback := jevshadow.CardFeedback{
		SourceEventID:   event.EventV2Base.Header.EventID,
		ActorID:         event.Event.Operator.OpenID,
		ChatID:          event.Event.Context.OpenChatID,
		MessageID:       event.Event.Context.OpenMessageID,
		ActionReference: stringValue(action.Value, "action_reference"),
		CorrectLabel:    stringValue(action.Value, "correct_label"),
	}
	feedbackContext, cancel := context.WithTimeout(ctx, jevFeedbackTimeout)
	defer cancel()
	result, err := shadow.RelayCardFeedback(feedbackContext, feedback)
	if err != nil {
		_ = level.Error(logger).Log(
			"msg", "Jev Feishu feedback rejected",
			"event_id", feedback.SourceEventID,
			"message_id", feedback.MessageID,
			"error", err,
		)
		return errorToast("反馈记录失败，请稍后重试")
	}
	_ = level.Info(logger).Log(
		"msg", "Jev Feishu feedback recorded",
		"event_id", feedback.SourceEventID,
		"message_id", feedback.MessageID,
		"feedback_id", result.ID,
		"duplicate", result.Duplicate,
	)
	content := "反馈已记录"
	if result.Duplicate {
		content = "反馈已记录（重复事件已忽略）"
	}
	return &callback.CardActionTriggerResponse{Toast: &callback.Toast{Type: "success", Content: content}}
}

func stringValue(values map[string]interface{}, key string) string {
	value, ok := values[key]
	if !ok {
		return ""
	}
	result, ok := value.(string)
	if !ok {
		return ""
	}
	return result
}

func ValidateJevFeedbackApp(appID, appSecret string) error {
	if appID == "" || appSecret == "" {
		return fmt.Errorf("Feishu feedback app credentials are empty")
	}
	return nil
}
