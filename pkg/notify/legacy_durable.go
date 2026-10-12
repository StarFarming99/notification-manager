package notify

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/kubesphere/notification-manager/apis/v2beta2"
	"github.com/kubesphere/notification-manager/pkg/constants"
	"github.com/kubesphere/notification-manager/pkg/controller"
	"github.com/kubesphere/notification-manager/pkg/internal"
	dingtalktype "github.com/kubesphere/notification-manager/pkg/internal/dingtalk"
	discordtype "github.com/kubesphere/notification-manager/pkg/internal/discord"
	emailtype "github.com/kubesphere/notification-manager/pkg/internal/email"
	feishutype "github.com/kubesphere/notification-manager/pkg/internal/feishu"
	pushovertype "github.com/kubesphere/notification-manager/pkg/internal/pushover"
	slacktype "github.com/kubesphere/notification-manager/pkg/internal/slack"
	smstype "github.com/kubesphere/notification-manager/pkg/internal/sms"
	telegramtype "github.com/kubesphere/notification-manager/pkg/internal/telegram"
	webhooktype "github.com/kubesphere/notification-manager/pkg/internal/webhook"
	wechattype "github.com/kubesphere/notification-manager/pkg/internal/wechat"
	spool "github.com/kubesphere/notification-manager/pkg/notification_spool"
	"github.com/kubesphere/notification-manager/pkg/template"
)

func legacyReceiver(channel string) internal.Receiver {
	switch channel {
	case constants.Email:
		return &emailtype.Receiver{}
	case constants.SMS:
		return &smstype.Receiver{}
	case constants.Slack:
		return &slacktype.Receiver{}
	case constants.Telegram:
		return &telegramtype.Receiver{}
	case constants.Discord:
		return &discordtype.Receiver{}
	case constants.DingTalk:
		return &dingtalktype.Receiver{}
	case constants.WeChat:
		return &wechattype.Receiver{}
	case constants.Pushover:
		return &pushovertype.Receiver{}
	}
	return nil
}
func receiverCommon(receiver internal.Receiver) *internal.Common {
	switch r := receiver.(type) {
	case *emailtype.Receiver:
		return r.Common
	case *smstype.Receiver:
		return r.Common
	case *slacktype.Receiver:
		return r.Common
	case *telegramtype.Receiver:
		return r.Common
	case *discordtype.Receiver:
		return r.Common
	case *dingtalktype.Receiver:
		return r.Common
	case *wechattype.Receiver:
		return r.Common
	case *pushovertype.Receiver:
		return r.Common
	case *feishutype.Receiver:
		return r.Common
	case *webhooktype.Receiver:
		return r.Common
	}
	return nil
}
func receiverConfigName(receiver internal.Receiver) string {
	switch r := receiver.(type) {
	case *emailtype.Receiver:
		if r.Config != nil && r.Config.Common != nil {
			return r.Config.Common.Name
		}
	case *smstype.Receiver:
		if r.Config != nil && r.Config.Common != nil {
			return r.Config.Common.Name
		}
	case *slacktype.Receiver:
		if r.Config != nil && r.Config.Common != nil {
			return r.Config.Common.Name
		}
	case *telegramtype.Receiver:
		if r.Config != nil && r.Config.Common != nil {
			return r.Config.Common.Name
		}
	case *dingtalktype.Receiver:
		if r.Config != nil && r.Config.Common != nil {
			return r.Config.Common.Name
		}
	case *wechattype.Receiver:
		if r.Config != nil && r.Config.Common != nil {
			return r.Config.Common.Name
		}
	case *pushovertype.Receiver:
		if r.Config != nil && r.Config.Common != nil {
			return r.Config.Common.Name
		}
	case *feishutype.Receiver:
		if r.Config != nil && r.Config.Common != nil {
			return r.Config.Common.Name
		}
	}
	return ""
}
func destinationIdentity(value interface{}) string {
	raw, _ := json.Marshal(value)
	return spool.Hash(raw)
}

// All original upstream channels remain available in formal delivery. Each
// independently sendable destination gets a separate ledger outcome; no Jev
// scope is required outside the explicitly installed annotation destination.
func legacyDestinations(receiver internal.Receiver) ([]frozenDestination, error) {
	var result []frozenDestination
	add := func(r internal.Receiver, destination string) {
		common := receiverCommon(r)
		common.Frozen = true
		common.TmplText = nil
		result = append(result, frozenDestination{r, destination})
	}
	switch r := receiver.(type) {
	case *emailtype.Receiver:
		for _, id := range r.To {
			v := r.Clone().(*emailtype.Receiver)
			v.To = []string{id}
			add(v, "email:"+destinationIdentity(id))
		}
	case *smstype.Receiver:
		for _, id := range r.PhoneNumbers {
			v := r.Clone().(*smstype.Receiver)
			v.PhoneNumbers = []string{id}
			add(v, "sms:"+destinationIdentity(id))
		}
	case *slacktype.Receiver:
		for _, id := range r.Channels {
			v := r.Clone().(*slacktype.Receiver)
			v.Channels = []string{id}
			add(v, "slack:"+destinationIdentity(id))
		}
	case *telegramtype.Receiver:
		for _, id := range r.Channels {
			v := r.Clone().(*telegramtype.Receiver)
			v.Channels = []string{id}
			add(v, "telegram:"+destinationIdentity(id))
		}
	case *discordtype.Receiver:
		if r.Webhook != nil {
			add(r.Clone(), "discord:"+destinationIdentity(r.Webhook))
		}
	case *dingtalktype.Receiver:
		for _, id := range r.ChatIDs {
			v := r.Clone().(*dingtalktype.Receiver)
			v.ChatIDs = []string{id}
			v.ChatBot = nil
			add(v, "dingtalk:"+destinationIdentity(id))
		}
		if r.ChatBot != nil {
			v := r.Clone().(*dingtalktype.Receiver)
			v.ChatIDs = nil
			v.Config = nil
			add(v, "dingtalk-bot:"+destinationIdentity(r.ChatBot.Webhook))
		}
	case *wechattype.Receiver:
		base := func() *wechattype.Receiver {
			v := r.Clone().(*wechattype.Receiver)
			v.ToUser = nil
			v.ToParty = nil
			v.ToTag = nil
			v.ChatBot = nil
			return v
		}
		for _, id := range r.ToUser {
			v := base()
			v.ToUser = []string{id}
			add(v, "wechat-user:"+destinationIdentity(id))
		}
		for _, id := range r.ToParty {
			v := base()
			v.ToParty = []string{id}
			add(v, "wechat-party:"+destinationIdentity(id))
		}
		for _, id := range r.ToTag {
			v := base()
			v.ToTag = []string{id}
			add(v, "wechat-tag:"+destinationIdentity(id))
		}
		if r.ChatBot != nil {
			v := base()
			v.ChatBot = r.ChatBot
			v.Config = nil
			add(v, "wechat-bot:"+destinationIdentity(r.ChatBot.Webhook))
		}
	case *pushovertype.Receiver:
		for _, profile := range r.Profiles {
			v := r.Clone().(*pushovertype.Receiver)
			v.Profiles = []*v2beta2.PushoverUserProfile{profile}
			add(v, "pushover:"+destinationIdentity(profile))
		}
	default:
		return nil, errors.New("original notification channel has no durable destination adapter")
	}
	if len(result) == 0 {
		return nil, errors.New("notification has no destination")
	}
	return result, nil
}
func annotationBindingConfigured(ctl *controller.Controller, id, version, receiver, destination string) bool {
	shadow := ctl.GetJevShadow()
	return shadow != nil && shadow.Enabled() && shadow.ProfileBindingConfigured(id, version, receiver, destination)
}
func profileAnnotationTarget(ctl *controller.Controller, data *template.Data, receiver internal.Receiver, destination string) bool {
	if data == nil || data.JevCaptureExcluded || data.ProfileID == "" || receiver.GetType() != constants.Feishu || !strings.HasPrefix(destination, "chat:") {
		return false
	}
	return data.ProfileID == "test" || annotationBindingConfigured(ctl, data.ProfileID, data.ProfileVersion, receiver.GetName(), strings.TrimPrefix(destination, "chat:"))
}
