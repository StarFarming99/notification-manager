package notify

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/kubesphere/notification-manager/apis/v2beta2"
	"github.com/kubesphere/notification-manager/pkg/constants"
	"github.com/kubesphere/notification-manager/pkg/controller"
	"github.com/kubesphere/notification-manager/pkg/internal"
	feishutype "github.com/kubesphere/notification-manager/pkg/internal/feishu"
	webhooktype "github.com/kubesphere/notification-manager/pkg/internal/webhook"
	spool "github.com/kubesphere/notification-manager/pkg/notification_spool"
	"github.com/kubesphere/notification-manager/pkg/template"
	"github.com/kubesphere/notification-manager/pkg/utils"
)

type FrozenNotification struct {
	Version  int              `json:"version"`
	Receiver json.RawMessage  `json:"receiver"`
	Options  *v2beta2.Options `json:"options,omitempty"`
	Data     *template.Data   `json:"data"`
	Content  string           `json:"content"`
}

// assertReferences excludes literal credentials from durable records. Public
// application IDs are identifiers; bot URLs, app secrets and bearer keys are not.
func assertReferences(value interface{}, field string) error {
	switch v := value.(type) {
	case map[string]interface{}:
		if literal, ok := v["value"].(string); ok && literal != "" && field != "appID" {
			return errors.New("durable notifications require credential references")
		}
		for k, child := range v {
			if err := assertReferences(child, k); err != nil {
				return err
			}
		}
	case []interface{}:
		for _, child := range v {
			if err := assertReferences(child, field); err != nil {
				return err
			}
		}
	}
	return nil
}

type frozenDestination struct {
	receiver    internal.Receiver
	destination string
}

func destinations(receiver internal.Receiver) ([]frozenDestination, error) {
	var result []frozenDestination
	switch r := receiver.(type) {
	case *feishutype.Receiver:
		base := func() *feishutype.Receiver {
			v := r.Clone().(*feishutype.Receiver)
			v.ChatIDs = nil
			v.User = nil
			v.Department = nil
			v.ChatBot = nil
			v.Frozen = true
			v.TmplText = nil
			return v
		}
		for _, id := range r.ChatIDs {
			v := base()
			v.ChatIDs = []string{id}
			result = append(result, frozenDestination{v, "chat:" + id})
		}
		for _, id := range r.User {
			v := base()
			v.User = []string{id}
			result = append(result, frozenDestination{v, "user:" + id})
		}
		for _, id := range r.Department {
			v := base()
			v.Department = []string{id}
			result = append(result, frozenDestination{v, "department:" + id})
		}
		if r.ChatBot != nil {
			v := base()
			v.ChatBot = r.ChatBot
			ref, _ := json.Marshal(r.ChatBot.Webhook)
			result = append(result, frozenDestination{v, "bot-ref:" + spool.Hash(ref)})
		}
	case *webhooktype.Receiver:
		u, err := url.Parse(r.URL)
		if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") {
			return nil, errors.New("durable webhook must use a credential-free URL and referenced HTTP credentials")
		}
		if r.HttpConfig != nil && r.HttpConfig.ProxyURL != "" {
			p, err := url.Parse(r.HttpConfig.ProxyURL)
			if err != nil || p.User != nil || p.RawQuery != "" {
				return nil, errors.New("proxy URL must not contain credentials")
			}
		}
		v := r.Clone().(*webhooktype.Receiver)
		v.Frozen = true
		v.TmplText = nil
		result = append(result, frozenDestination{v, "webhook:" + spool.Hash([]byte(r.URL))})
	default:
		return nil, fmt.Errorf("durable target adapter unavailable for channel %s", receiver.GetType())
	}
	if len(result) == 0 {
		return nil, errors.New("notification has no destination")
	}
	return result, nil
}

// Freeze is called after the original silence/route/filter/aggregation stages,
// before the request is acknowledged. Each destination owns its own outcome.
func Freeze(logger log.Logger, ctl *controller.Controller, input map[internal.Receiver][]*template.Data) ([]spool.Target, error) {
	var result []spool.Target
	for receiver, groups := range input {
		r := receiver.Clone()
		factory, ok := factories[r.GetType()]
		if !ok {
			return nil, errors.New("unknown notification channel")
		}
		nf, err := factory(logger, r, ctl)
		if err != nil {
			return nil, err
		}
		renderer, ok := nf.(interface {
			RenderForDurable(*template.Data) (string, error)
		})
		if !ok {
			return nil, fmt.Errorf("channel %s has no durable renderer", r.GetType())
		}
		for _, group := range groups {
			data := group.Clone()
			for _, alert := range data.Alerts {
				if alert.Labels == nil {
					alert.Labels = template.KV{}
				}
				if alert.Labels[constants.ReceiverName] == "" {
					alert.Labels[constants.ReceiverName] = r.GetName()
				}
			}
			content, err := renderer.RenderForDurable(data)
			if err != nil {
				return nil, err
			}
			targets, err := destinations(r)
			if err != nil {
				return nil, err
			}
			for _, target := range targets {
				if data.ProfileID != "" {
					if f, ok := target.receiver.(*feishutype.Receiver); ok && f.Config != nil {
						if shadow := ctl.GetJevShadow(); shadow != nil && shadow.Enabled() {
							appID, err := ctl.GetCredential(f.AppID)
							if err != nil {
								return nil, err
							}
							if err := shadow.ValidateProfileSender(data.ProfileID, data.ProfileVersion, appID); err != nil {
								return nil, err
							}
						}
					}
				}
				if f, ok := target.receiver.(*feishutype.Receiver); ok && f.Config != nil && f.AppSecret != nil && f.AppSecret.Value != "" {
					ref, err := ctl.FreezeFeishuCredentialReference(f.Config)
					if err != nil {
						return nil, err
					}
					config := *f.Config
					config.AppSecret = nil
					config.AppID = &v2beta2.Credential{Value: ref.AppID}
					config.CredentialConfigRef = ref
					f.Config = &config
				}
				raw, err := json.Marshal(target.receiver)
				if err != nil {
					return nil, err
				}
				var decoded interface{}
				if err := json.Unmarshal(raw, &decoded); err != nil {
					return nil, err
				}
				if err := assertReferences(decoded, ""); err != nil {
					return nil, err
				}
				payload, err := json.Marshal(FrozenNotification{Version: spool.Version, Receiver: raw, Options: ctl.ReceiverOpts, Data: data, Content: content})
				if err != nil {
					return nil, err
				}
				if len(payload) > 2<<20 {
					return nil, errors.New("frozen notification exceeds 2 MiB")
				}
				result = append(result, spool.Target{Receiver: r.GetName(), Channel: r.GetType(), Destination: target.destination, ContentRevision: spool.Hash(payload), Payload: payload})
			}
		}
	}
	sort.Slice(result, func(i, j int) bool {
		a, b := result[i], result[j]
		return strings.Join([]string{a.Receiver, a.Channel, a.Destination, a.ContentRevision}, "\x00") < strings.Join([]string{b.Receiver, b.Channel, b.Destination, b.ContentRevision}, "\x00")
	})
	return result, nil
}

// SendFrozen never re-routes or re-renders. Live credential references may be
// resolved, but a receiver/template edit cannot change the accepted target.
func SendFrozen(ctx context.Context, logger log.Logger, ctl *controller.Controller, target spool.Target) (sendErr error) {
	ctx, attempt := utils.TrackDelivery(ctx)
	defer func() {
		if sendErr != nil && !attempt.Started() {
			sendErr = &utils.PreSendError{Err: sendErr}
		}
	}()
	if ctl.DeliveryProfiles != nil {
		p, err := ctl.DeliveryProfiles.Store.ProfileVersion(target.ProfileID, target.ProfileVersion)
		if err != nil {
			return err
		}
		if target.Channel == constants.Feishu && strings.HasPrefix(target.Destination, "chat:") {
			if shadow := ctl.GetJevShadow(); shadow != nil && shadow.Enabled() {
				if err := shadow.ValidateProfileBinding(p.ID, p.Version, p.CardOwnerID, target.Receiver, strings.TrimPrefix(target.Destination, "chat:")); err != nil {
					return err
				}
			}
		}
		if p.CardOwnerID != target.CardOwnerID {
			return errors.New("frozen profile owner invalid")
		}
		if p.ID == "test" && (target.Destination != "chat:"+p.TestChatID || target.Channel != constants.Feishu || target.Receiver != p.Receiver || target.OriginalDestination != "chat:"+p.SourceChatID) {
			return errors.New("frozen test destination invalid")
		}
	}
	var frozen FrozenNotification
	if err := json.Unmarshal(target.Payload, &frozen); err != nil {
		return err
	}
	if frozen.Version != spool.Version || frozen.Data == nil {
		return errors.New("unsupported frozen notification")
	}
	var receiver internal.Receiver
	switch target.Channel {
	case constants.Feishu:
		receiver = &feishutype.Receiver{}
	case constants.Webhook:
		receiver = &webhooktype.Receiver{}
	default:
		return errors.New("unsupported frozen channel")
	}
	if err := json.Unmarshal(frozen.Receiver, receiver); err != nil {
		return err
	}
	if target.ProfileID != "" {
		if frozen.Data.ProfileID != target.ProfileID || frozen.Data.ProfileVersion != target.ProfileVersion || frozen.Data.CardOwnerID != target.CardOwnerID || frozen.Data.OriginalDestination != target.OriginalDestination {
			return errors.New("frozen profile payload mismatch")
		}
		if f, ok := receiver.(*feishutype.Receiver); ok {
			if len(f.ChatIDs) != 1 || "chat:"+f.ChatIDs[0] != target.Destination || len(f.User) > 0 || len(f.Department) > 0 || f.ChatBot != nil {
				if target.ProfileID == "test" {
					return errors.New("frozen test transport escapes destination fence")
				}
			}
		}
	}
	if target.ProfileID != "" {
		if f, ok := receiver.(*feishutype.Receiver); ok && f.Config != nil {
			if shadow := ctl.GetJevShadow(); shadow != nil && shadow.Enabled() {
				appID, err := ctl.GetCredential(f.AppID)
				if err != nil {
					return err
				}
				if err := shadow.ValidateProfileSender(target.ProfileID, target.ProfileVersion, appID); err != nil {
					return err
				}
			}
		}
	}
	if f, ok := receiver.(*feishutype.Receiver); ok && f.Config != nil && f.CredentialConfigRef != nil {
		secret, err := ctl.ResolveFeishuCredentialReference(f.CredentialConfigRef)
		if err != nil {
			return err
		}
		if f.AppID == nil || f.AppID.Value != f.CredentialConfigRef.AppID {
			return errors.New("frozen app identity does not match credential config reference")
		}
		config := *f.Config
		config.AppSecret = &v2beta2.Credential{Value: secret}
		f.Config = &config
	}
	// Receiver defaults were frozen at intake; configure only transport options.
	nf, err := factories[target.Channel](logger, receiver, ctl)
	if err != nil {
		return err
	}
	if configured, ok := nf.(interface{ ConfigureFrozen(*v2beta2.Options) }); ok {
		configured.ConfigureFrozen(frozen.Options)
	}
	frozen.Data.FrozenContent = &frozen.Content
	return nf.Notify(ctx, frozen.Data)
}

// FreezeHistory preserves per-alert NotifySuccessful semantics while original
// destination delivery remains target-scoped. Each history message is frozen
// separately and waits for a confirmed original delivery of that alert.
func FreezeHistory(logger log.Logger, ctl *controller.Controller, primary []spool.Target) ([]spool.Target, error) {
	receivers := ctl.GetHistoryReceivers()
	// History has no public Receiver CR name. Bind only the frozen copy to a
	// reserved ledger identity; legacy memory notification rendering is unchanged.
	for i, receiver := range receivers {
		r := receiver.Clone().(*webhooktype.Receiver)
		r.Name = "__nm_internal_history_webhook__"
		receivers[i] = r
	}
	if len(receivers) == 0 {
		return nil, nil
	}
	alerts := map[string]*template.Alert{}
	dependencies := map[string][]string{}
	for _, target := range primary {
		var frozen FrozenNotification
		if err := json.Unmarshal(target.Payload, &frozen); err != nil {
			return nil, err
		}
		for _, alert := range frozen.Data.Alerts {
			alerts[alert.ID] = alert.Clone()
			dependencies[alert.ID] = append(dependencies[alert.ID], target.ContentRevision)
		}
	}
	var result []spool.Target
	for id, alert := range alerts {
		alert.NotifySuccessful = true
		if alert.NotificationTime.IsZero() {
			alert.NotificationTime = time.Now().UTC()
		}
		groups := map[internal.Receiver][]*template.Data{}
		for _, receiver := range receivers {
			groups[receiver] = []*template.Data{{Alerts: template.Alerts{alert}}}
		}
		history, err := Freeze(logger, ctl, groups)
		if err != nil {
			return nil, err
		}
		for i := range history {
			history[i].DependencyRevisions = dependencies[id]
		}
		result = append(result, history...)
	}
	return result, nil
}
