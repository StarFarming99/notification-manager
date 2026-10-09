package v1

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/kubesphere/notification-manager/pkg/internal"
	"github.com/kubesphere/notification-manager/pkg/notify"
	"github.com/kubesphere/notification-manager/pkg/template"
	"io"
	"net/http"
	"sort"
	"time"

	"github.com/kubesphere/notification-manager/pkg/constants"
	"github.com/kubesphere/notification-manager/pkg/deliveryprofiles"
	feishutype "github.com/kubesphere/notification-manager/pkg/internal/feishu"
	spool "github.com/kubesphere/notification-manager/pkg/notification_spool"
)

// PrepareInitialProfiles performs the first activation only after the actual
// selected Receiver, credentials and original template are usable. Persisted
// pauses/activations are retained across later starts.
func (h *HttpHandler) PrepareInitialProfiles(ctx context.Context) error {
	if h.notifierCtl.DeliveryProfiles == nil {
		return nil
	}
	if shadow := h.notifierCtl.GetJevShadow(); shadow != nil && shadow.Enabled() {
		registered, err := h.alerts.Durable.RegisteredProfiles()
		if err != nil {
			return err
		}
		for _, p := range registered {
			if err := shadow.ValidateProfileOwner(p.ID, p.Version, p.CardOwnerID); err != nil {
				return err
			}
		}
	}
	status, err := h.alerts.Durable.ProfileStatus()
	if err != nil {
		return err
	}
	test, ok := status["test"].(map[string]interface{})
	if !ok {
		return errors.New("test profile state missing")
	}
	state := test["state"].(spool.ProfileState)
	if state.BootstrapVersion == "" {
		return nil
	}
	profile, err := h.alerts.Durable.ProfileVersion("test", state.BootstrapVersion)
	if err != nil {
		return err
	}
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	for {
		err = h.validateProfile(profile)
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return errors.New("initial profile compatibility validation failed")
		case <-time.After(200 * time.Millisecond):
		}
	}
	state, err = h.alerts.Durable.ControlProfile("test", "prepare", profile.Version, state.Revision, "startup", "verified original scoped receiver/template/credentials")
	if err != nil {
		return err
	}
	_, err = h.alerts.Durable.ControlProfile("test", "activate", profile.Version, state.Revision, "startup", "initial scoped test admission")
	return err
}

// TestAlert fails closed when the trusted Receiver scope drifts after bootstrap.
// Original pipeline filtering still decides which alerts belong to that scope.
func (h *HttpHandler) TestAlert(w http.ResponseWriter, r *http.Request) {
	snap, ok := deliveryprofiles.Snapshot(r.Context())
	if !ok || snap.ID != "test" {
		http.Error(w, "server-bound test profile required", 403)
		return
	}
	if err := h.validateTestScope(snap.DeliveryProfile); err != nil {
		http.Error(w, err.Error(), 503)
		return
	}
	h.Alert(w, r)
}
func (h *HttpHandler) validateTestScope(p spool.DeliveryProfile) error {
	receivers := h.notifierCtl.RcvsFromName([]string{p.Receiver}, "", constants.Feishu)
	if len(receivers) != 1 {
		return errors.New("scoped test receiver is unavailable or ambiguous")
	}
	f, ok := receivers[0].(*feishutype.Receiver)
	if !ok || f.AlertSelector == nil {
		return errors.New("scoped receiver must select critical alerts")
	}
	critical, err := f.AlertSelector.Matches(map[string]string{"severity": "critical"})
	if err != nil || !critical {
		return errors.New("scoped critical selector drifted")
	}
	warning, _ := f.AlertSelector.Matches(map[string]string{"severity": "warning"})
	missing, _ := f.AlertSelector.Matches(map[string]string{})
	if warning || missing {
		return errors.New("scoped receiver now admits non-critical alerts")
	}
	matches := 0
	for _, chat := range f.ChatIDs {
		if chat == p.SourceChatID {
			matches++
		}
	}
	if matches != 1 {
		return errors.New("scoped production destination drifted")
	}
	return nil
}

func (h *HttpHandler) ProfileStatus(w http.ResponseWriter, _ *http.Request) {
	status, err := h.alerts.Durable.ProfileStatus()
	if err != nil {
		http.Error(w, "profile state unavailable", 503)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(status)
}

// Formal readiness is a separate promotion contract. Test readiness alone
// must never authorize routing the shared production Service to this sender.
func (h *HttpHandler) ServeFormalReadiness(w http.ResponseWriter, _ *http.Request) {
	profiles := h.notifierCtl.DeliveryProfiles
	if profiles == nil || h.alerts.Durable == nil || !h.alerts.Accepting() {
		http.Error(w, "formal unavailable", 503)
		return
	}
	snap, err := profiles.Store.ProfileSnapshot("formal")
	if err != nil {
		http.Error(w, "formal profile not active", 503)
		return
	}
	if err := h.validateProfile(snap.DeliveryProfile); err != nil {
		http.Error(w, "formal configuration incompatible", 503)
		return
	}
	if err := h.alerts.Durable.Ready(); err != nil {
		http.Error(w, "formal storage unavailable", 503)
		return
	}
	w.WriteHeader(http.StatusOK)
}
func (h *HttpHandler) ProfileControl(w http.ResponseWriter, r *http.Request, id, action string) {
	var request struct {
		ExpectedRevision uint64 `json:"expected_revision"`
		Version          string `json:"version"`
		Actor            string `json:"actor"`
		Evidence         string `json:"evidence"`
	}
	dec := json.NewDecoder(io.LimitReader(r.Body, 16<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&request); err != nil {
		http.Error(w, "invalid control request", 400)
		return
	}
	if err := dec.Decode(new(interface{})); err != io.EOF {
		http.Error(w, "one control object required", 400)
		return
	}
	if action == "prepare" {
		p, err := h.alerts.Durable.ProfileVersion(id, request.Version)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if err := h.validateProfile(p); err != nil {
			http.Error(w, err.Error(), 409)
			return
		}
	}
	result, err := h.alerts.Durable.ControlProfile(id, action, request.Version, request.ExpectedRevision, request.Actor, request.Evidence)
	if err != nil {
		status := 400
		if errors.Is(err, spool.ErrProfileConflict) {
			status = 409
		}
		http.Error(w, err.Error(), status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

func (h *HttpHandler) validateProfile(p spool.DeliveryProfile) error {
	receivers := h.notifierCtl.RcvsFromName(nil, ".*", "")
	if p.ID == "test" {
		if shadow := h.notifierCtl.GetJevShadow(); shadow != nil && shadow.Enabled() {
			if err := shadow.ValidateProfileBinding(p.ID, p.Version, p.CardOwnerID, p.Receiver, p.TestChatID); err != nil {
				return err
			}
		}
		receivers = h.notifierCtl.RcvsFromName([]string{p.Receiver}, "", constants.Feishu)
	}
	if len(receivers) == 0 {
		return errors.New("no enabled receivers available for this profile")
	}
	matched := 0
	for _, r := range receivers {
		if r.GetType() != constants.Feishu && r.GetType() != constants.Webhook {
			return errors.New("formal preparation blocked: channel lacks durable adapter")
		}
		if err := r.Validate(); err != nil {
			return err
		}
		if f, ok := r.(*feishutype.Receiver); ok {
			if f.Config == nil {
				return errors.New("feishu credentials unavailable")
			}
			appID, err := h.notifierCtl.GetCredential(f.AppID)
			if err != nil {
				return errors.New("feishu app unavailable")
			}
			if shadow := h.notifierCtl.GetJevShadow(); shadow != nil && shadow.Enabled() {
				if err := shadow.ValidateProfileSender(p.ID, p.Version, appID); err != nil {
					return err
				}
				if p.ID == "formal" {
					for _, chat := range f.ChatIDs {
						if err := shadow.ValidateProfileBinding(p.ID, p.Version, p.CardOwnerID, r.GetName(), chat); err != nil {
							return err
						}
					}
				}
			}
			if _, err := h.notifierCtl.GetCredential(f.AppSecret); err != nil {
				return errors.New("feishu credential unavailable")
			}
			for _, chat := range f.ChatIDs {
				if chat == p.SourceChatID {
					matched++
				}
			}
			if p.ID == "test" {
				if f.AlertSelector == nil {
					return errors.New("scoped receiver must select critical alerts")
				}
				critical, err := f.AlertSelector.Matches(map[string]string{"severity": "critical"})
				if err != nil || !critical {
					return errors.New("scoped critical receiver selector is incompatible")
				}
				warning, _ := f.AlertSelector.Matches(map[string]string{"severity": "warning"})
				missing, _ := f.AlertSelector.Matches(map[string]string{})
				if warning || missing {
					return errors.New("scoped receiver selector also admits non-critical alerts")
				}
			}
		}
	}
	now := time.Now().UTC()
	groups := map[internal.Receiver][]*template.Data{}
	for _, r := range receivers {
		groups[r] = []*template.Data{(&template.Data{Alerts: template.Alerts{{ID: "profile-compatibility", Labels: template.KV{"alertname": "ProfilePreparation", "severity": "critical", "cluster": h.notifierCtl.GetCluster(), "namespace": "profile-compatibility"}, Annotations: template.KV{"description": "profile compatibility rendering", "summary": "profile compatibility rendering"}, StartsAt: now, EndsAt: now.Add(time.Hour)}}}).Format()}
	}
	if p.ID == "test" {
		groups = notify.ApplyProfile(groups, spool.ProfileSnapshot{DeliveryProfile: p})
	}
	if _, err := notify.Freeze(h.logger, h.notifierCtl, groups); err != nil {
		return errors.New("original receiver template/frozen credential compatibility unavailable")
	}
	if p.ID == "test" && matched != 1 {
		return errors.New("test profile must resolve exactly one logical receiver/source chat")
	}
	return nil
}

// CanonicalAMIntent ignores peer externalURL and firing's sliding EndsAt.
// Resolved time, original alert labels/annotations, transport receiver and
// group key stay in identity; a changed update is a new delivery intent.
func CanonicalAMIntent(raw []byte) (string, error) {
	var data struct {
		Receiver string                   `json:"receiver"`
		GroupKey string                   `json:"groupKey"`
		Alerts   []map[string]interface{} `json:"alerts"`
	}
	if err := json.Unmarshal(raw, &data); err != nil {
		return "", err
	}
	rows := make([]string, 0, len(data.Alerts))
	for _, a := range data.Alerts {
		status, _ := a["status"].(string)
		row := map[string]interface{}{"status": status, "labels": a["labels"], "annotations": a["annotations"], "startsAt": a["startsAt"]}
		if status == "resolved" {
			row["endsAt"] = a["endsAt"]
		}
		encoded, err := json.Marshal(row)
		if err != nil {
			return "", err
		}
		rows = append(rows, string(encoded))
	}
	sort.Strings(rows)
	encoded, err := json.Marshal(map[string]interface{}{"receiver": data.Receiver, "group_key": data.GroupKey, "alerts": rows})
	if err != nil {
		return "", err
	}
	return spool.Hash(encoded), nil
}
