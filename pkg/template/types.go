package template

import (
	"encoding/json"
	"sort"
	"time"

	"github.com/kubesphere/notification-manager/pkg/utils"

	"github.com/kubesphere/notification-manager/pkg/constants"
)

var (
	labelNeedToHiden      = []string{"rule_id"}
	annotationNeedToHiden = []string{"runbook_url", "message", "summary", "summary_cn"}
)

// NMDispatchReference comes only from the accepted durable target before a
// platform send. It is not an Alertmanager or PagerDuty incident identity.
type NMDispatchReference struct {
	SchemaVersion  string `json:"schema_version"`
	AssessmentMode string `json:"assessment_mode"`
	DispatchID     string `json:"dispatch_id"`
	TargetID       string `json:"target_id"`
}

// AMInputReference preserves original AM transport locators without using them
// to route notifications or change canonical alert/deduplication identities.
type AMInputReference struct {
	EntryIndexes  []int  `json:"entry_indexes,omitempty"`
	SchemaVersion string `json:"schema_version"`
	ReferenceKind string `json:"reference_kind"`
	RequestSHA256 string `json:"request_sha256"`
	Receiver      string `json:"receiver"`
	GroupKey      string `json:"group_key"`
	GroupLabels   KV     `json:"group_labels"`
	ExternalURL   string `json:"external_url"`
}

func (r AMInputReference) Clone() AMInputReference { r.GroupLabels = r.GroupLabels.Clone(); return r }

type Data struct {
	AMInputRefs         []AMInputReference   `json:"am_input_refs,omitempty" hash:"ignore"`
	GroupCaptureIssue   string               `json:"group_capture_issue,omitempty" hash:"ignore"`
	NMDispatch          *NMDispatchReference `json:"nm_dispatch,omitempty" hash:"ignore"`
	JevCaptureExcluded  bool                 `json:"jev_capture_excluded,omitempty"`
	ProfileID           string               `json:"profile_id,omitempty"`
	ProfileVersion      string               `json:"profile_version,omitempty"`
	CardOwnerID         string               `json:"card_owner_id,omitempty"`
	OriginalDestination string               `json:"original_destination,omitempty"`
	FrozenContent       *string              `json:"-"`
	Alerts              Alerts               `json:"alerts"`
	DeliveryID          string               `json:"deliveryID,omitempty"`

	GroupLabels       KV `json:"groupLabels"`
	CommonLabels      KV `json:"commonLabels"`
	CommonAnnotations KV `json:"commonAnnotations"`
}

func (d *Data) Format() *Data {

	if len(d.Alerts) == 0 {
		return d
	}

	var (
		commonLabels      = d.Alerts[0].Labels.Clone()
		commonAnnotations = d.Alerts[0].Annotations.Clone()
	)
	for _, a := range d.Alerts[1:] {
		if len(commonLabels) == 0 && len(commonAnnotations) == 0 {
			break
		}
		for ln, lv := range commonLabels {
			if a.Labels[ln] != lv {
				delete(commonLabels, ln)
			}
		}
		for an, av := range commonAnnotations {
			if a.Annotations[an] != av {
				delete(commonAnnotations, an)
			}
		}
	}

	d.CommonLabels = make(map[string]string)
	for k, v := range commonLabels {
		if !utils.StringIsNil(v) {
			d.CommonLabels[k] = v
		}
	}

	d.CommonAnnotations = make(map[string]string)
	for k, v := range commonAnnotations {
		if !utils.StringIsNil(v) {
			d.CommonAnnotations[k] = v
		}
	}

	return d
}

func (d *Data) Status() string {
	if len(d.Alerts.Firing()) == len(d.Alerts) {
		return constants.AlertFiring
	} else if len(d.Alerts.Resolved()) == len(d.Alerts) {
		return constants.AlertResolved
	} else {
		return ""
	}
}

func (d *Data) Clone() *Data {
	nd := &Data{
		Alerts:             nil,
		JevCaptureExcluded: d.JevCaptureExcluded,
		ProfileID:          d.ProfileID, ProfileVersion: d.ProfileVersion, CardOwnerID: d.CardOwnerID, OriginalDestination: d.OriginalDestination,
		DeliveryID:        d.DeliveryID,
		FrozenContent:     d.FrozenContent,
		GroupLabels:       d.GroupLabels.Clone(),
		CommonLabels:      d.CommonLabels.Clone(),
		CommonAnnotations: d.CommonAnnotations.Clone(),
	}

	nd.GroupCaptureIssue = d.GroupCaptureIssue
	for _, ref := range d.AMInputRefs {
		nd.AMInputRefs = append(nd.AMInputRefs, ref.Clone())
	}
	if d.NMDispatch != nil {
		ref := *d.NMDispatch
		nd.NMDispatch = &ref
	}
	for _, a := range d.Alerts {
		nd.Alerts = append(nd.Alerts, a.Clone())
	}

	return nd
}

// Pair is a key/value string pair.
type Pair struct {
	Name, Value string
}

// Pairs is a list of key/value string pairs.
type Pairs []Pair

func (ps Pairs) DefaultFilter() Pairs {
	return ps.Filter(append(labelNeedToHiden, annotationNeedToHiden...)...)
}

func (ps Pairs) Filter(keys ...string) Pairs {
	for i := 0; i < len(ps); i++ {
		if utils.StringInList(ps[i].Name, keys) {
			ps = append(ps[:i], ps[i+1:]...)
			i--
		}
	}

	return ps
}

// Names returns a list of names of the pairs.
func (ps Pairs) Names() []string {
	ns := make([]string, 0, len(ps))
	for _, p := range ps {
		ns = append(ns, p.Name)
	}
	return ns
}

// Values returns a list of values of the pairs.
func (ps Pairs) Values() []string {
	vs := make([]string, 0, len(ps))
	for _, p := range ps {
		vs = append(vs, p.Value)
	}
	return vs
}

// KV is a set of key/value string pairs.
type KV map[string]string

// SortedPairs returns a sorted list of key/value pairs.
func (kv KV) SortedPairs() Pairs {
	var (
		pairs     = make([]Pair, 0, len(kv))
		keys      = make([]string, 0, len(kv))
		sortStart = 0
	)
	for k := range kv {
		if utils.StringInList(k, labelNeedToHiden) {
			continue
		}
		if k == constants.AlertName {
			keys = append([]string{k}, keys...)
			sortStart = 1
		} else {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys[sortStart:])

	for _, k := range keys {
		pairs = append(pairs, Pair{k, kv[k]})
	}
	return pairs
}

// Remove returns a copy of the key/value set without the given keys.
func (kv KV) Remove(keys []string) KV {
	keySet := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		keySet[k] = struct{}{}
	}

	res := KV{}
	for k, v := range kv {
		if _, ok := keySet[k]; !ok {
			res[k] = v
		}
	}
	return res
}

// Names returns the names of the label names in the LabelSet.
func (kv KV) Names() []string {
	return kv.SortedPairs().Names()
}

// Values returns a list of the values in the LabelSet.
func (kv KV) Values() []string {
	return kv.SortedPairs().Values()
}

func (kv KV) Clone() KV {
	m := make(map[string]string)
	for k, v := range kv {
		m[k] = v
	}
	return m
}

type Alert struct {
	AMInputRefs  []AMInputReference `json:"am_input_refs,omitempty" hash:"ignore"`
	ID           string             `json:"id"`
	Status       string             `json:"status"`
	Labels       KV                 `json:"labels"`
	Annotations  KV                 `json:"annotations"`
	GeneratorURL string             `json:"generatorURL,omitempty"`

	StartsAt time.Time `json:"startsAt,omitempty"`
	EndsAt   time.Time `json:"endsAt,omitempty"`

	NotifySuccessful bool                              `json:"-"`
	NotificationTime time.Time                         `json:"notificationTime,omitempty"`
	Receiver         map[string]map[string]interface{} `json:"receiver,omitempty"`
}

func (a *Alert) Message() string {
	if a.Annotations == nil {
		return ""
	}
	message := a.Annotations[constants.AlertMessage]
	if utils.StringIsNil(message) {
		message = a.Annotations[constants.AlertSummary]
		if utils.StringIsNil(message) {
			message = a.Annotations[constants.AlertSummaryCN]
		}
	}

	return message
}

func (a *Alert) MessageCN() string {
	if a.Annotations == nil {
		return ""
	}
	message := a.Annotations[constants.AlertSummaryCN]
	if utils.StringIsNil(message) {
		message = a.Annotations[constants.AlertMessage]
		if utils.StringIsNil(message) {
			message = a.Annotations[constants.AlertSummary]
		}
	}

	return message
}

// Alerts is a list of Alert objects.
type Alerts []*Alert

// Firing returns the subset of alerts that are firing.
func (as Alerts) Firing() []*Alert {
	var res []*Alert
	for _, a := range as {
		if a.Status == constants.AlertFiring {
			res = append(res, a)
		}
	}
	return res
}

// Resolved returns the subset of alerts that are resolved.
func (as Alerts) Resolved() []*Alert {
	var res []*Alert
	for _, a := range as {
		if a.Status == constants.AlertResolved {
			res = append(res, a)
		}
	}
	return res
}

func (a *Alert) Clone() *Alert {
	refs := make([]AMInputReference, len(a.AMInputRefs))
	for i, ref := range a.AMInputRefs {
		refs[i] = ref.Clone()
	}
	return &Alert{AMInputRefs: refs,
		ID:               a.ID,
		Status:           a.Status,
		StartsAt:         a.StartsAt,
		EndsAt:           a.EndsAt,
		NotificationTime: a.NotificationTime,
		Labels:           a.Labels.Clone(),
		Annotations:      a.Annotations.Clone(),
		GeneratorURL:     a.GeneratorURL,
		Receiver:         a.Receiver,
	}
}

// CompactAMInputReferences runs only on the frozen target's private Data copy.
// Original template members and aggregation remain unchanged; metadata is
// de-duplicated once with exact post-filter/post-aggregation entry indexes.
func (d *Data) CompactAMInputReferences() {
	refs := map[string]*AMInputReference{}
	for index, alert := range d.Alerts {
		for _, ref := range alert.AMInputRefs {
			ref.EntryIndexes = nil
			raw, _ := json.Marshal(ref)
			key := string(raw)
			v := refs[key]
			if v == nil {
				copy := ref.Clone()
				v = &copy
				refs[key] = v
			}
			if len(v.EntryIndexes) == 0 || v.EntryIndexes[len(v.EntryIndexes)-1] != index {
				v.EntryIndexes = append(v.EntryIndexes, index)
			}
		}
		alert.AMInputRefs = nil
	}
	if len(refs) == 0 {
		return
	}
	d.AMInputRefs = nil
	keys := []string{}
	for key := range refs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		d.AMInputRefs = append(d.AMInputRefs, *refs[key])
	}
}
