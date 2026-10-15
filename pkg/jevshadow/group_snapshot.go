package jevshadow

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/kubesphere/notification-manager/pkg/template"
)

const (
	GroupAssessmentMode     = "group-v1"
	FinalSnapshotVersion    = "nm-final-snapshot-v1"
	MaxFinalSnapshotMembers = 1000
	MaxFinalSnapshotBytes   = 2 << 20
)

type FinalSnapshotMember struct {
	EventID         string            `json:"event_id"`
	EpisodeID       string            `json:"episode_id"`
	Fingerprint     string            `json:"fingerprint"`
	Status          string            `json:"status"`
	StartsAt        string            `json:"starts_at"`
	EndsAt          *string           `json:"ends_at"`
	Labels          map[string]string `json:"labels"`
	Annotations     map[string]string `json:"annotations"`
	GeneratorURL    string            `json:"generator_url"`
	RawEntryIndexes []int             `json:"raw_entry_indexes"`
}
type SnapshotAMInputRef struct {
	SchemaVersion string            `json:"schema_version"`
	ReferenceKind string            `json:"reference_kind"`
	RequestSHA256 string            `json:"request_sha256"`
	Receiver      string            `json:"receiver"`
	GroupKey      string            `json:"group_key"`
	GroupLabels   map[string]string `json:"group_labels"`
	ExternalURL   string            `json:"external_url"`
	EntryIndexes  []int             `json:"entry_indexes"`
}
type FinalSnapshot struct {
	SchemaVersion       string                `json:"schema_version"`
	AssessmentMode      string                `json:"assessment_mode"`
	LogicalSource       string                `json:"logical_source"`
	Environment         string                `json:"environment"`
	SourceRegion        string                `json:"source_region"`
	PermissionDomain    string                `json:"permission_domain"`
	ExecutionDomain     string                `json:"execution_domain"`
	ProfileID           string                `json:"profile_id"`
	ProfileVersion      string                `json:"profile_version"`
	CardOwnerID         string                `json:"card_owner_id"`
	Receiver            string                `json:"receiver"`
	ObservationReceiver string                `json:"observation_receiver"`
	SenderApp           string                `json:"sender_app"`
	Destination         string                `json:"destination"`
	OriginalDestination string                `json:"original_destination"`
	DispatchID          string                `json:"dispatch_id"`
	TargetID            string                `json:"target_id"`
	GroupLabels         map[string]string     `json:"group_labels"`
	LogicalGroupID      string                `json:"logical_group_id"`
	SnapshotID          string                `json:"snapshot_id,omitempty"`
	MembersHash         string                `json:"members_hash"`
	RawEntryCount       int                   `json:"raw_entry_count"`
	MemberCount         int                   `json:"member_count"`
	AMInputRefs         []SnapshotAMInputRef  `json:"am_input_refs"`
	Members             []FinalSnapshotMember `json:"members"`
}

func (s *Service) buildFinalSnapshot(data *template.Data, receiver, destination string) (*FinalSnapshot, error) {
	if data.GroupCaptureIssue != "" {
		return nil, errors.New("group capture incomplete: " + data.GroupCaptureIssue)
	}
	if data.NMDispatch == nil || data.NMDispatch.SchemaVersion != "nm-target-v1" || data.NMDispatch.AssessmentMode != GroupAssessmentMode || data.NMDispatch.DispatchID == "" || data.NMDispatch.TargetID == "" {
		return nil, errors.New("versioned accepted NM target required for group snapshot")
	}
	if len(data.Alerts) < 1 || len(data.Alerts) > MaxFinalSnapshotMembers {
		return nil, errors.New("group snapshot raw member limit exceeded; no truncation")
	}
	canonical, err := s.canonicalData(data)
	if err != nil {
		return nil, err
	}
	snapshot := &FinalSnapshot{SchemaVersion: FinalSnapshotVersion, AssessmentMode: GroupAssessmentMode, LogicalSource: s.config.LogicalSource, Environment: s.config.Environment, SourceRegion: s.config.SourceRegion, PermissionDomain: s.config.PermissionDomain, ExecutionDomain: s.config.ExecutionDomain, ProfileID: data.ProfileID, ProfileVersion: data.ProfileVersion, CardOwnerID: s.config.CardOwnerID, Receiver: receiver, ObservationReceiver: s.config.ObservationReceiver, SenderApp: s.config.SenderApp, Destination: destination, OriginalDestination: data.OriginalDestination, DispatchID: data.NMDispatch.DispatchID, TargetID: data.NMDispatch.TargetID, GroupLabels: map[string]string(data.GroupLabels.Clone()), RawEntryCount: len(data.Alerts), AMInputRefs: []SnapshotAMInputRef{}, Members: []FinalSnapshotMember{}}
	members := map[string]*FinalSnapshotMember{}
	refs := map[string]*SnapshotAMInputRef{}
	for index, alert := range canonical.Alerts {
		episode, err := episodeV2(s.config.LogicalSource, s.config.Environment, s.config.PermissionDomain, alert.ID, alert.StartsAt)
		if err != nil {
			return nil, err
		}
		eventID := occurrenceV2(episode, alert)
		member := members[eventID]
		if member == nil {
			var end *string
			if alert.Status == "resolved" && !alert.EndsAt.Before(alert.StartsAt) {
				value := pythonISOTime(alert.EndsAt)
				end = &value
			}
			member = &FinalSnapshotMember{EventID: eventID, EpisodeID: episode, Fingerprint: alert.ID, Status: alert.Status, StartsAt: pythonISOTime(alert.StartsAt), EndsAt: end, Labels: map[string]string(alert.Labels.Clone()), Annotations: map[string]string(alert.Annotations.Clone()), GeneratorURL: alert.GeneratorURL, RawEntryIndexes: []int{}}
			members[eventID] = member
		} else if member.GeneratorURL != alert.GeneratorURL {
			return nil, errors.New("same canonical event has conflicting original trigger locators")
		}
		member.RawEntryIndexes = append(member.RawEntryIndexes, index)
		for _, ref := range alert.AMInputRefs {
			encoded, err := groupCanonicalJSON(ref)
			if err != nil {
				return nil, err
			}
			key := string(encoded)
			v := refs[key]
			if v == nil {
				v = &SnapshotAMInputRef{SchemaVersion: ref.SchemaVersion, ReferenceKind: ref.ReferenceKind, RequestSHA256: ref.RequestSHA256, Receiver: ref.Receiver, GroupKey: ref.GroupKey, GroupLabels: map[string]string(ref.GroupLabels.Clone()), ExternalURL: ref.ExternalURL, EntryIndexes: []int{}}
				refs[key] = v
			}
			if len(v.EntryIndexes) == 0 || v.EntryIndexes[len(v.EntryIndexes)-1] != index {
				v.EntryIndexes = append(v.EntryIndexes, index)
			}
		}
	}
	for _, ref := range data.AMInputRefs {
		copy := ref.Clone()
		copy.EntryIndexes = nil
		encoded, err := groupCanonicalJSON(copy)
		if err != nil {
			return nil, err
		}
		key := string(encoded)
		v := refs[key]
		if v == nil {
			v = &SnapshotAMInputRef{SchemaVersion: ref.SchemaVersion, ReferenceKind: ref.ReferenceKind, RequestSHA256: ref.RequestSHA256, Receiver: ref.Receiver, GroupKey: ref.GroupKey, GroupLabels: map[string]string(ref.GroupLabels.Clone()), ExternalURL: ref.ExternalURL, EntryIndexes: []int{}}
			refs[key] = v
		}
		seen := map[int]bool{}
		for _, index := range v.EntryIndexes {
			seen[index] = true
		}
		for _, index := range ref.EntryIndexes {
			if !seen[index] {
				v.EntryIndexes = append(v.EntryIndexes, index)
				seen[index] = true
			}
		}
		sort.Ints(v.EntryIndexes)
	}
	ids := make([]string, 0, len(members))
	for id := range members {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		snapshot.Members = append(snapshot.Members, *members[id])
	}
	keys := make([]string, 0, len(refs))
	for key := range refs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		snapshot.AMInputRefs = append(snapshot.AMInputRefs, *refs[key])
	}
	snapshot.MemberCount = len(snapshot.Members)
	snapshot.MembersHash = rawSHA256([]byte(strings.Join(ids, "\n")))
	group, err := groupCanonicalJSON([]interface{}{snapshot.LogicalSource, snapshot.Environment, snapshot.PermissionDomain, snapshot.ExecutionDomain, snapshot.ProfileID, snapshot.ProfileVersion, snapshot.CardOwnerID, snapshot.Receiver, snapshot.SenderApp, snapshot.Destination, snapshot.GroupLabels})
	if err != nil {
		return nil, err
	}
	snapshot.LogicalGroupID = "ng1_" + rawSHA256(group)[:32]
	encoded, err := groupCanonicalJSON(snapshot)
	if err != nil {
		return nil, err
	}
	if len(encoded) > MaxFinalSnapshotBytes {
		return nil, errors.New("group snapshot byte budget exceeded; no truncation")
	}
	snapshot.SnapshotID = "gs1_" + rawSHA256(encoded)[:32]
	if err := validateFinalSnapshot(snapshot); err != nil {
		return nil, err
	}
	return snapshot, nil
}
func rawSHA256(raw []byte) string { digest := sha256.Sum256(raw); return hex.EncodeToString(digest[:]) }

func validateFinalSnapshot(snapshot *FinalSnapshot) error {
	if snapshot == nil || snapshot.SchemaVersion != FinalSnapshotVersion || snapshot.AssessmentMode != GroupAssessmentMode || snapshot.MemberCount < 1 || snapshot.MemberCount > MaxFinalSnapshotMembers || snapshot.MemberCount != len(snapshot.Members) || snapshot.RawEntryCount < snapshot.MemberCount || snapshot.RawEntryCount > MaxFinalSnapshotMembers {
		return errors.New("invalid final snapshot version or counts")
	}
	if snapshot.LogicalSource == "" || snapshot.Environment == "" || snapshot.SourceRegion == "" || snapshot.PermissionDomain == "" || snapshot.ExecutionDomain == "" || snapshot.ProfileID == "" || snapshot.ProfileVersion == "" || snapshot.CardOwnerID == "" || snapshot.Receiver == "" || snapshot.ObservationReceiver == "" || snapshot.SenderApp == "" || snapshot.Destination == "" || snapshot.DispatchID == "" || snapshot.TargetID == "" {
		return errors.New("final snapshot scope is incomplete")
	}
	full, fullErr := groupCanonicalJSON(snapshot)
	if fullErr != nil || len(full) > MaxFinalSnapshotBytes {
		return errors.New("final snapshot exceeds byte budget")
	}
	group, groupErr := groupCanonicalJSON([]interface{}{snapshot.LogicalSource, snapshot.Environment, snapshot.PermissionDomain, snapshot.ExecutionDomain, snapshot.ProfileID, snapshot.ProfileVersion, snapshot.CardOwnerID, snapshot.Receiver, snapshot.SenderApp, snapshot.Destination, snapshot.GroupLabels})
	if groupErr != nil || snapshot.LogicalGroupID != "ng1_"+rawSHA256(group)[:32] {
		return errors.New("logical group identity mismatch")
	}
	copy := *snapshot
	copy.SnapshotID = ""
	encoded, err := groupCanonicalJSON(copy)
	if err != nil || len(encoded) > MaxFinalSnapshotBytes || snapshot.SnapshotID != "gs1_"+rawSHA256(encoded)[:32] {
		return errors.New("final snapshot hash or byte limit mismatch")
	}
	ids := []string{}
	seen := map[string]bool{}
	indexes := map[int]bool{}
	for _, member := range snapshot.Members {
		if member.EventID == "" || member.EpisodeID == "" || member.Fingerprint == "" || member.StartsAt == "" || (member.Status != "firing" && member.Status != "resolved") || seen[member.EventID] || len(member.RawEntryIndexes) == 0 {
			return errors.New("invalid final snapshot member")
		}
		start, parseErr := time.Parse(time.RFC3339Nano, member.StartsAt)
		if parseErr != nil {
			return errors.New("invalid canonical member starts_at")
		}
		fingerprint, identityErr := fingerprintV2(snapshot.LogicalSource, snapshot.Environment, snapshot.PermissionDomain, member.Labels, nil)
		if identityErr != nil || fingerprint != member.Fingerprint {
			return errors.New("member fingerprint mismatch")
		}
		episode, identityErr := episodeV2(snapshot.LogicalSource, snapshot.Environment, snapshot.PermissionDomain, member.Fingerprint, start)
		if identityErr != nil || episode != member.EpisodeID {
			return errors.New("member episode mismatch")
		}
		alert := &template.Alert{Status: member.Status, Labels: template.KV(member.Labels), Annotations: template.KV(member.Annotations), StartsAt: start}
		if member.EndsAt != nil {
			end, err := time.Parse(time.RFC3339Nano, *member.EndsAt)
			if err != nil {
				return errors.New("invalid member ends_at")
			}
			alert.EndsAt = end
		}
		if occurrenceV2(episode, alert) != member.EventID {
			return errors.New("member occurrence mismatch")
		}
		for name, value := range snapshot.GroupLabels {
			if member.Labels[name] != value {
				return errors.New("member escaped final group labels")
			}
		}
		seen[member.EventID] = true
		ids = append(ids, member.EventID)
		for _, index := range member.RawEntryIndexes {
			if index < 0 || index >= snapshot.RawEntryCount || indexes[index] {
				return errors.New("invalid final snapshot entry mapping")
			}
			indexes[index] = true
		}
	}
	sort.Strings(ids)
	if snapshot.MembersHash != rawSHA256([]byte(strings.Join(ids, "\n"))) || len(indexes) != snapshot.RawEntryCount {
		return errors.New("final snapshot membership hash or coverage mismatch")
	}
	for _, ref := range snapshot.AMInputRefs {
		if ref.SchemaVersion != "am-input-v1" || ref.ReferenceKind != "raw_payload_sha256" || len(ref.RequestSHA256) != 64 {
			return errors.New("invalid original AM reference")
		}
		if _, err := hex.DecodeString(ref.RequestSHA256); err != nil {
			return errors.New("invalid AM payload hash")
		}
		refSeen := map[int]bool{}
		for _, index := range ref.EntryIndexes {
			if index < 0 || index >= snapshot.RawEntryCount || refSeen[index] {
				return errors.New("invalid AM entry mapping")
			}
			refSeen[index] = true
		}
	}
	return nil
}

// New group hashes use recursively sorted object keys, unlike legacy struct
// hashes which must remain byte-for-byte unchanged for pending intents.
func groupCanonicalJSON(value interface{}) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var normalized interface{}
	if err := decoder.Decode(&normalized); err != nil {
		return nil, err
	}
	encoded, err := canonicalJSON(normalized)
	if err != nil {
		return nil, err
	}
	// Python ensure_ascii=False preserves these valid UTF-8 JSON characters;
	// encoding/json intentionally escapes them even when HTML escaping is off.
	out := make([]byte, 0, len(encoded))
	for i := 0; i < len(encoded); {
		if encoded[i] == '\\' && i+1 < len(encoded) {
			if encoded[i+1] == '\\' {
				out = append(out, encoded[i:i+2]...)
				i += 2
				continue
			}
			if i+6 <= len(encoded) && (string(encoded[i:i+6]) == "\\u2028" || string(encoded[i:i+6]) == "\\u2029") {
				if encoded[i+5] == '8' {
					out = append(out, []byte("\u2028")...)
				} else {
					out = append(out, []byte("\u2029")...)
				}
				i += 6
				continue
			}
		}
		out = append(out, encoded[i])
		i++
	}
	return out, nil
}
