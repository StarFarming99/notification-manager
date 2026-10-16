package jevshadow

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// A lifecycle confirmation is deliberately separate from a confirmed PATCH.
// Unknown intents and visible annotation revisions remain immutable facts.
type groupLifecycleFence struct {
	Component      GroupAnnotationComponent `json:"component"`
	IdempotencyKey string                   `json:"idempotency_key"`
	PayloadHash    string                   `json:"payload_hash"`
	RequestID      string                   `json:"request_id"`
	ConfirmedAt    time.Time                `json:"confirmed_at"`
}

func latestGroupLifecycleMembers(state cardPersistentState) []GroupLifecycleMember {
	if state.GroupLifecycleFence != nil {
		return state.GroupLifecycleFence.Component.LifecycleMembers
	}
	if state.GroupComponent != nil {
		return state.GroupComponent.LifecycleMembers
	}
	return nil
}

func validateGroupLifecycleFence(state cardPersistentState) error {
	f := state.GroupLifecycleFence
	if f == nil {
		return nil
	}
	if f.Component.Kind != "lifecycle_only" || f.Component.LifecycleRevision != state.GroupLifecycleRevision || f.ConfirmedAt.IsZero() || len(f.IdempotencyKey) < 8 || len(f.IdempotencyKey) > 256 || f.RequestID != requestID(state.MessageID, f.IdempotencyKey) {
		return errors.New("invalid lifecycle fence identity")
	}
	if err := f.Component.validate(state.FinalSnapshot, state.MessageID, state.BaseCard); err != nil {
		return err
	}
	hash, err := groupPayloadHash(f.Component)
	if err != nil || hash != f.PayloadHash {
		return errors.New("invalid lifecycle fence hash")
	}
	return nil
}

func (s *Service) persistGroupLifecycleFence(state *cardPersistentState, messageID, key string, c GroupAnnotationComponent) (string, error) {
	if err := c.validate(state.FinalSnapshot, messageID, state.BaseCard); err != nil {
		return "", err
	}
	hash, err := groupPayloadHash(c)
	if err != nil {
		return "", ErrInvalidPayload
	}
	if old := state.GroupLifecycleFence; old != nil && old.IdempotencyKey == key {
		if old.PayloadHash != hash {
			return "", ErrRevision
		}
		return old.RequestID, nil
	}
	if _, used := state.AppliedKeys[key]; used {
		return "", ErrRevision
	}
	// A risk confirmation racing ahead does not invalidate a terminal fact.
	// The visible revision is only a caller observation, never a fence CAS.
	if c.ExpectedBaseRevision != state.BaseRevision || c.ExpectedAnnotationRevision > state.AnnotationRevision || c.ExpectedLifecycleRevision != state.GroupLifecycleRevision || c.LifecycleRevision <= state.GroupLifecycleRevision {
		return "", ErrRevision
	}
	resolved := map[string]bool{}
	for _, m := range latestGroupLifecycleMembers(*state) {
		if m.CurrentStatus == "resolved" {
			resolved[m.EventID] = true
		}
	}
	for _, m := range c.LifecycleMembers {
		if resolved[m.EventID] && m.CurrentStatus != "resolved" {
			return "", ErrRevision
		}
	}
	id := requestID(messageID, key)
	if err := s.cardStore.persistTerminalMembers(state.FinalSnapshot, c.LifecycleMembers, c.AsOf, "lifecycle_only"); err != nil {
		return "", err
	}
	state.GroupLifecycleRevision = c.LifecycleRevision
	state.GroupLifecycleFence = &groupLifecycleFence{Component: c, IdempotencyKey: key, PayloadHash: hash, RequestID: id, ConfirmedAt: time.Now().UTC()}
	// A separate atomic journal permits terminal persistence while the message
	// writer is in HTTP or has an unknown attempt. Never rewrite that intent.
	raw, err := json.Marshal(state.GroupLifecycleFence)
	if err != nil {
		return "", err
	}
	err = writeLifecycleFile(s.cardStore.cardFencePath(messageID), raw)
	return id, err
}

func (s *cardStateStore) cardFencePath(messageID string) string {
	return filepath.Join(s.rootDir, "lifecycles", "card-"+cardStateKey(messageID)+".json")
}

func (s *cardStateStore) effectiveGroupState(state cardPersistentState) (cardPersistentState, error) {
	raw, err := os.ReadFile(s.cardFencePath(state.MessageID))
	if errors.Is(err, os.ErrNotExist) {
		return state, nil
	}
	if err != nil {
		return state, err
	}
	var fence groupLifecycleFence
	if len(raw) > maxGroupAnnotationBytes+4096 || json.Unmarshal(raw, &fence) != nil {
		return state, errors.New("invalid lifecycle fence journal")
	}
	if fence.Component.LifecycleRevision < state.GroupLifecycleRevision {
		return state, errors.New("lifecycle fence journal regression")
	}
	state.GroupLifecycleRevision = fence.Component.LifecycleRevision
	state.GroupLifecycleFence = &fence
	if err := validateGroupLifecycleFence(state); err != nil {
		return state, err
	}
	return state, nil
}

func (s *Service) applySilentGroupLifecycle(messageID, key string, c GroupAnnotationComponent) (string, error) {
	var id string
	err := s.cardStore.withLifecycleLock(func() error {
		state, err := s.cardStore.Load(messageID)
		if err != nil {
			return err
		}
		state, err = s.cardStore.effectiveGroupState(state)
		if err != nil {
			return err
		}
		id, err = s.persistGroupLifecycleFence(&state, messageID, key, c)
		return err
	})
	return id, err
}

func writeLifecycleFile(path string, raw []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".terminal-*.tmp")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	defer tmp.Close()
	if err := tmp.Chmod(0600); err != nil {
		return err
	}
	if _, err := tmp.Write(raw); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	return syncDirectory(dir)
}

type terminalEpisodeFact struct {
	SchemaVersion     string `json:"schema_version"`
	ScopeKey          string `json:"scope_key"`
	EpisodeID         string `json:"episode_id"`
	ResolutionEventID string `json:"resolution_event_id"`
	ResolvedAt        string `json:"resolved_at"`
	ObservedAt        string `json:"observed_at"`
	Origin            string `json:"origin"`
}

func episodeScopeKey(snapshot *FinalSnapshot) string {
	// All trusted routing/owner identities participate. A new startsAt creates
	// another episode, and a different destination/profile cannot fence it.
	value, _ := groupPayloadHash([]string{snapshot.LogicalSource, snapshot.Environment, snapshot.PermissionDomain, snapshot.ExecutionDomain, snapshot.ProfileID, snapshot.ProfileVersion, snapshot.CardOwnerID, snapshot.Receiver, snapshot.SenderApp, snapshot.Destination})
	return value
}

func (s *cardStateStore) withLifecycleLock(fn func() error) error {
	return s.WithMessageLock("jev-terminal-episode-facts-v1", fn)
}
func (s *cardStateStore) terminalPath(scope, episode string) string {
	return filepath.Join(s.rootDir, "lifecycles", cardStateKey(scope+"\x00"+episode)+".json")
}

func (s *cardStateStore) readTerminal(scope, episode string) (*terminalEpisodeFact, error) {
	raw, err := os.ReadFile(s.terminalPath(scope, episode))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var f terminalEpisodeFact
	if len(raw) > 16384 || json.Unmarshal(raw, &f) != nil || f.SchemaVersion != "terminal-episode-v1" || f.ScopeKey != scope || f.EpisodeID != episode || f.ResolutionEventID == "" {
		return nil, errors.New("invalid terminal episode fact")
	}
	if _, err := parseGroupTime(f.ResolvedAt); err != nil {
		return nil, err
	}
	return &f, nil
}

func (s *cardStateStore) persistTerminalMembers(snapshot *FinalSnapshot, members []GroupLifecycleMember, asOf, origin string) error {
	scope := episodeScopeKey(snapshot)
	dir := filepath.Join(s.rootDir, "lifecycles")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	for _, m := range members {
		if m.CurrentStatus != "resolved" || m.ResolutionEventID == nil || m.ResolvedAt == nil {
			continue
		}
		old, err := s.readTerminal(scope, m.EpisodeID)
		if err != nil {
			return err
		}
		if old != nil {
			continue
		}
		f := terminalEpisodeFact{SchemaVersion: "terminal-episode-v1", ScopeKey: scope, EpisodeID: m.EpisodeID, ResolutionEventID: *m.ResolutionEventID, ResolvedAt: *m.ResolvedAt, ObservedAt: asOf, Origin: origin}
		raw, err := json.Marshal(f)
		if err != nil {
			return err
		}
		tmp, err := os.CreateTemp(dir, ".terminal-*.tmp")
		if err != nil {
			return err
		}
		name := tmp.Name()
		err = func() error {
			defer os.Remove(name)
			defer tmp.Close()
			if err := tmp.Chmod(0600); err != nil {
				return err
			}
			if _, err := tmp.Write(raw); err != nil {
				return err
			}
			if err := tmp.Sync(); err != nil {
				return err
			}
			if err := tmp.Close(); err != nil {
				return err
			}
			return os.Rename(name, s.terminalPath(scope, m.EpisodeID))
		}()
		if err != nil {
			return err
		}
	}
	return syncDirectory(dir)
}

// Called on the executor's existing asynchronous successful-receipt path;
// neither MySQL nor Jev/model availability participates in the base send.
func (s *cardStateStore) captureSnapshotTerminals(snapshot *FinalSnapshot) error {
	if snapshot == nil {
		return nil
	}
	members := []GroupLifecycleMember{}
	for _, m := range snapshot.Members {
		if m.Status == "resolved" {
			id := m.EventID
			members = append(members, GroupLifecycleMember{EventID: m.EventID, EpisodeID: m.EpisodeID, CurrentStatus: "resolved", ResolutionEventID: &id, ResolvedAt: m.EndsAt})
		}
	}
	if len(members) == 0 {
		return nil
	}
	return s.withLifecycleLock(func() error {
		return s.persistTerminalMembers(snapshot, members, time.Now().UTC().Format(time.RFC3339Nano), "trusted_nm_receipt")
	})
}

func (s *Service) ensureRiskLifecycleCurrent(state cardPersistentState, c GroupAnnotationComponent) error {
	if c.Kind == "lifecycle" {
		return ErrRevision
	} // old stored render inputs stay readable; no new visible recovery block
	if c.ActiveMemberCount < 1 {
		return ErrRevision
	}
	for _, m := range latestGroupLifecycleMembers(state) {
		if m.CurrentStatus == "resolved" {
			for _, requested := range c.LifecycleMembers {
				if requested.EventID == m.EventID && requested.CurrentStatus != "resolved" {
					return ErrRevision
				}
			}
		}
	}
	scope := episodeScopeKey(state.FinalSnapshot)
	for _, m := range c.LifecycleMembers {
		if m.CurrentStatus != "firing" {
			continue
		}
		f, err := s.cardStore.readTerminal(scope, m.EpisodeID)
		if err != nil {
			return fmt.Errorf("%w: terminal facts: %v", ErrCardWriterUnavailable, err)
		}
		if f != nil {
			return ErrRevision
		}
	}
	return nil
}
