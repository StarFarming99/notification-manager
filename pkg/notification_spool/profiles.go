package notification_spool

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

// RegisteredProfiles retains older versions required by frozen queued work.
func (s *Store) RegisteredProfiles() ([]DeliveryProfile, error) {
	var result []DeliveryProfile
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("meta")).ForEach(func(k, v []byte) error {
			if !bytes.HasPrefix(k, []byte("profile:")) {
				return nil
			}
			var p DeliveryProfile
			if err := json.Unmarshal(v, &p); err != nil {
				return err
			}
			result = append(result, p)
			return nil
		})
	})
	return result, err
}

// DeliveryProfile is immutable automatic-delivery provenance, never credentials.
type DeliveryProfile struct {
	ID           string `json:"id"`
	Version      string `json:"version"`
	CardOwnerID  string `json:"card_owner_id"`
	Receiver     string `json:"receiver,omitempty"`
	SourceChatID string `json:"source_chat_id,omitempty"`
	TestChatID   string `json:"test_chat_id,omitempty"`
}

type ProfileState struct {
	BootstrapVersion string          `json:"bootstrap_version,omitempty"`
	Revision         uint64          `json:"revision"`
	ActiveVersion    string          `json:"active_version"`
	Paused           bool            `json:"paused"`
	Prepared         map[string]bool `json:"prepared"`
}

type ProfileSnapshot struct {
	DeliveryProfile
	Revision uint64 `json:"revision"`
}

var ErrProfileConflict = errors.New("delivery profile revision conflict")
var ErrProfilePaused = errors.New("delivery profile is not accepting")

func profileKey(id, version string) []byte { return []byte("profile:" + id + ":" + version) }
func stateKey(id string) []byte            { return []byte("profile-state:" + id) }
func readProfile(tx *bolt.Tx, id, version string) (DeliveryProfile, error) {
	var p DeliveryProfile
	raw := tx.Bucket([]byte("meta")).Get(profileKey(id, version))
	if raw == nil {
		return p, errors.New("profile version is not registered")
	}
	err := json.Unmarshal(raw, &p)
	return p, err
}
func readProfileState(tx *bolt.Tx, id string) (ProfileState, error) {
	var s ProfileState
	raw := tx.Bucket([]byte("meta")).Get(stateKey(id))
	if raw == nil {
		return s, errors.New("profile is not registered")
	}
	err := json.Unmarshal(raw, &s)
	return s, err
}

// RegisterProfiles never overwrites an immutable version or resets persisted
// activation/pause state after restart. Initial admission is test-only.
func (s *Store) RegisterProfiles(profiles []DeliveryProfile, initialTestVersion string) error {
	return s.update(func(tx *bolt.Tx) error {
		meta := tx.Bucket([]byte("meta"))
		for _, p := range profiles {
			if (p.ID != "test" && p.ID != "formal") || p.Version == "" || len(p.Version) > 128 || p.CardOwnerID == "" {
				return errors.New("bounded test/formal profile identity and owner required")
			}
			if p.ID == "test" && (p.Receiver == "" || p.SourceChatID == "" || p.TestChatID == "" || p.SourceChatID == p.TestChatID) {
				return errors.New("test scope must name distinct source/test chats and logical receiver")
			}
			if p.ID == "formal" && (p.Receiver != "" || p.SourceChatID != "" || p.TestChatID != "") {
				return errors.New("formal retains all original destinations")
			}
			raw, _ := json.Marshal(p)
			if old := meta.Get(profileKey(p.ID, p.Version)); old != nil && string(old) != string(raw) {
				return errors.New("immutable profile version changed")
			}
			if err := meta.Put(profileKey(p.ID, p.Version), raw); err != nil {
				return err
			}
			if meta.Get(stateKey(p.ID)) == nil {
				st := ProfileState{Revision: 1, Paused: true, Prepared: map[string]bool{}}
				if p.ID == "test" && p.Version == initialTestVersion {
					st.BootstrapVersion = p.Version
				}
				if err := put(meta, string(stateKey(p.ID)), st); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (s *Store) ProfileSnapshot(id string) (ProfileSnapshot, error) {
	var snap ProfileSnapshot
	err := s.db.View(func(tx *bolt.Tx) error {
		st, err := readProfileState(tx, id)
		if err != nil {
			return err
		}
		if st.Paused || st.ActiveVersion == "" || !st.Prepared[st.ActiveVersion] {
			return ErrProfilePaused
		}
		p, err := readProfile(tx, id, st.ActiveVersion)
		if err != nil {
			return err
		}
		snap = ProfileSnapshot{DeliveryProfile: p, Revision: st.Revision}
		return nil
	})
	return snap, err
}
func (s *Store) ProfileVersion(id, version string) (DeliveryProfile, error) {
	var p DeliveryProfile
	err := s.db.View(func(tx *bolt.Tx) error { var err error; p, err = readProfile(tx, id, version); return err })
	return p, err
}

// ControlProfile changes admission/delivery using one persisted CAS. Prepare
// must be preceded by compatibility validation in the authenticated handler.
func (s *Store) ControlProfile(id, action, version string, expected uint64, actor, evidence string) (ProfileState, error) {
	var result ProfileState
	if actor == "" || evidence == "" || len(actor) > 128 || len(evidence) > 512 {
		return result, errors.New("bounded audit actor and evidence required")
	}
	err := s.update(func(tx *bolt.Tx) error {
		st, err := readProfileState(tx, id)
		if err != nil {
			return err
		}
		if st.Revision != expected {
			return ErrProfileConflict
		}
		switch action {
		case "prepare":
			if _, err := readProfile(tx, id, version); err != nil {
				return err
			}
			st.Prepared[version] = true
		case "activate":
			if !st.Prepared[version] {
				return errors.New("profile version was not prepared")
			}
			st.ActiveVersion = version
			st.Paused = false
			st.BootstrapVersion = ""
		case "pause":
			st.Paused = true
		case "resume":
			if st.ActiveVersion == "" || !st.Prepared[st.ActiveVersion] {
				return errors.New("profile version was not prepared")
			}
			st.Paused = false
		default:
			return errors.New("unknown profile control action")
		}
		st.Revision++
		if err := put(tx.Bucket([]byte("meta")), string(stateKey(id)), st); err != nil {
			return err
		}
		key, err := ID()
		if err != nil {
			return err
		}
		if err := put(tx.Bucket([]byte("audit")), key, map[string]interface{}{"action": "profile_" + action, "profile_id": id, "version": version, "revision": st.Revision, "actor": actor, "evidence": evidence, "at": time.Now().UTC()}); err != nil {
			return err
		}
		result = st
		return nil
	})
	return result, err
}

func (s *Store) ProfileStatus() (map[string]interface{}, error) {
	result := map[string]interface{}{}
	err := s.db.View(func(tx *bolt.Tx) error {
		for _, id := range []string{"test", "formal"} {
			st, err := readProfileState(tx, id)
			if err != nil {
				continue
			}
			counts := map[string]int{}
			if err := tx.Bucket([]byte("targets")).ForEach(func(_, raw []byte) error {
				var t Target
				if err := json.Unmarshal(raw, &t); err != nil {
					return err
				}
				if t.ProfileID == id {
					counts[t.State]++
				}
				return nil
			}); err != nil {
				return err
			}
			result[id] = map[string]interface{}{"state": st, "counts": counts}
		}
		return nil
	})
	return result, err
}

func validateProfileTarget(tx *bolt.Tx, t Target) error {
	if t.ProfileID == "" {
		return nil
	}
	p, err := readProfile(tx, t.ProfileID, t.ProfileVersion)
	if err != nil {
		return err
	}
	if t.CardOwnerID != p.CardOwnerID {
		return errors.New("frozen card owner mismatch")
	}
	if p.ID == "test" && (t.Channel != "feishu" || t.Receiver != p.Receiver || t.Destination != "chat:"+p.TestChatID || t.OriginalDestination != "chat:"+p.SourceChatID) {
		return errors.New("frozen test destination scope violated")
	}
	return nil
}
func checkProfileAdmission(tx *bolt.Tx, snap *ProfileSnapshot) error {
	if snap == nil {
		return nil
	}
	st, err := readProfileState(tx, snap.ID)
	if err != nil {
		return err
	}
	if st.Paused {
		return ErrProfilePaused
	}
	if st.Revision != snap.Revision || st.ActiveVersion != snap.Version || !st.Prepared[snap.Version] {
		return ErrProfileConflict
	}
	return nil
}
func profileDeliveryPaused(tx *bolt.Tx, t Target) (bool, error) {
	if err := validateProfileTarget(tx, t); err != nil {
		return false, fmt.Errorf("invalid frozen profile: %w", err)
	}
	if t.ProfileID == "" {
		return false, nil
	}
	st, err := readProfileState(tx, t.ProfileID)
	return st.Paused, err
}
