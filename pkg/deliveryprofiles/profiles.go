// Package deliveryprofiles binds intake authentication to immutable delivery
// profiles. Payloads cannot select a lane, change activation or carry secrets.
package deliveryprofiles

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	spool "github.com/kubesphere/notification-manager/pkg/notification_spool"
)

const ProductionChat = "oc_6c65c3f33939cb742e54cd633f95d3a2"
const TestChat = "oc_fbb2c50fed6ea3bd0f0777bea6f35358"

type File struct {
	Version            int                     `json:"version"`
	Profiles           []spool.DeliveryProfile `json:"profiles"`
	InitialTestVersion string                  `json:"initial_test_version"`
	RetryDedupeWindow  string                  `json:"retry_dedupe_window"`
	RepeatInterval     string                  `json:"repeat_interval"`
}
type Manager struct {
	RecordAdmissionFailure               func(string)
	Store                                *spool.Store
	Window                               time.Duration
	RepeatInterval                       time.Duration
	SendInterval                         time.Duration
	FormalSendInterval                   time.Duration
	TestWorkers                          int
	FormalWorkers                        int
	TestActiveLimit                      int
	testToken, formalToken, controlToken string
	cutoverReadToken                     string
	InitialTestVersion                   string
}
type snapshotKey struct{}

func Snapshot(ctx context.Context) (*spool.ProfileSnapshot, bool) {
	s, ok := ctx.Value(snapshotKey{}).(spool.ProfileSnapshot)
	return &s, ok
}

func New(db *spool.Store, config File, testToken, formalToken, controlToken string, readTokens ...string) (*Manager, error) {
	if db == nil {
		return nil, errors.New("delivery profiles require the durable store")
	}
	if config.Version != 1 || len(config.Profiles) != 2 {
		return nil, errors.New("exactly two versioned delivery profiles required")
	}
	for _, token := range []string{testToken, formalToken, controlToken} {
		if len(token) < 32 {
			return nil, errors.New("independent intake/control credentials must each contain at least 32 bytes")
		}
	}
	if testToken == formalToken || testToken == controlToken || formalToken == controlToken {
		return nil, errors.New("test, formal and control credentials must differ")
	}
	readToken := ""
	if len(readTokens) > 1 {
		return nil, errors.New("at most one optional cutover read credential accepted")
	}
	if len(readTokens) == 1 {
		readToken = readTokens[0]
	}
	if readToken != "" && (len(readToken) < 32 || readToken == testToken || readToken == formalToken || readToken == controlToken) {
		return nil, errors.New("cutover read credential requires at least 32 independent bytes")
	}
	seen := map[string]bool{}
	for _, p := range config.Profiles {
		if seen[p.ID] {
			return nil, errors.New("duplicate profile identity")
		}
		seen[p.ID] = true
		if p.ID == "test" && (p.SourceChatID != ProductionChat || p.TestChatID != TestChat) {
			return nil, errors.New("test profile must mirror only the designated production chat to the designated test chat")
		}
	}
	if !seen["test"] || !seen["formal"] {
		return nil, errors.New("test and formal profiles required")
	}
	window, err := time.ParseDuration(config.RetryDedupeWindow)
	if err != nil || window <= 0 || window > 5*time.Minute {
		return nil, errors.New("positive retry dedupe window of at most five minutes required")
	}
	repeat, err := time.ParseDuration(config.RepeatInterval)
	if err != nil || repeat <= window {
		return nil, errors.New("AM repeat interval must exceed the retry dedupe window")
	}
	if err := db.RegisterProfiles(config.Profiles, config.InitialTestVersion); err != nil {
		return nil, err
	}
	if err := db.ConfigureTestCapacity(db.DefaultTestCapacity()); err != nil {
		return nil, err
	}
	return &Manager{TestWorkers: 0, FormalWorkers: 0, TestActiveLimit: db.DefaultTestCapacity(), Store: db, Window: window, RepeatInterval: repeat, SendInterval: 2 * time.Second, testToken: testToken, formalToken: formalToken, controlToken: controlToken, cutoverReadToken: readToken, InitialTestVersion: config.InitialTestVersion}, nil
}
func FromEnv(db *spool.Store) (*Manager, error) {
	path := os.Getenv("NM_DELIVERY_PROFILES_FILE")
	if path == "" {
		return nil, nil
	}
	if os.Getenv("NM_CONFIGURATION_NAME") == "" {
		return nil, errors.New("profiled instances must bind NM_CONFIGURATION_NAME")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(raw) > 64<<10 {
		return nil, errors.New("profile configuration too large")
	}
	var config File
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&config); err != nil {
		return nil, err
	}
	if err := dec.Decode(new(interface{})); err != io.EOF {
		return nil, errors.New("profile configuration must contain one JSON object")
	}
	manager, err := New(db, config, os.Getenv("NM_TEST_INTAKE_TOKEN"), os.Getenv("NM_FORMAL_INTAKE_TOKEN"), os.Getenv("NM_PROFILE_CONTROL_TOKEN"), os.Getenv("NM_CUTOVER_READ_TOKEN"))
	if err != nil {
		return nil, err
	}
	if raw := os.Getenv("NM_NOTIFICATION_SEND_INTERVAL"); raw != "" {
		interval, err := time.ParseDuration(raw)
		if err != nil || interval < 100*time.Millisecond || interval > time.Minute {
			return nil, errors.New("profiled notification send interval must be between 100ms and one minute")
		}
		manager.SendInterval = interval
	}
	if raw := os.Getenv("NM_FORMAL_NOTIFICATION_SEND_INTERVAL"); raw != "" {
		interval, err := time.ParseDuration(raw)
		if err != nil || interval < 0 || interval > time.Second {
			return nil, errors.New("formal send interval must be zero through one second")
		}
		manager.FormalSendInterval = interval
	}
	for _, setting := range []struct {
		name  string
		value *int
	}{{"NM_TEST_NOTIFICATION_WORKERS", &manager.TestWorkers}, {"NM_FORMAL_NOTIFICATION_WORKERS", &manager.FormalWorkers}, {"NM_TEST_MAX_PENDING_TARGETS", &manager.TestActiveLimit}} {
		if raw := os.Getenv(setting.name); raw != "" {
			value, err := strconv.Atoi(raw)
			if err != nil || value < 1 {
				return nil, errors.New("positive lane limit required")
			}
			*setting.value = value
		}
	}
	if manager.TestWorkers > 32 || manager.FormalWorkers > 32 {
		return nil, errors.New("lane worker limit exceeded")
	}
	if err := db.ConfigureTestCapacity(manager.TestActiveLimit); err != nil {
		return nil, err
	}
	if raw := os.Getenv("NM_FORMAL_DISABLED"); raw != "" {
		disabled, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, errors.New("invalid NM_FORMAL_DISABLED")
		}
		if disabled {
			if err := db.DisableFormal(); err != nil {
				return nil, err
			}
		}
	}
	return manager, nil
}

// Guard snapshots the server-selected lane once. Submit checks that same
// revision atomically, so a pause/activation racing with rendering cannot admit
// work after the control operation committed.
func (m *Manager) Guard(id string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := m.formalToken
		if id == "test" {
			token = m.testToken
		}
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
			http.Error(w, "unauthorized intake lane", http.StatusUnauthorized)
			return
		}
		snap, err := m.Store.ProfileSnapshot(id)
		if err != nil {
			if m.RecordAdmissionFailure != nil {
				m.RecordAdmissionFailure(id)
			}
			w.Header().Set("Retry-After", "1")
			http.Error(w, "delivery profile not accepting", http.StatusServiceUnavailable)
			return
		}
		next(w, r.WithContext(context.WithValue(r.Context(), snapshotKey{}, snap)))
	}
}
func (m *Manager) ControlGuard(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+m.controlToken)) != 1 {
			http.Error(w, "unauthorized profile control", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

// FormalCompatibility is used only behind the separate source-fenced legacy
// listener. It still snapshots prepared formal admission; payloads and headers
// cannot select test or bypass a paused profile.
func (m *Manager) FormalCompatibility(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		snapshot, err := m.Store.ProfileSnapshot("formal")
		if err != nil {
			if m.RecordAdmissionFailure != nil {
				m.RecordAdmissionFailure("formal")
			}
			http.Error(w, "formal delivery profile not accepting", 503)
			return
		}
		next(w, r.WithContext(context.WithValue(r.Context(), snapshotKey{}, snapshot)))
	}
}

// IntervalFor never lets the test budget pace the original production lane.
func (m *Manager) IntervalFor(id string) time.Duration {
	if id == "formal" {
		return m.FormalSendInterval
	}
	if id == "test" {
		return m.SendInterval
	}
	return 0
}
func (m *Manager) BudgetStatus() map[string]interface{} {
	return map[string]interface{}{"test_interval": m.SendInterval.String(), "formal_interval": m.FormalSendInterval.String(), "test_workers": m.TestWorkers, "formal_workers": m.FormalWorkers, "test_active_limit": m.TestActiveLimit, "formal_disabled": m.Store.FormalDisabled(), "clock_isolation": true, "queue_isolation": true}
}

// CutoverReadGuard is only installed on the read-only GET status resource.
// Holding its independent credential never authorizes profile mutations/intake.
func (m *Manager) CutoverReadGuard(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "cutover status is read-only", http.StatusMethodNotAllowed)
			return
		}
		authorization := []byte(r.Header.Get("Authorization"))
		control := len(m.controlToken) >= 32 && subtle.ConstantTimeCompare(authorization, []byte("Bearer "+m.controlToken)) == 1
		readonly := len(m.cutoverReadToken) >= 32 && subtle.ConstantTimeCompare(authorization, []byte("Bearer "+m.cutoverReadToken)) == 1
		if !control && !readonly {
			http.Error(w, "unauthorized cutover read", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}
