package jevshadow

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"

	"github.com/kubesphere/notification-manager/pkg/template"
)

// Tokens remain environment references; immutable public ownership is persisted
// independently of the active NM profile. Historical test scope stays installed.
type DeliveryScope struct {
	ProfileID                                    string   `json:"profile_id"`
	ProfileVersion                               string   `json:"profile_version"`
	CardOwnerID                                  string   `json:"card_owner_id"`
	ExecutionDomain                              string   `json:"execution_domain"`
	SenderApp                                    string   `json:"sender_app"`
	Receivers                                    []string `json:"receivers"`
	Destinations                                 []string `json:"destinations"`
	ReceiptTokenEnv                              string   `json:"receipt_token_env"`
	AnnotationTokenEnv                           string   `json:"annotation_token_env"`
	FeedbackTokenEnv                             string   `json:"feedback_token_env"`
	receiptToken, annotationToken, feedbackToken string
}

func scopeKey(id, version string) string { return id + ":" + version }
func loadDeliveryScopes(credentials bool) (map[string]DeliveryScope, error) {
	path := os.Getenv("JEV_DELIVERY_SCOPES_FILE")
	if path == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(raw) > 64<<10 {
		return nil, errors.New("delivery scopes too large")
	}
	var file struct {
		Version  int             `json:"version"`
		Profiles []DeliveryScope `json:"profiles"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&file); err != nil {
		return nil, err
	}
	if dec.Decode(new(interface{})) != io.EOF {
		return nil, errors.New("one delivery scope document required")
	}
	if file.Version != 1 || len(file.Profiles) < 2 || len(file.Profiles) > 16 {
		return nil, errors.New("preinstalled test/formal versioned scopes required")
	}
	result := make(map[string]DeliveryScope)
	seen := map[string]bool{}
	tokens := map[string]bool{}
	envName := regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,127}$`)
	for _, scope := range file.Profiles {
		if (scope.ProfileID != "test" && scope.ProfileID != "formal") || scope.ProfileVersion == "" || len(scope.ProfileVersion) > 128 || scope.CardOwnerID == "" || scope.ExecutionDomain == "" || scope.SenderApp == "" || len(scope.Receivers) == 0 || len(scope.Destinations) == 0 {
			return nil, errors.New("incomplete immutable scope identity")
		}
		key := scopeKey(scope.ProfileID, scope.ProfileVersion)
		if _, ok := result[key]; ok {
			return nil, errors.New("scope version cannot be rebound")
		}
		seen[scope.ProfileID] = true
		for _, name := range []string{scope.ReceiptTokenEnv, scope.AnnotationTokenEnv, scope.FeedbackTokenEnv} {
			if !envName.MatchString(name) {
				return nil, errors.New("scope credentials must be named environment references")
			}
			if credentials {
				value := os.Getenv(name)
				if len(value) < 32 || tokens[value] {
					return nil, errors.New("scope credentials must be independent and at least 32 bytes")
				}
				tokens[value] = true
			}
		}
		if credentials {
			scope.receiptToken = os.Getenv(scope.ReceiptTokenEnv)
			scope.annotationToken = os.Getenv(scope.AnnotationTokenEnv)
			scope.feedbackToken = os.Getenv(scope.FeedbackTokenEnv)
		}
		sort.Strings(scope.Receivers)
		sort.Strings(scope.Destinations)
		result[key] = scope
	}
	if !seen["test"] || !seen["formal"] {
		return nil, errors.New("both test and formal scope must stay installed")
	}
	return result, nil
}
func scopeAllows(scope DeliveryScope, receiver, destination string) bool {
	return contains(scope.Receivers, receiver) && contains(scope.Destinations, destination)
}
func contains(values []string, value string) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}

func (s *Service) ValidateProfileBinding(id, version, owner, receiver, destination string) error {
	if !s.Enabled() {
		return nil
	}
	scope, ok := s.config.DeliveryScopes[scopeKey(id, version)]
	if !ok || scope.CardOwnerID != owner || !scopeAllows(scope, receiver, destination) {
		return errors.New("frozen profile is outside preinstalled executor scope")
	}
	return nil
}

func (s *Service) ValidateProfileSender(id, version, app string) error {
	if !s.Enabled() {
		return nil
	}
	scope, ok := s.config.DeliveryScopes[scopeKey(id, version)]
	if !ok || scope.SenderApp != app {
		return errors.New("frozen profile application differs from executor scope")
	}
	return nil
}

func (s *Service) ValidateProfileOwner(id, version, owner string) error {
	if !s.Enabled() {
		return nil
	}
	scope, ok := s.config.DeliveryScopes[scopeKey(id, version)]
	if !ok || scope.CardOwnerID != owner || scope.SenderApp != s.config.SenderApp {
		return errors.New("registered historical profile lacks its original executor scope")
	}
	return nil
}

func (s *Service) IndependentExecutorCredential(token string) bool {
	if len(token) < 32 || token == s.config.Token || token == s.config.AnnotationToken || token == s.config.FeedbackToken {
		return false
	}
	for _, scope := range s.config.DeliveryScopes {
		if token == scope.receiptToken || token == scope.annotationToken || token == scope.feedbackToken {
			return false
		}
	}
	return true
}
func (s *Service) scopeService(data *template.Data) (*Service, error) {
	if data == nil {
		return nil, ErrInvalidPayload
	}
	if data.ProfileID == "" {
		if len(s.config.DeliveryScopes) > 0 {
			return nil, ErrInvalidPayload
		}
		if data.ProfileVersion != "" || data.CardOwnerID != "" && data.CardOwnerID != s.config.CardOwnerID {
			return nil, ErrInvalidPayload
		}
		return s, nil
	}
	if s.config.expectedProfileID != "" {
		if data.ProfileID != s.config.expectedProfileID || data.ProfileVersion != s.config.expectedProfileVersion || data.CardOwnerID != s.config.CardOwnerID {
			return nil, ErrInvalidPayload
		}
		return s, nil
	}
	scope, ok := s.config.DeliveryScopes[scopeKey(data.ProfileID, data.ProfileVersion)]
	if !ok || scope.CardOwnerID != data.CardOwnerID {
		return nil, ErrInvalidPayload
	}
	child := s.scopeServices[scopeKey(data.ProfileID, data.ProfileVersion)]
	if child == nil {
		return nil, ErrDisabled
	}
	return child, nil
}
func (s *Service) messageScope(messageID string) (*Service, error) {
	var selected *Service
	all := []*Service{s}
	for _, child := range s.scopeServices {
		all = append(all, child)
	}
	for _, candidate := range all {
		if _, err := candidate.getCardBinding(messageID); err == nil {
			if selected != nil {
				return nil, ErrRevision
			}
			selected = candidate
		} else if !errors.Is(err, ErrCardNotFound) && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	if selected == nil {
		return nil, ErrCardNotFound
	}
	return selected, nil
}

func bindScopeDirectory(root string, scope DeliveryScope) (string, error) {
	// The key includes profile/version. Reusing that key with different owner,
	// domain, app or destinations fails rather than redirecting frozen history.
	key := framedDigest("executor-scope-v1", scope.ProfileID, scope.ProfileVersion)
	dir := filepath.Join(root, "scope-"+key[:24])
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	identity := scope
	identity.ReceiptTokenEnv = ""
	identity.AnnotationTokenEnv = ""
	identity.FeedbackTokenEnv = ""
	raw, err := json.Marshal(identity)
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, "scope-binding.json")
	if prior, err := os.ReadFile(path); err == nil {
		if !bytes.Equal(prior, raw) {
			return "", errors.New("immutable executor scope changed; historical scope must be retained")
		}
		return dir, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return "", err
	}
	if _, err = f.Write(raw); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return "", err
	}
	if closeErr != nil {
		return "", closeErr
	}
	if err := syncDirectory(dir); err != nil {
		return "", err
	}
	return dir, nil
}
