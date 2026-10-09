package jevshadow

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const maxFeedbackResponseBytes = 64 * 1024

type CardFeedback struct {
	SourceEventID   string `json:"source_event_id"`
	ActorID         string `json:"actor_id"`
	ChatID          string `json:"chat_id"`
	MessageID       string `json:"message_id"`
	ActionReference string `json:"action_reference"`
	CorrectLabel    string `json:"correct_label"`
}

type FeedbackResult struct {
	ID        string `json:"id"`
	Duplicate bool   `json:"duplicate"`
}

func (s *Service) FeedbackEnabled() bool {
	return s != nil && !s.IsRelay() && s.config.FeedbackEnabled && s.Enabled() && ((s.config.Environment != "production" && s.config.Environment != "prod") || s.config.ExistingCallbackIntegrated)
}

// HandleCardFeedback receives identities verified by the existing unique
// callback consumer. It never subscribes to the Feishu event stream itself.
func (s *Service) HandleCardFeedback(token string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if len(token) < 32 || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if !s.FeedbackEnabled() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		var request struct {
			AppID string `json:"app_id"`
			CardFeedback
		}
		decoder := json.NewDecoder(io.LimitReader(r.Body, 16<<10))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&request); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if err := ensureEOF(decoder); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		selected := s
		if len(s.scopeServices) > 0 {
			var err error
			selected, err = s.messageScope(request.MessageID)
			if err != nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			binding, err := selected.getCardBinding(request.MessageID)
			if err != nil || binding.destination != request.ChatID {
				w.WriteHeader(http.StatusForbidden)
				return
			}
		}
		if request.AppID != selected.config.SenderApp {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if _, ok := selected.config.DestinationAllowlist[request.ChatID]; !ok {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		result, err := selected.RelayCardFeedback(ctx, request.CardFeedback)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		writeJSON(w, http.StatusAccepted, result)
	}
}

func (s *Service) FeedbackTarget() (string, string, bool) {
	if !s.FeedbackEnabled() || len(s.config.ReceiverAllowlist) != 1 || len(s.config.DestinationAllowlist) != 1 {
		return "", "", false
	}
	var receiver, destination string
	for value := range s.config.ReceiverAllowlist {
		receiver = value
	}
	for value := range s.config.DestinationAllowlist {
		destination = value
	}
	return receiver, destination, true
}

func (s *Service) RelayCardFeedback(ctx context.Context, feedback CardFeedback) (FeedbackResult, error) {
	if !s.FeedbackEnabled() {
		return FeedbackResult{}, ErrDisabled
	}
	feedback.SourceEventID = strings.TrimSpace(feedback.SourceEventID)
	feedback.ActorID = strings.TrimSpace(feedback.ActorID)
	feedback.ChatID = strings.TrimSpace(feedback.ChatID)
	feedback.MessageID = strings.TrimSpace(feedback.MessageID)
	feedback.ActionReference = strings.TrimSpace(feedback.ActionReference)
	feedback.CorrectLabel = strings.TrimSpace(feedback.CorrectLabel)
	if feedback.SourceEventID == "" || feedback.ActorID == "" || feedback.ChatID == "" ||
		feedback.MessageID == "" || feedback.ActionReference == "" {
		return FeedbackResult{}, fmt.Errorf("invalid Feishu feedback identity")
	}
	if feedback.CorrectLabel != "accurate" && feedback.CorrectLabel != "inaccurate" {
		return FeedbackResult{}, fmt.Errorf("invalid overall feedback label %q", feedback.CorrectLabel)
	}
	if _, allowed := s.config.DestinationAllowlist[feedback.ChatID]; !allowed {
		return FeedbackResult{}, fmt.Errorf("feedback chat is outside the UAT allowlist")
	}

	payload := map[string]interface{}{
		"schema_version":   "1",
		"source":           "button",
		"source_event_id":  feedback.SourceEventID,
		"actor_id":         feedback.ActorID,
		"chat_id":          feedback.ChatID,
		"message_id":       feedback.MessageID,
		"action_reference": feedback.ActionReference,
		"dimension":        "overall",
		"correct_label":    feedback.CorrectLabel,
		"reason":           "Feishu card button feedback",
		"evidence_refs":    []string{},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return FeedbackResult{}, err
	}
	feedbackURL, err := s.feedbackURL()
	if err != nil {
		return FeedbackResult{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, feedbackURL, bytes.NewReader(body))
	if err != nil {
		return FeedbackResult{}, err
	}
	token := s.config.FeedbackToken
	if token == "" {
		token = s.config.Token
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	client := *s.client
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := client.Do(req)
	if err != nil {
		return FeedbackResult{}, err
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maxFeedbackResponseBytes))
	if err != nil {
		return FeedbackResult{}, err
	}
	if response.StatusCode != http.StatusAccepted {
		return FeedbackResult{}, fmt.Errorf("Jev feedback returned %d: %s", response.StatusCode, strings.TrimSpace(string(responseBody)))
	}
	var result FeedbackResult
	if err := json.Unmarshal(responseBody, &result); err != nil {
		return FeedbackResult{}, fmt.Errorf("decode Jev feedback response: %w", err)
	}
	if result.ID == "" {
		return FeedbackResult{}, fmt.Errorf("Jev feedback response omitted id")
	}
	return result, nil
}

func (s *Service) feedbackURL() (string, error) {
	parsed, err := url.Parse(s.config.ReceiptURL)
	if err != nil {
		return "", err
	}
	parsed.Path = "/v1/feedback"
	parsed.RawPath = ""
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}
