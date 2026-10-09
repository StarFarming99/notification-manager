package jevshadow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const maxFeedbackResponseBytes = 64 * 1024

type CardFeedback struct {
	SourceEventID   string
	ActorID         string
	ChatID          string
	MessageID       string
	ActionReference string
	CorrectLabel    string
}

type FeedbackResult struct {
	ID        string `json:"id"`
	Duplicate bool   `json:"duplicate"`
}

func (s *Service) FeedbackEnabled() bool {
	return s != nil && !s.IsRelay() && s.config.FeedbackEnabled && s.Enabled()
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
	response, err := s.client.Do(req)
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
