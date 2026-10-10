package jevshadow

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func conservativeComponent() AnnotationComponent {
	component := validPolicyFactsV1Component()
	component.Routing = &ChoiceAnswer{
		Choice:        "pool",
		Probabilities: map[string]float64{"pool": 0.99, "oncall": 0.01},
	}
	component.DecisionSource = "conservative_policy"
	component.PolicyProposal = "oncall"
	component.PolicyReasonCode = "critical_evidence_missing"
	component.ThresholdApplied = boolPointer(false)
	component.ThresholdSource = "not_applicable"
	component.PoolMinProbability = nil
	return component
}

func TestConservativePolicyPreservesRawModelAndExplainsEvidenceGuard(t *testing.T) {
	reasons := map[string]string{
		"pool_reason_unclear":            "Pool reason unclear",
		"pool_reason_tie":                "Pool reason tie",
		"critical_evidence_missing":      "Critical evidence missing",
		"material_risk_present":          "Material risk present",
		"recurring_episode_unverified":   "Recurring episode unverified",
		"first_notification_unconfirmed": "First notification unconfirmed",
		"incident_binding_unconfirmed":   "Incident binding unconfirmed",
	}
	for reason, label := range reasons {
		t.Run(reason, func(t *testing.T) {
			component := conservativeComponent()
			component.PolicyReasonCode = reason
			encoded, err := json.Marshal(component)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := decodeAnnotation(bytes.NewReader(encoded))
			if err != nil {
				t.Fatalf("conservative evidence guard rejected: %v", err)
			}
			if decoded.Routing.Choice != "pool" || decoded.Routing.Probabilities["pool"] != 0.99 ||
				decoded.PoolReason.Probabilities["non_urgent"] != 0.85 || decoded.ModelStatus != "available" {
				t.Fatalf("raw model facts changed: %#v", decoded)
			}
			markdown := decoded.markdown()
			for _, want := range []string{
				"**Pool 99%** · Oncall 1%", "**Non-urgent**",
				"Policy: Oncall · Conservative policy · " + label,
			} {
				if !strings.Contains(markdown, want) {
					t.Fatalf("missing %q in %s", want, markdown)
				}
			}
			if strings.Contains(markdown, "Model unavailable") || strings.Contains(markdown, "Pool threshold") {
				t.Fatalf("conservative guard misrepresented model or threshold: %s", markdown)
			}
		})
	}
}

func TestConservativePolicyRejectsInvalidOrAmbiguousFacts(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*AnnotationComponent)
	}{
		{"pool", func(c *AnnotationComponent) { c.PolicyProposal = "pool" }},
		{"original", func(c *AnnotationComponent) { c.PolicyProposal = "original" }},
		{"unavailable", func(c *AnnotationComponent) { c.ModelStatus = "unavailable" }},
		{"no_model", func(c *AnnotationComponent) { c.Routing, c.PoolReason = nil, nil }},
		{"missing_reason_distribution", func(c *AnnotationComponent) { c.PoolReason = nil }},
		{"missing_probability", func(c *AnnotationComponent) { delete(c.Routing.Probabilities, "oncall") }},
		{"threshold_applied", func(c *AnnotationComponent) { c.ThresholdApplied = boolPointer(true) }},
		{"missing_threshold_flag", func(c *AnnotationComponent) { c.ThresholdApplied = nil }},
		{"threshold_source_default", func(c *AnnotationComponent) { c.ThresholdSource = "default" }},
		{"threshold_source_override", func(c *AnnotationComponent) { c.ThresholdSource = "override" }},
		{"threshold_value", func(c *AnnotationComponent) { c.PoolMinProbability = floatPointer(0.90) }},
		{"unknown_reason", func(c *AnnotationComponent) { c.PolicyReasonCode = "future_evidence_guard" }},
		{"routing_tie", func(c *AnnotationComponent) { c.PolicyReasonCode = "routing_tie" }},
		{"threshold_reason", func(c *AnnotationComponent) { c.PolicyReasonCode = "threshold_not_met" }},
		{"operator_reason", func(c *AnnotationComponent) { c.PolicyReasonCode = "manual_override" }},
		{"missing_context_flag", func(c *AnnotationComponent) { c.ContextReduced = nil }},
		{"unknown_source", func(c *AnnotationComponent) { c.DecisionSource = "conservative_policy_v2" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			component := conservativeComponent()
			test.mutate(&component)
			encoded, err := json.Marshal(component)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodeAnnotation(bytes.NewReader(encoded)); err == nil {
				t.Fatal("invalid conservative facts accepted")
			}
		})
	}
}

func TestConservativePolicyDoesNotWeakenExistingThresholdVerification(t *testing.T) {
	component := validPolicyFactsV1Component()
	component.Routing = &ChoiceAnswer{
		Choice: "pool", Probabilities: map[string]float64{"pool": 0.99, "oncall": 0.01},
	}
	component.PolicyProposal = "oncall"
	component.PolicyReasonCode = "critical_evidence_missing"
	if err := component.validate(); err == nil {
		t.Fatal("model_policy accepted Oncall against a met Pool threshold")
	}
	component.PolicyProposal = "pool"
	if err := component.validate(); err != nil {
		t.Fatalf("unchanged consistent threshold policy rejected: %v", err)
	}
}

func TestConservativePolicyAppendsWithoutChangingOriginalCard(t *testing.T) {
	component := conservativeComponent()
	card, err := renderCard(map[string]interface{}{
		"header": map[string]interface{}{"title": "original title"},
		"elements": []interface{}{
			map[string]interface{}{"tag": "action", "name": "original callback actions"},
		},
	}, map[string]AnnotationComponent{"event-1": component})
	if err != nil {
		t.Fatal(err)
	}
	if card["header"].(map[string]interface{})["title"] != "original title" {
		t.Fatal("original header changed")
	}
	elements := card["elements"].([]interface{})
	if len(elements) != 5 || elements[0].(map[string]interface{})["name"] != "original callback actions" {
		t.Fatalf("original callback actions changed: %#v", elements)
	}
	summary := elements[2].(map[string]interface{})["text"].(map[string]interface{})["content"].(string)
	if !strings.Contains(summary, "Policy: Oncall · Conservative policy · Critical evidence missing") {
		t.Fatalf("missing conservative decision: %s", summary)
	}
}
