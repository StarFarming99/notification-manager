package jevshadow

import (
	"encoding/json"
	"os"
	"testing"
	"time"
)

func TestPythonGoCanonicalIdentityGolden(t *testing.T) {
	raw, err := os.ReadFile("testdata/canonical-identity-v2.golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures struct {
		Cases []struct {
			Name        string            `json:"name"`
			Labels      map[string]string `json:"labels"`
			Source      string            `json:"logical_source"`
			Environment string            `json:"environment"`
			Domain      string            `json:"permission_domain"`
			Excluded    []string          `json:"excluded_labels"`
			StartsAt    string            `json:"starts_at"`
			Fingerprint string            `json:"fingerprint"`
			Episode     string            `json:"episode_id"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	for _, f := range fixtures.Cases {
		excluded := map[string]struct{}{}
		for _, key := range f.Excluded {
			excluded[key] = struct{}{}
		}
		fp, err := fingerprintV2(f.Source, f.Environment, f.Domain, f.Labels, excluded)
		if err != nil || fp != f.Fingerprint {
			t.Fatalf("%s fingerprint %s %v", f.Name, fp, err)
		}
		starts, err := time.Parse(time.RFC3339Nano, f.StartsAt)
		if err != nil {
			t.Fatal(err)
		}
		ep, err := episodeV2(f.Source, f.Environment, f.Domain, fp, starts)
		if err != nil || ep != f.Episode {
			t.Fatalf("%s episode %s %v", f.Name, ep, err)
		}
	}
}
