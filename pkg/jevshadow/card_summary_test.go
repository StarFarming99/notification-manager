package jevshadow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/kubesphere/notification-manager/pkg/template"
)

func groupedFixture(n int) (map[string]interface{}, *cardMemberScope, map[string]AnnotationComponent) {
	base := persistentCardBase()
	base["elements"] = append(base["elements"].([]interface{}), map[string]interface{}{"tag": "action", "actions": []interface{}{
		map[string]interface{}{"tag": "button", "value": map[string]interface{}{"action": "ack"}},
		map[string]interface{}{"tag": "button", "value": map[string]interface{}{"action": "group"}},
		map[string]interface{}{"tag": "button", "value": map[string]interface{}{"action": "silence"}},
	}})
	scope := &cardMemberScope{PermissionDomain: "infra-us", ExpectedCardBindingID: "cb_grouped_fixture", Complete: true}
	all := map[string]AnnotationComponent{}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("occ2_%048d", i)
		scope.Members = append(scope.Members, cardScopeMember{EventID: id, EpisodeID: fmt.Sprintf("ep-%d", i), Fingerprint: fmt.Sprintf("fp-%d", i), StartsAt: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), Status: "firing"})
		c := validV2Component()
		c.MemberEventIDs = []string{id}
		c.AnnotationRevision = i + 1
		link := "https://aiops.example/apps/alert-center/cards/cb_grouped_fixture?domain=infra-us"
		c.TraceURL = &link
		all[stableComponentKey(c.MemberEventIDs)] = c
	}
	return base, scope, all
}
func summaryMarkdown(t *testing.T, card map[string]interface{}) string {
	t.Helper()
	elements := card["elements"].([]interface{})
	count := 0
	content := ""
	for _, item := range elements {
		element := item.(map[string]interface{})
		if text, ok := element["text"].(map[string]interface{}); ok {
			value, _ := text["content"].(string)
			if strings.Contains(value, "**Jev：") {
				count++
				content = value
			}
		}
	}
	if count != 1 {
		t.Fatalf("summary count %d: %#v", count, card)
	}
	if strings.Contains(content, "Member:") || strings.Contains(content, "Members:") {
		t.Fatalf("member blocks leaked: %s", content)
	}
	return content
}

func TestGroupedCardFourThenFourteenUseOnePolicySummary(t *testing.T) {
	base, scope, all := groupedFixture(14)
	original, _ := json.Marshal(base)
	annotations := map[string]AnnotationComponent{}
	for i, member := range scope.Members {
		c := all[stableComponentKey([]string{member.EventID})]
		if i < 4 {
			c.Routing = &ChoiceAnswer{Choice: "pool", Probabilities: map[string]float64{"pool": .55, "oncall": .45}}
			c.PolicyProposal = "oncall"
			c.PolicyReasonCode = "pool_probability_below_threshold"
			c.PoolMinProbability = floatPointer(.9)
		}
		annotations[stableComponentKey(c.MemberEventIDs)] = c
		rendered, err := renderCardSummary(newCardRenderSnapshot(base, annotations, scope))
		if err != nil {
			t.Fatal(err)
		}
		text := summaryMarkdown(t, rendered)
		if !strings.Contains(text, "建议人工处理") {
			t.Fatal(text)
		}
		if i == 3 && (!strings.Contains(text, "本卡14条告警 · 已收到4条判断 · 其余10条尚无有效结果") || !strings.Contains(text, "人工处理4条 / 建议入池0条")) {
			t.Fatal(text)
		}
		if i == 13 && (!strings.Contains(text, "已收到14条判断") || !strings.Contains(text, "人工处理4条 / 建议入池10条")) {
			t.Fatal(text)
		}
		elements := rendered["elements"].([]interface{})
		if len(elements) != 5 || !reflect.DeepEqual(elements[:2], newCardRenderSnapshot(base, nil, nil).BaseCard["elements"]) {
			t.Fatalf("base/buttons changed %#v", elements)
		}
		raw, _ := json.Marshal(rendered)
		if bytes.Count(raw, []byte("查看本卡全部判断")) != 1 || bytes.Count(raw, []byte("\"url\"")) != 1 {
			t.Fatal("detail link is not unique")
		}
	}
	after, _ := json.Marshal(base)
	if !bytes.Equal(original, after) {
		t.Fatal("base mutated")
	}
}

func TestGroupedCardNeverUsesLastPoolOrModelArgmax(t *testing.T) {
	base, scope, all := groupedFixture(14)
	first := stableComponentKey([]string{scope.Members[0].EventID})
	c := all[first]
	c.Routing = &ChoiceAnswer{Choice: "pool", Probabilities: map[string]float64{"pool": .55, "oncall": .45}}
	c.PolicyProposal = "oncall"
	c.PolicyReasonCode = "pool_probability_below_threshold"
	c.PoolMinProbability = floatPointer(.9)
	all[first] = c
	snapshot := newCardRenderSnapshot(base, all, scope)
	counts := summarizeCard(snapshot)
	if counts.Oncall != 1 || counts.Pool != 13 || counts.Missing != 0 {
		t.Fatalf("bad counts %+v", counts)
	}
	card, _ := renderCardSummary(snapshot)
	text := summaryMarkdown(t, card)
	if !strings.Contains(text, "建议人工处理") || strings.Contains(text, "覆盖完整") || strings.Contains(text, "Pool 55%") {
		t.Fatal(text)
	}
}

func TestGroupedCardMissingProtectedFailedConflictingAndLegacyScope(t *testing.T) {
	tests := []struct {
		name   string
		change func(*cardMemberScope, map[string]AnnotationComponent)
	}{
		{"missing", func(s *cardMemberScope, a map[string]AnnotationComponent) {
			delete(a, stableComponentKey([]string{s.Members[1].EventID}))
		}},
		{"protected", func(s *cardMemberScope, a map[string]AnnotationComponent) {
			key := stableComponentKey([]string{s.Members[1].EventID})
			c := a[key]
			c.PolicyProposal = "original"
			c.PolicyReasonCode = "protected_scope"
			c.Routing = nil
			c.PoolReason = nil
			a[key] = c
		}},
		{"failed", func(s *cardMemberScope, a map[string]AnnotationComponent) {
			key := stableComponentKey([]string{s.Members[1].EventID})
			c := a[key]
			c.PolicyProposal = "original"
			c.Routing = nil
			c.PoolReason = nil
			c.PolicyReasonCode = "model_error"
			a[key] = c
		}},
		{"overlap", func(s *cardMemberScope, a map[string]AnnotationComponent) {
			c := validV2Component()
			c.MemberEventIDs = []string{s.Members[0].EventID, s.Members[1].EventID}
			a[stableComponentKey(c.MemberEventIDs)] = c
		}},
		{"outside-scope", func(s *cardMemberScope, a map[string]AnnotationComponent) {
			c := validV2Component()
			c.MemberEventIDs = []string{s.Members[0].EventID, "another-event"}
			a[stableComponentKey(c.MemberEventIDs)] = c
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base, scope, all := groupedFixture(2)
			test.change(scope, all)
			card, _ := renderCardSummary(newCardRenderSnapshot(base, all, scope))
			if text := summaryMarkdown(t, card); strings.Contains(text, "覆盖完整") {
				t.Fatal(text)
			}
		})
	}
	base, _, all := groupedFixture(2)
	card, _ := renderCardSummary(newCardRenderSnapshot(base, all, nil))
	text := summaryMarkdown(t, card)
	if !strings.Contains(text, "总范围未知") || strings.Contains(text, "本卡2条") || strings.Contains(text, "覆盖完整") {
		t.Fatal(text)
	}
}

func TestGroupedCardResolvedLifecycleDoesNotReuseFiringOrOtherEpisode(t *testing.T) {
	base, scope, all := groupedFixture(2)
	// Exact resolved scope is authoritative; matching or old firing results are ignored.
	scope.Members[0].Status = "resolved"
	scope.Members[1].StartsAt = scope.Members[1].StartsAt.Add(time.Hour)
	scope.Members[1].EventID = "new-occurrence"
	scope.Members[1].EpisodeID = "new-episode"
	card, _ := renderCardSummary(newCardRenderSnapshot(base, all, scope))
	text := summaryMarkdown(t, card)
	if !strings.Contains(text, "已恢复1条") || !strings.Contains(text, "尚无有效结果") || strings.Contains(text, "覆盖完整") {
		t.Fatal(text)
	}
	scope.Members[1].Status = "resolved"
	card, _ = renderCardSummary(newCardRenderSnapshot(base, all, scope))
	text = summaryMarkdown(t, card)
	if !strings.Contains(text, "本卡告警已恢复") || strings.Contains(text, "Pool 92%") {
		t.Fatal(text)
	}
	// Conflicting firing/resolved observations in a receipt share an episode.
	scope.Members[1].Status = "firing"
	scope.Members[1].EpisodeID = scope.Members[0].EpisodeID
	counts := summarizeCard(newCardRenderSnapshot(base, all, scope))
	if counts.Firing != 0 || counts.Resolved != 2 || counts.Received != 0 {
		t.Fatalf("terminal regressed %+v", counts)
	}
}

func TestGroupedCardLargeReceiptDoesNotChangeComponentMemberLimit(t *testing.T) {
	base, scope, all := groupedFixture(1001)
	selected := map[string]AnnotationComponent{}
	for _, member := range scope.Members[:4] {
		key := stableComponentKey([]string{member.EventID})
		selected[key] = all[key]
	}
	card, _ := renderCardSummary(newCardRenderSnapshot(base, selected, scope))
	text := summaryMarkdown(t, card)
	if !strings.Contains(text, "本卡1001条告警") || !strings.Contains(text, "其余997条尚无有效结果") || len(card["elements"].([]interface{})) != 5 {
		t.Fatal(text)
	}
	c := validV2Component()
	c.MemberEventIDs = nil
	for _, member := range scope.Members[:101] {
		c.MemberEventIDs = append(c.MemberEventIDs, member.EventID)
	}
	if c.validate() == nil {
		t.Fatal("101-member annotation accepted")
	}
	c.MemberEventIDs = c.MemberEventIDs[:100]
	if err := c.validate(); err != nil {
		t.Fatal(err)
	}
}

func TestGroupedCardDetailNeverMislabelsMemberTraceAsCard(t *testing.T) {
	base, scope, all := groupedFixture(2)
	for key, c := range all {
		trace := "https://aiops.example/apps/alert-center/traces/member?domain=infra-us"
		c.TraceURL = &trace
		all[key] = c
	}
	card, _ := renderCardSummary(newCardRenderSnapshot(base, all, scope))
	raw, _ := json.Marshal(card)
	if bytes.Contains(raw, []byte("查看本卡全部判断")) || bytes.Contains(raw, []byte("/traces/")) {
		t.Fatalf("trace mislabeled %s", raw)
	}
	for key, c := range all {
		link := "https://aiops.example/apps/alert-center/cards/cb_other?domain=infra-eu"
		c.TraceURL = &link
		all[key] = c
	}
	if cardDetailURL(all, scope, scope.ExpectedCardBindingID) != "" {
		t.Fatal("cross-domain detail accepted")
	}
}

func groupedRuntimeConfig(url, root string) Config {
	cfg := persistentCardTestConfig(url, root)
	cfg.Environment = "production"
	cfg.annotationProfileEnabled = true
	cfg.PermissionDomain = "infra-us"
	cfg.AnnotationToken = strings.Repeat("a", 32)
	cfg.CardOwnerID = "test-owner"
	cfg.ExecutionDomain = "test-exec"
	cfg.ObservationURL = url
	return cfg
}
func groupedRuntimeData(n int) *template.Data {
	data := persistentCardTestData()
	data.Alerts = nil
	for i := 0; i < n; i++ {
		data.Alerts = append(data.Alerts, &template.Alert{Status: "firing", Labels: template.KV{"alertname": "KubePodNotReady", "namespace": "fixture", "pod": fmt.Sprintf("pod-%02d", i)}, Annotations: template.KV{"summary": "fixture"}, StartsAt: time.Date(2026, 10, 15, 1, 0, 0, 0, time.UTC)})
	}
	return data
}

func TestGroupedCardHTTPReceiptAnnotationRestartAndUnknown(t *testing.T) {
	receipts := make(chan deliveryReceipt, 8)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var receipt deliveryReceipt
		_ = json.NewDecoder(r.Body).Decode(&receipt)
		if len(receipt.Members) > 0 {
			receipts <- receipt
		}
		w.WriteHeader(202)
	}))
	defer upstream.Close()
	cfg := groupedRuntimeConfig(upstream.URL, t.TempDir())
	service, err := New(log.NewNopLogger(), cfg, upstream.Client())
	if err != nil {
		t.Fatal(err)
	}
	var patches atomic.Int32
	var last map[string]interface{}
	var platformMu sync.Mutex
	platform := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PATCH" || r.URL.Path != "/messages/om-group" {
			t.Errorf("wrong platform target %s %s", r.Method, r.URL.Path)
		}
		platformMu.Lock()
		defer platformMu.Unlock()
		_ = json.NewDecoder(r.Body).Decode(&last)
		patches.Add(1)
		w.WriteHeader(200)
	}))
	defer platform.Close()
	patcher := func(ctx context.Context, mid string, card map[string]interface{}) error {
		raw, _ := json.Marshal(card)
		request, _ := http.NewRequestWithContext(ctx, "PATCH", platform.URL+"/messages/"+mid, bytes.NewReader(raw))
		response, e := platform.Client().Do(request)
		if e != nil {
			return e
		}
		response.Body.Close()
		return nil
	}
	service.SetCardPatcherResolver(func(context.Context, string, string) (CardPatcher, error) { return patcher, nil })
	mux := http.NewServeMux()
	token := strings.Repeat("r", 32)
	mux.HandleFunc("/deliveries", service.HandleSuccessfulDelivery(token))
	mux.HandleFunc("/annotation", func(w http.ResponseWriter, r *http.Request) { service.HandleAnnotation(w, r, "om-group") })
	executor := httptest.NewServer(mux)
	defer executor.Close()
	data := groupedRuntimeData(14)
	delivery := SuccessfulDelivery{Data: data, Receiver: "jev-shadow-feishu-uat", Destination: "oc_test", MessageID: "om-group", SenderApp: cfg.SenderApp, BaseCard: persistentCardBase()}
	raw, _ := json.Marshal(delivery)
	request, _ := http.NewRequest("POST", executor.URL+"/deliveries", bytes.NewReader(raw))
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := executor.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 202 {
		t.Fatalf("capture %d", response.StatusCode)
	}
	receipt := <-receipts
	if len(receipt.Members) != 14 {
		t.Fatal("receipt scope dropped")
	}
	state, err := service.cardStore.Load("om-group")
	if err != nil || state.MemberScope == nil || len(state.MemberScope.Members) != 14 {
		t.Fatalf("scope missing %v", err)
	}
	for i, member := range receipt.Members {
		c := validV2Component()
		c.MemberEventIDs = []string{member.EventID}
		c.AnnotationRevision = i + 1
		link := "https://aiops.example/apps/alert-center/cards/" + cardBindingID(cfg.SenderApp, "om-group") + "?domain=infra-us"
		c.TraceURL = &link
		if i == 0 {
			c.PolicyProposal = "oncall"
			c.PolicyReasonCode = "pool_probability_below_threshold"
			c.PoolMinProbability = floatPointer(.95)
		}
		raw, _ = json.Marshal(c)
		request, _ = http.NewRequest("PUT", executor.URL+"/annotation", bytes.NewReader(raw))
		request.Header.Set("Authorization", "Bearer "+cfg.AnnotationToken)
		request.Header.Set("Idempotency-Key", fmt.Sprintf("grouped-key-%d", i))
		response, err = executor.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != 200 {
			t.Fatalf("annotation %d status %d", i, response.StatusCode)
		}
		platformMu.Lock()
		text := summaryMarkdown(t, last)
		platformMu.Unlock()
		if !strings.Contains(text, "建议人工处理") {
			t.Fatal(text)
		}
	}
	if patches.Load() != 14 {
		t.Fatal("missing platform patches")
	}
	service.Close()
	restarted, err := New(log.NewNopLogger(), cfg, upstream.Client())
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	restarted.SetCardPatcherResolver(func(context.Context, string, string) (CardPatcher, error) {
		return func(context.Context, string, map[string]interface{}) error {
			return errors.New("network outcome unknown")
		}, nil
	})
	state, err = restarted.cardStore.Load("om-group")
	if err != nil || len(state.Annotations) != 14 {
		t.Fatalf("restart lost members %v", err)
	}
	c := state.Annotations[stableComponentKey([]string{receipt.Members[13].EventID})]
	c.AnnotationRevision = 15
	if _, err = restarted.applyAnnotation(context.Background(), "om-group", "unknown-next-revision", c); !errors.Is(err, ErrPatchStateUnknown) {
		t.Fatal(err)
	}
	unknown, _ := restarted.cardStore.Load("om-group")
	if unknown.PendingPatch == nil || unknown.PendingPatch.RendererVersion != rendererCardSummary || unknown.PendingPatch.RenderSnapshot == nil {
		t.Fatal("missing frozen intent")
	}
	// Mutating current scope/annotation inputs must not mutate the frozen intent.
	savedHash, _ := hashCanonical(unknown.PendingPatch.RenderSnapshot)
	unknown.MemberScope.Members[0].Status = "resolved"
	newHash, _ := hashCanonical(unknown.PendingPatch.RenderSnapshot)
	if savedHash != newHash {
		t.Fatal("intent aliased current scope")
	}
	if _, err = restarted.applyAnnotation(context.Background(), "om-group", "unknown-next-revision", c); !errors.Is(err, ErrPatchStateUnknown) {
		t.Fatal("unknown replayed", err)
	}
	if patches.Load() != 14 {
		t.Fatal("unknown reissued a platform request")
	}
}

func TestGroupedCardLegacyPendingHashAndUnknownSurviveUpgrade(t *testing.T) {
	for _, sendState := range []string{patchSendSending, patchSendUnknown} {
		t.Run(sendState, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(202) }))
			defer upstream.Close()
			cfg := persistentCardTestConfig(upstream.URL, t.TempDir())
			service, err := New(log.NewNopLogger(), cfg, upstream.Client())
			if err != nil {
				t.Fatal(err)
			}
			if err := service.captureCardBinding("jev-shadow-feishu-uat", "oc_test", "om-legacy", persistentCardBase(), func(context.Context, string, map[string]interface{}) error { return nil }); err != nil {
				t.Fatal(err)
			}
			state, err := service.cardStore.Load("om-legacy")
			if err != nil {
				t.Fatal(err)
			}
			state.Version = legacyCardStateVersion
			state.MemberScope = nil
			c := validV2Component()
			key := stableComponentKey(c.MemberEventIDs)
			rendered, err := renderCardLegacy(state.BaseCard, map[string]AnnotationComponent{key: c})
			if err != nil {
				t.Fatal(err)
			}
			payloadHash, _ := hashCanonical(c)
			renderHash, _ := hashCanonical(rendered)
			t.Logf("original legacy renderer card hash: %s", renderHash)
			if renderHash != "68b3260f6ac3401bdfc99575fc33f8a0082e74fe938639957a77c3a1006a76a7" {
				t.Fatal("legacy rendering bytes changed")
			}
			state.PatchSendState = sendState
			state.PendingPatch = &cardPatchIntent{IdempotencyKey: "legacy-pending-key", PayloadHash: payloadHash, RequestID: requestID("om-legacy", "legacy-pending-key"), ComponentKey: key, Component: c, RenderedCard: rendered, RenderedCardHash: renderHash, AnnotationRevision: 1, StartedAt: time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)}
			if err := service.cardStore.Save(&state); err != nil {
				t.Fatal(err)
			}
			before, _ := hashCanonical(state.PendingPatch)
			service.Close()
			restarted, err := New(log.NewNopLogger(), cfg, upstream.Client())
			if err != nil {
				t.Fatal(err)
			}
			defer restarted.Close()
			var calls atomic.Int32
			restarted.SetCardPatcherResolver(func(context.Context, string, string) (CardPatcher, error) {
				return func(context.Context, string, map[string]interface{}) error { calls.Add(1); return nil }, nil
			})
			if _, err := restarted.applyAnnotation(context.Background(), "om-legacy", "legacy-pending-key", c); !errors.Is(err, ErrPatchStateUnknown) {
				t.Fatal(err)
			}
			loaded, err := restarted.cardStore.Load("om-legacy")
			if err != nil {
				t.Fatal(err)
			}
			after, _ := hashCanonical(loaded.PendingPatch)
			if before != after || loaded.PendingPatch.RendererVersion != "" || loaded.PendingPatch.RenderedCardHash != renderHash || calls.Load() != 0 {
				t.Fatal("legacy pending was rewritten or replayed")
			}
		})
	}
}

func TestGroupedCardLegacyCompletedUnsentAndKnownFailedAcceptNextSummary(t *testing.T) {
	for _, previous := range []string{patchSendNone, patchSendFailed, patchSendConfirmed} {
		t.Run(previous, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(202) }))
			defer upstream.Close()
			cfg := persistentCardTestConfig(upstream.URL, t.TempDir())
			service, err := New(log.NewNopLogger(), cfg, upstream.Client())
			if err != nil {
				t.Fatal(err)
			}
			if err := service.captureCardBinding("jev-shadow-feishu-uat", "oc_test", "om-upgrade", persistentCardBase(), func(context.Context, string, map[string]interface{}) error { return nil }); err != nil {
				t.Fatal(err)
			}
			state, _ := service.cardStore.Load("om-upgrade")
			state.Version = legacyCardStateVersion
			state.MemberScope = nil
			state.PatchSendState = previous
			c := validV2Component()
			if previous == patchSendConfirmed {
				state.AnnotationRevision = 1
				state.Annotations[stableComponentKey(c.MemberEventIDs)] = c
				payload, _ := hashCanonical(c)
				state.AppliedKeys["old-confirmed-key"] = persistedAppliedRequest{PayloadHash: payload, RequestID: requestID("om-upgrade", "old-confirmed-key")}
				c.AnnotationRevision = 2
			}
			if err := service.cardStore.Save(&state); err != nil {
				t.Fatal(err)
			}
			service.Close()
			restarted, err := New(log.NewNopLogger(), cfg, upstream.Client())
			if err != nil {
				t.Fatal(err)
			}
			defer restarted.Close()
			var last map[string]interface{}
			restarted.SetCardPatcherResolver(func(context.Context, string, string) (CardPatcher, error) {
				return func(_ context.Context, _ string, card map[string]interface{}) error { last = card; return nil }, nil
			})
			if _, err := restarted.applyAnnotation(context.Background(), "om-upgrade", "new-summary-revision", c); err != nil {
				t.Fatal(err)
			}
			text := summaryMarkdown(t, last)
			if !strings.Contains(text, "总范围未知") {
				t.Fatal(text)
			}
			state, err = restarted.cardStore.Load("om-upgrade")
			if err != nil || state.Version != cardStateVersion || state.MemberScope != nil || len(state.Annotations) != 1 {
				t.Fatalf("unsafe legacy upgrade %v", err)
			}
		})
	}
}

func TestGroupedCardConcurrentSameMIDPreservesEveryMemberAndRejectsOldRevision(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(202) }))
	defer upstream.Close()
	cfg := groupedRuntimeConfig(upstream.URL, t.TempDir())
	service, err := New(log.NewNopLogger(), cfg, upstream.Client())
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	var mu sync.Mutex
	cards := []map[string]interface{}{}
	patcher := func(_ context.Context, _ string, card map[string]interface{}) error {
		mu.Lock()
		cards = append(cards, card)
		mu.Unlock()
		return nil
	}
	if err := service.captureDurable(groupedRuntimeData(14), "jev-shadow-feishu-uat", "oc_test", "om-concurrent", persistentCardBase(), patcher); err != nil {
		t.Fatal(err)
	}
	state, _ := service.cardStore.Load("om-concurrent")
	members := state.MemberScope.Members
	// Each worker retries only the retryable global revision gap with its exact
	// immutable component. Every successful PATCH remains a single summary.
	var wg sync.WaitGroup
	errs := make(chan error, len(members))
	for i, member := range members {
		wg.Add(1)
		go func(i int, id string) {
			defer wg.Done()
			c := validV2Component()
			c.MemberEventIDs = []string{id}
			c.AnnotationRevision = i + 1
			for attempt := 0; attempt < 100; attempt++ {
				_, err := service.applyAnnotation(context.Background(), "om-concurrent", fmt.Sprintf("concurrent-key-%d", i), c)
				if err == nil {
					return
				}
				var gap *retryableAnnotationRevisionError
				if !errors.As(err, &gap) {
					errs <- err
					return
				}
				time.Sleep(time.Millisecond)
			}
			errs <- errors.New("revision retry budget exhausted")
		}(i, member.EventID)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	state, err = service.cardStore.Load("om-concurrent")
	if err != nil || len(state.Annotations) != 14 || state.AnnotationRevision != 14 || len(cards) != 14 {
		t.Fatalf("lost concurrent members %v", err)
	}
	for _, card := range cards {
		summaryMarkdown(t, card)
	}
	stale := validV2Component()
	stale.MemberEventIDs = []string{members[0].EventID}
	stale.AnnotationRevision = 1
	if _, err := service.applyAnnotation(context.Background(), "om-concurrent", "stale-ordered-request", stale); !errors.Is(err, ErrRevision) {
		t.Fatal("old result accepted", err)
	}
	before := len(cards)
	latest := state.Annotations[stableComponentKey([]string{members[13].EventID})]
	if _, err := service.applyAnnotation(context.Background(), "om-concurrent", "concurrent-key-13", latest); err != nil || len(cards) != before {
		t.Fatal("idempotent request repatched", err)
	}
}

func TestGroupedCardOverlapCannotHideExistingOncall(t *testing.T) {
	base, scope, all := groupedFixture(2)
	key := stableComponentKey([]string{scope.Members[0].EventID})
	oncall := all[key]
	oncall.PolicyProposal = "oncall"
	oncall.PolicyReasonCode = "pool_probability_below_threshold"
	oncall.PoolMinProbability = floatPointer(.95)
	all[key] = oncall
	latePool := validV2Component()
	latePool.MemberEventIDs = []string{scope.Members[0].EventID, scope.Members[1].EventID}
	latePool.AnnotationRevision = 3
	all[stableComponentKey(latePool.MemberEventIDs)] = latePool
	snapshot := newCardRenderSnapshot(base, all, scope)
	counts := summarizeCard(snapshot)
	card, _ := renderCardSummary(snapshot)
	text := summaryMarkdown(t, card)
	if counts.Oncall != 1 || counts.Conflicts != 2 || !strings.Contains(text, "建议人工处理") || strings.Contains(text, "覆盖完整") {
		t.Fatalf("overlap hid Oncall %+v: %s", counts, text)
	}
}

func TestGroupedCardBindingIdentityMatchesJevAndRejectsSameDomainOtherCard(t *testing.T) {
	for _, vector := range []struct{ app, mid, expected string }{
		{"cli_fixture", "om_fixture_group", "card_73e2ce036f5b4304a1b9a88acb77998d"},
		{"cli_a<b", "om_你好", "card_7f1720f82c307e4c3c20a15c3ca967d8"},
	} {
		if got := cardBindingID(vector.app, vector.mid); got != vector.expected {
			t.Fatalf("cross-language binding mismatch: %s", got)
		}
	}
	const expected = "card_2d19408a905fbc14ff6f01b70d119a39"
	if got := cardBindingID("infra-alerts", "om-group"); got != expected {
		t.Fatalf("Jev binding mismatch %s", got)
	}
	base, scope, all := groupedFixture(1)
	scope.ExpectedCardBindingID = expected
	for key, c := range all {
		wrong := "https://aiops.example/apps/alert-center/cards/card_other?domain=infra-us"
		c.TraceURL = &wrong
		all[key] = c
	}
	snapshot := newCardRenderSnapshot(base, all, scope)
	if snapshot.DetailURL != "" {
		t.Fatal("same-domain unrelated card accepted")
	}
	for key, c := range all {
		correct := "https://aiops.example/apps/alert-center/cards/" + expected + "?domain=infra-us"
		c.TraceURL = &correct
		all[key] = c
	}
	snapshot = newCardRenderSnapshot(base, all, scope)
	if snapshot.DetailURL == "" {
		t.Fatal("valid bound card rejected")
	}
}

func TestGroupedCardLargeActualCaptureKeepsReceiptScopeAndMIDIsolation(t *testing.T) {
	receipts := make(chan deliveryReceipt, 4)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var receipt deliveryReceipt
		_ = json.NewDecoder(r.Body).Decode(&receipt)
		if len(receipt.Members) > 0 {
			receipts <- receipt
		}
		w.WriteHeader(202)
	}))
	defer upstream.Close()
	cfg := groupedRuntimeConfig(upstream.URL, t.TempDir())
	service, err := New(log.NewNopLogger(), cfg, upstream.Client())
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	var patched map[string]interface{}
	patcher := func(_ context.Context, _ string, card map[string]interface{}) error { patched = card; return nil }
	service.SetCardPatcherResolver(func(context.Context, string, string) (CardPatcher, error) { return patcher, nil })
	relayToken := strings.Repeat("r", 32)
	for _, mid := range []string{"om-large-first", "om-large-second"} {
		delivery := SuccessfulDelivery{Data: groupedRuntimeData(1001), Receiver: "jev-shadow-feishu-uat", Destination: "oc_test", MessageID: mid, SenderApp: cfg.SenderApp, BaseCard: persistentCardBase()}
		raw, _ := json.Marshal(delivery)
		r := httptest.NewRequest("POST", "/deliveries", bytes.NewReader(raw))
		r.Header.Set("Authorization", "Bearer "+relayToken)
		w := httptest.NewRecorder()
		service.HandleSuccessfulDelivery(relayToken)(w, r)
		if w.Code != 202 {
			t.Fatalf("large receipt capture %d", w.Code)
		}
		receipt := <-receipts
		if len(receipt.Members) != 1001 {
			t.Fatalf("large receipt lost members %d", len(receipt.Members))
		}
	}
	first, _ := service.cardStore.Load("om-large-first")
	second, _ := service.cardStore.Load("om-large-second")
	if first.MemberScope.ExpectedCardBindingID == second.MemberScope.ExpectedCardBindingID || len(first.MemberScope.Members) != 1001 || len(second.MemberScope.Members) != 1001 {
		t.Fatal("card membership identity mixed")
	}
	c := validV2Component()
	c.MemberEventIDs = []string{first.MemberScope.Members[0].EventID}
	link := "https://aiops.example/apps/alert-center/cards/" + first.MemberScope.ExpectedCardBindingID + "?domain=infra-us"
	c.TraceURL = &link
	if _, err := service.applyAnnotation(context.Background(), "om-large-first", "large-member-annotation", c); err != nil {
		t.Fatal(err)
	}
	text := summaryMarkdown(t, patched)
	if !strings.Contains(text, "本卡1001条告警") || !strings.Contains(text, "其余1000条尚无有效结果") {
		t.Fatal(text)
	}
	second, _ = service.cardStore.Load("om-large-second")
	if len(second.Annotations) != 0 || second.AnnotationRevision != 0 {
		t.Fatal("another MID inherited judgment")
	}
	// Replaying a different actual member set under the same MID/base is not a
	// license to change an immutable successful-delivery scope.
	if err := service.captureDurable(groupedRuntimeData(14), "jev-shadow-feishu-uat", "oc_test", "om-large-first", persistentCardBase(), patcher); !errors.Is(err, ErrRevision) {
		t.Fatal("scope mutation accepted", err)
	}
}
