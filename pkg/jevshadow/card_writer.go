package jevshadow

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

type CardPatchOutcome string

const (
	CardPatchKnownFailed CardPatchOutcome = "failed"
	CardPatchUnknown     CardPatchOutcome = "unknown"
)

// CardPatchError lets the channel writer distinguish a confirmed rejection
// from a network result that might have reached Feishu. Unknown results are
// never retried by blindly replaying an older full-card snapshot.
type CardPatchError struct {
	Outcome CardPatchOutcome
	Err     error
}

func (e *CardPatchError) Error() string {
	if e == nil || e.Err == nil {
		return "card patch failed"
	}
	return e.Err.Error()
}

func (e *CardPatchError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func (s *Service) SetCardPatcherResolver(resolver CardPatcherResolver) {
	if s == nil {
		return
	}
	for _, child := range s.scopeServices {
		child.SetCardPatcherResolver(resolver)
	}
	s.mu.Lock()
	s.patcherResolver = resolver
	s.mu.Unlock()
}

func (s *Service) registerCardPatcher(receiver, destination string, patcher CardPatcher) {
	if s == nil || patcher == nil {
		return
	}
	key := cardPatcherKey(receiver, destination)
	s.mu.Lock()
	s.patchers[key] = patcher
	s.mu.Unlock()
}

func (s *Service) resolveCardPatcher(
	ctx context.Context,
	receiver string,
	destination string,
	existing CardPatcher,
) (CardPatcher, error) {
	if existing != nil {
		return existing, nil
	}
	key := cardPatcherKey(receiver, destination)
	s.mu.RLock()
	patcher := s.patchers[key]
	resolver := s.patcherResolver
	s.mu.RUnlock()
	if patcher != nil {
		return patcher, nil
	}
	if resolver == nil {
		return nil, ErrCardWriterUnavailable
	}
	patcher, err := resolver(ctx, receiver, destination)
	if err != nil || patcher == nil {
		if err == nil {
			err = errors.New("card patcher resolver returned nil")
		}
		return nil, fmt.Errorf("%w: %v", ErrCardWriterUnavailable, err)
	}
	s.registerCardPatcher(receiver, destination, patcher)
	return patcher, nil
}

func cardPatcherKey(receiver, destination string) string {
	return strings.TrimSpace(receiver) + "\x00" + strings.TrimSpace(destination)
}

func (s *Service) captureCardBinding(
	receiver string,
	destination string,
	messageID string,
	baseCard map[string]interface{},
	patcher CardPatcher,
) error {
	return s.captureCardBindingWithScope(receiver, destination, messageID, baseCard, patcher, nil)
}

func (s *Service) captureCardBindingWithScope(receiver, destination, messageID string, baseCard map[string]interface{}, patcher CardPatcher, scope *cardMemberScope) error {
	return s.captureCardBindingWithSnapshot(receiver, destination, messageID, baseCard, patcher, scope, nil)
}
func (s *Service) captureCardBindingWithSnapshot(receiver, destination, messageID string, baseCard map[string]interface{}, patcher CardPatcher, scope *cardMemberScope, snapshot *FinalSnapshot) error {
	now := time.Now().UTC()
	state := cardPersistentState{FinalSnapshot: snapshot,
		MemberScope:        scope,
		Version:            cardStateVersion,
		MessageID:          messageID,
		Receiver:           receiver,
		Destination:        destination,
		SenderApp:          s.config.SenderApp,
		BaseCard:           baseCard,
		BaseRevision:       1,
		BaseSendState:      baseSendConfirmed,
		AnnotationRevision: 0,
		Annotations:        make(map[string]AnnotationComponent),
		AppliedKeys:        make(map[string]persistedAppliedRequest),
		PatchSendState:     patchSendNone,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	if s.cardStore != nil {
		if err := s.cardStore.WithMessageLock(messageID, func() error {
			existing, err := s.cardStore.Load(messageID)
			if err == nil {
				existingHash, hashErr := hashCanonical(existing.BaseCard)
				baseHash, newHashErr := hashCanonical(baseCard)
				if hashErr != nil || newHashErr != nil || existingHash != baseHash ||
					existing.Receiver != receiver || existing.Destination != destination {
					return ErrRevision
				}
				if scope != nil && existing.MemberScope != nil {
					oldScopeHash, _ := hashCanonical(existing.MemberScope)
					newScopeHash, _ := hashCanonical(scope)
					if oldScopeHash != newScopeHash {
						return ErrRevision
					}
				}
				// Legacy bindings remain scope-unknown. A replay must not invent
				// the original receipt scope or modify an unresolved intent.
				if snapshot != nil && existing.FinalSnapshot != nil && !equalGroupSnapshot(snapshot, existing.FinalSnapshot) {
					return ErrRevision
				}
				if snapshot != nil && existing.FinalSnapshot == nil && existing.AnnotationRevision == 0 && existing.PendingPatch == nil && existing.PendingGroupPatch == nil && existing.PatchSendState != patchSendUnknown {
					// A fresh authenticated successful receipt may bind a
					// revision-0 legacy card in isolated replay/new mode. No
					// completed annotation or unknown intent is rewritten.
					existing.FinalSnapshot = snapshot
					existing.MemberScope = scope
					existing.Version = cardStateVersion
					if err := s.cardStore.Save(&existing); err != nil {
						return err
					}
				}
				state = existing
				return nil
			}
			if !errors.Is(err, ErrCardNotFound) {
				return err
			}
			return s.cardStore.Save(&state)
		}); err != nil {
			return err
		}
	}
	binding := bindingFromCardState(state, patcher)
	s.mu.Lock()
	if s.cards[messageID] != nil {
		s.mu.Unlock()
		return nil
	}
	if len(s.cards) >= s.cardBindingCacheLimit() {
		s.evictOldestCardLocked()
	}
	s.cards[messageID] = binding
	s.mu.Unlock()
	return nil
}

func (s *Service) getCardBinding(messageID string) (*cardBinding, error) {
	s.mu.RLock()
	binding := s.cards[messageID]
	s.mu.RUnlock()
	if binding != nil {
		return binding, nil
	}
	if s.cardStore == nil {
		return nil, ErrCardNotFound
	}
	state, err := s.cardStore.Load(messageID)
	if err != nil {
		return nil, err
	}
	s.mu.RLock()
	patcher := s.patchers[cardPatcherKey(state.Receiver, state.Destination)]
	s.mu.RUnlock()
	binding = bindingFromCardState(state, patcher)
	s.mu.Lock()
	if existing := s.cards[messageID]; existing != nil {
		binding = existing
	} else {
		if len(s.cards) >= s.cardBindingCacheLimit() {
			s.evictOldestCardLocked()
		}
		s.cards[messageID] = binding
	}
	s.mu.Unlock()
	return binding, nil
}

func (s *Service) applyPersistentAnnotation(
	ctx context.Context,
	messageID string,
	idempotencyKey string,
	component AnnotationComponent,
) (string, error) {
	binding, err := s.getCardBinding(messageID)
	if err != nil {
		return "", err
	}
	binding.mu.Lock()
	defer binding.mu.Unlock()
	patcher, err := s.resolveCardPatcher(ctx, binding.receiver, binding.destination, binding.patcher)
	if err != nil {
		return "", err
	}
	var resultID string
	err = s.cardStore.WithMessageLock(messageID, func() error {
		state, loadErr := s.cardStore.Load(messageID)
		if loadErr != nil {
			return loadErr
		}
		if state.FinalSnapshot != nil || state.PendingGroupPatch != nil {
			return ErrRevision
		}
		if state.SenderApp != s.config.SenderApp || state.Receiver != binding.receiver || state.Destination != binding.destination {
			return fmt.Errorf("%w: persisted card writer identity changed", ErrCardWriterUnavailable)
		}
		if state.PatchSendState == patchSendSending && state.PendingPatch != nil {
			state.PatchSendState = patchSendUnknown
			state.LastPatchError = "writer stopped before the persisted patch intent was confirmed"
			if saveErr := s.cardStore.Save(&state); saveErr != nil {
				return fmt.Errorf("%w: persist recovered unknown patch state: %v", ErrPatchStateUnknown, saveErr)
			}
			syncBindingFromCardState(binding, state, patcher)
			return ErrPatchStateUnknown
		}
		if state.PendingPatch != nil || state.PatchSendState == patchSendUnknown {
			syncBindingFromCardState(binding, state, patcher)
			return ErrPatchStateUnknown
		}
		payloadHash, hashErr := hashCanonical(component)
		if hashErr != nil {
			return fmt.Errorf("%w: %v", ErrInvalidPayload, hashErr)
		}
		if applied, ok := state.AppliedKeys[idempotencyKey]; ok {
			if applied.PayloadHash != payloadHash {
				return ErrRevision
			}
			resultID = applied.RequestID
			syncBindingFromCardState(binding, state, patcher)
			return nil
		}
		if component.ExpectedBaseRevision != state.BaseRevision {
			return ErrRevision
		}
		expectedRevision := state.AnnotationRevision + 1
		if component.AnnotationRevision > expectedRevision {
			return &retryableAnnotationRevisionError{expected: expectedRevision, received: component.AnnotationRevision}
		}
		if component.AnnotationRevision != expectedRevision {
			return ErrRevision
		}
		componentKey := stableComponentKey(component.MemberEventIDs)
		annotations := make(map[string]AnnotationComponent, len(state.Annotations)+1)
		for key, value := range state.Annotations {
			annotations[key] = value
		}
		annotations[componentKey] = component
		renderSnapshot := newCardRenderSnapshot(state.BaseCard, annotations, state.MemberScope, state.SenderApp, state.MessageID)
		rendered, renderErr := renderCardSummary(renderSnapshot)
		if renderErr != nil {
			return fmt.Errorf("%w: %v", ErrInvalidPayload, renderErr)
		}
		renderedHash, renderHashErr := hashCanonical(rendered)
		if renderHashErr != nil {
			return fmt.Errorf("%w: %v", ErrInvalidPayload, renderHashErr)
		}
		resultID = requestID(messageID, idempotencyKey)
		return s.writerGate.withAttempt(ctx, messageID, func() error {
			state.PatchSendState = patchSendSending
			state.LastPatchError = ""
			state.Version = cardStateVersion
			state.PendingPatch = &cardPatchIntent{
				RendererVersion:    rendererCardSummary,
				RenderSnapshot:     &renderSnapshot,
				IdempotencyKey:     idempotencyKey,
				PayloadHash:        payloadHash,
				RequestID:          resultID,
				ComponentKey:       componentKey,
				Component:          component,
				RenderedCard:       rendered,
				RenderedCardHash:   renderedHash,
				AnnotationRevision: component.AnnotationRevision,
				StartedAt:          time.Now().UTC(),
			}
			if saveErr := s.cardStore.Save(&state); saveErr != nil {
				return fmt.Errorf("%w: persist patch intent: %v", ErrCardWriterUnavailable, saveErr)
			}

			patchErr := patcher(ctx, messageID, rendered)
			if patchErr != nil {
				state.LastPatchError = boundedCardStateError(patchErr)
				state.PatchSendState = classifyPatchFailure(patchErr)
				if state.PatchSendState == patchSendFailed {
					state.PendingPatch = nil
				}
				if saveErr := s.cardStore.Save(&state); saveErr != nil {
					syncBindingFromCardState(binding, state, patcher)
					return fmt.Errorf("%w: persist patch failure: %v", ErrPatchStateUnknown, saveErr)
				}
				syncBindingFromCardState(binding, state, patcher)
				if state.PatchSendState == patchSendUnknown {
					return fmt.Errorf("%w: %v", ErrPatchStateUnknown, patchErr)
				}
				return patchErr
			}

			state.Annotations = annotations
			state.ConfirmedRendererVersion = rendererCardSummary
			state.AnnotationRevision = component.AnnotationRevision
			state.AppliedKeys[idempotencyKey] = persistedAppliedRequest{PayloadHash: payloadHash, RequestID: resultID}
			state.PatchSendState = patchSendConfirmed
			state.PendingPatch = nil
			state.LastPatchError = ""
			if saveErr := s.cardStore.Save(&state); saveErr != nil {
				syncBindingFromCardState(binding, state, patcher)
				return fmt.Errorf("%w: persist confirmed patch: %v", ErrPatchStateUnknown, saveErr)
			}
			syncBindingFromCardState(binding, state, patcher)
			return nil
		})
	})
	if err != nil {
		return "", err
	}
	return resultID, nil
}

func classifyPatchFailure(err error) string {
	var patchError *CardPatchError
	if errors.As(err, &patchError) && patchError.Outcome == CardPatchKnownFailed {
		return patchSendFailed
	}
	return patchSendUnknown
}

func bindingFromCardState(state cardPersistentState, patcher CardPatcher) *cardBinding {
	binding := &cardBinding{}
	syncBindingFromCardState(binding, state, patcher)
	return binding
}

func syncBindingFromCardState(binding *cardBinding, state cardPersistentState, patcher CardPatcher) {
	applied := make(map[string]appliedRequest, len(state.AppliedKeys))
	for key, value := range state.AppliedKeys {
		applied[key] = appliedRequest{payloadHash: value.PayloadHash, requestID: value.RequestID}
	}
	binding.messageID = state.MessageID
	binding.receiver = state.Receiver
	binding.destination = state.Destination
	binding.senderApp = state.SenderApp
	binding.baseCard = state.BaseCard
	binding.baseRevision = state.BaseRevision
	binding.baseSendState = state.BaseSendState
	binding.annotationRevision = state.AnnotationRevision
	binding.annotations = state.Annotations
	binding.memberScope = state.MemberScope
	if state.FinalSnapshot != nil {
		binding.memberScope = nil
	}
	binding.appliedKeys = applied
	binding.patchSendState = state.PatchSendState
	binding.pendingPatch = state.PendingPatch
	binding.lastPatchError = state.LastPatchError
	binding.patcher = patcher
	binding.createdAt = state.CreatedAt
	binding.updatedAt = state.UpdatedAt
}
