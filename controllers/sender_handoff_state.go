package controllers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"time"

	"github.com/kubesphere/notification-manager/apis/v2beta2"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type SenderContainerExit struct {
	Name        string      `json:"name"`
	ContainerID string      `json:"container_id"`
	ExitCode    int32       `json:"exit_code"`
	Reason      string      `json:"reason,omitempty"`
	FinishedAt  metav1.Time `json:"finished_at"`
}
type SenderPodEvidence struct {
	Name                        string                `json:"name"`
	UID                         string                `json:"uid"`
	NodeName                    string                `json:"node_name,omitempty"`
	NodeUID                     string                `json:"node_uid,omitempty"`
	TerminatedAt                *metav1.Time          `json:"terminated_at,omitempty"`
	NodeReadyVerifiedAt         *metav1.Time          `json:"node_ready_verified_at,omitempty"`
	NodeLeaseRenewedAt          *metav1.MicroTime     `json:"node_lease_renewed_at,omitempty"`
	NormalGraceSeconds          int64                 `json:"normal_grace_seconds,omitempty"`
	ContainerExits              []SenderContainerExit `json:"container_exits,omitempty"`
	ContainerIDs                map[string]string     `json:"container_ids"`
	NormalDeleteRequestedAt     *metav1.Time          `json:"normal_delete_requested_at,omitempty"`
	NormalDeleteGraceSeconds    int64                 `json:"normal_delete_grace_seconds,omitempty"`
	NormalDeleteResourceVersion string                `json:"normal_delete_resource_version,omitempty"`
	NormalDeleteResult          string                `json:"normal_delete_result,omitempty"`
	ObservedGraceSeconds        *int64                `json:"observed_grace_seconds,omitempty"`
	ManagedNormalStopIntentAt   *metav1.Time          `json:"managed_normal_stop_intent_at,omitempty"`
	ConfiguredGraceSeconds      int64                 `json:"configured_grace_seconds,omitempty"`
}
type SenderHandoffState struct {
	OperationID              string              `json:"operation_id"`
	CandidateUID             string              `json:"candidate_uid"`
	CandidateName            string              `json:"candidate_name"`
	CandidateSelector        map[string]string   `json:"candidate_selector"`
	OriginalUID              string              `json:"original_uid"`
	StableServiceUID         string              `json:"stable_service_uid"`
	OriginalReplicas         int32               `json:"original_replicas"`
	CandidateReplicas        int32               `json:"candidate_replicas"`
	ExecutorName             string              `json:"executor_name,omitempty"`
	ExecutorUID              string              `json:"executor_uid,omitempty"`
	ExecutorReplicas         int32               `json:"executor_replicas,omitempty"`
	ExecutorPods             []SenderPodEvidence `json:"executor_pods,omitempty"`
	ExecutorStopRequestedAt  *metav1.Time        `json:"executor_stop_requested_at,omitempty"`
	ExecutorExitVerifiedAt   *metav1.Time        `json:"executor_exit_verified_at,omitempty"`
	ExecutorFenced           bool                `json:"executor_fenced"`
	Stage                    string              `json:"stage"`
	StartedAt                metav1.Time         `json:"started_at"`
	OldStopRequestedAt       *metav1.Time        `json:"old_stop_requested_at,omitempty"`
	OldExitVerifiedAt        *metav1.Time        `json:"old_exit_verified_at,omitempty"`
	SwitchedAt               *metav1.Time        `json:"switched_at,omitempty"`
	RollbackStartedAt        *metav1.Time        `json:"rollback_started_at,omitempty"`
	CandidateStopRequestedAt *metav1.Time        `json:"candidate_stop_requested_at,omitempty"`
	CandidateExitVerifiedAt  *metav1.Time        `json:"candidate_exit_verified_at,omitempty"`
	RolledBackAt             *metav1.Time        `json:"rolled_back_at,omitempty"`
	OldPods                  []SenderPodEvidence `json:"old_pods"`
	CandidatePods            []SenderPodEvidence `json:"candidate_pods,omitempty"`
	CandidateFenced          bool                `json:"candidate_fenced"`
	StopTimeoutSeconds       int64               `json:"stop_timeout_seconds"`
	MaxGapSeconds            int64               `json:"max_gap_seconds"`
	LastError                string              `json:"last_error,omitempty"`
	// Old rc7 memory work cannot be enumerated. Never claim a zero-loss drain.
	ProcessUncertainty string              `json:"process_uncertainty,omitempty"`
	UnexpectedPods     []SenderPodEvidence `json:"unexpected_pods,omitempty"`
	LossCount          *int64              `json:"loss_count"`
}

func handoffState(nm *v2beta2.NotificationManager) (*SenderHandoffState, error) {
	raw := nm.Annotations[SenderHandoffStateAnnotation]
	if raw == "" {
		return nil, nil
	}
	if len(raw) > 64<<10 {
		return nil, errors.New("handoff state too large")
	}
	var s SenderHandoffState
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		return nil, err
	}
	if s.OperationID == "" || s.OriginalUID == "" || s.CandidateUID == "" || s.StableServiceUID == "" {
		return nil, errors.New("invalid persistent handoff state")
	}
	return &s, nil
}
func (r *NotificationManagerReconciler) saveState(ctx context.Context, nm *v2beta2.NotificationManager, s *SenderHandoffState) error {
	raw, err := json.Marshal(s)
	if err != nil {
		return err
	}
	if len(raw) > 64<<10 {
		return errors.New("handoff state exceeds bounded evidence size")
	}
	if nm.Annotations == nil {
		nm.Annotations = map[string]string{}
	}
	if nm.Annotations[SenderHandoffStateAnnotation] == string(raw) {
		return nil
	}
	nm.Annotations[SenderHandoffStateAnnotation] = string(raw)
	return r.Update(ctx, nm)
}
func (r *NotificationManagerReconciler) nodeReachable(ctx context.Context, name, uid string) error {
	var node corev1.Node
	if err := r.Get(ctx, client.ObjectKey{Name: name}, &node); err != nil {
		return fmt.Errorf("cannot verify node %s: %w", name, err)
	}
	if string(node.UID) != uid || node.DeletionTimestamp != nil {
		return errors.New("sender node identity changed or deleting")
	}
	ready := false
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue {
			ready = true
		}
	}
	if !ready {
		return errors.New("sender node is not Ready; API disappearance does not prove process exit")
	}
	var lease coordinationv1.Lease
	if err := r.Get(ctx, client.ObjectKey{Namespace: "kube-node-lease", Name: name}, &lease); err != nil {
		return fmt.Errorf("cannot verify node heartbeat lease: %w", err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != name || lease.Spec.RenewTime == nil {
		return errors.New("sender node lease identity/heartbeat unavailable")
	}
	age := time.Since(lease.Spec.RenewTime.Time)
	if age < -10*time.Second || age > 90*time.Second {
		return errors.New("sender node heartbeat stale; process state unknown")
	}
	return nil
}
func (r *NotificationManagerReconciler) deploymentPods(ctx context.Context, d *appsv1.Deployment) ([]corev1.Pod, error) {
	var list corev1.PodList
	if d.Spec.Selector == nil || len(d.Spec.Selector.MatchLabels) == 0 || len(d.Spec.Selector.MatchExpressions) != 0 {
		return nil, errors.New("managed sender requires a concrete matchLabels selector")
	}
	if err := r.List(ctx, &list, client.InNamespace(r.Namespace), client.MatchingLabels(d.Spec.Selector.MatchLabels)); err != nil {
		return nil, err
	}
	if len(list.Items) > 64 {
		return nil, errors.New("too many sender Pods for bounded stop evidence")
	}
	for _, p := range list.Items {
		owner := metav1.GetControllerOf(&p)
		if owner == nil || owner.Kind != "ReplicaSet" {
			return nil, errors.New("selected sender Pod does not have a Deployment ReplicaSet owner")
		}
		var rs appsv1.ReplicaSet
		if err := r.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: owner.Name}, &rs); err != nil {
			return nil, err
		}
		parent := metav1.GetControllerOf(&rs)
		if rs.UID != owner.UID || parent == nil || parent.Kind != "Deployment" || parent.UID != d.UID {
			return nil, errors.New("selected sender Pod belongs to a different Deployment UID")
		}
	}
	return list.Items, nil
}
func (r *NotificationManagerReconciler) capturePods(ctx context.Context, d *appsv1.Deployment) ([]SenderPodEvidence, error) {
	pods, err := r.deploymentPods(ctx, d)
	if err != nil {
		return nil, err
	}
	result := []SenderPodEvidence{}
	for _, p := range pods {
		if p.DeletionTimestamp != nil {
			return nil, errors.New("sender Pod was already deleting before guarded stop")
		}
		if p.Spec.NodeName == "" {
			return nil, errors.New("sender Pod node identity is unavailable")
		}
		var node corev1.Node
		if err := r.Get(ctx, client.ObjectKey{Name: p.Spec.NodeName}, &node); err != nil {
			return nil, err
		}
		if err := r.nodeReachable(ctx, node.Name, string(node.UID)); err != nil {
			return nil, err
		}
		ids := map[string]string{}
		for _, c := range p.Status.ContainerStatuses {
			if c.ContainerID == "" {
				return nil, errors.New("sender runtime container identity missing before stop")
			}
			ids[c.Name] = c.ContainerID
		}
		if len(ids) != len(p.Spec.Containers) {
			return nil, errors.New("sender containers not fully observable before stop")
		}
		result = append(result, SenderPodEvidence{Name: p.Name, UID: string(p.UID), NodeName: node.Name, NodeUID: string(node.UID), ContainerIDs: ids})
	}
	if len(result) == 0 && d.Spec.Replicas != nil && *d.Spec.Replicas > 0 {
		return nil, errors.New("sender has desired replicas but no verifiable Pods")
	}
	return result, nil
}
func (r *NotificationManagerReconciler) finalizeBeforeStop(ctx context.Context, evidence []SenderPodEvidence) error {
	for _, e := range evidence {
		var p corev1.Pod
		if err := r.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: e.Name}, &p); err != nil {
			return err
		}
		if string(p.UID) != e.UID || p.DeletionTimestamp != nil {
			return errors.New("sender changed before finalizer protected stop")
		}
		if controllerutil.AddFinalizer(&p, SenderStopFinalizer) {
			if err := r.Update(ctx, &p); err != nil {
				return err
			}
		}
	}
	return nil
}
func setFormalFence(d *appsv1.Deployment, operation string) {
	if d.Annotations == nil {
		d.Annotations = map[string]string{}
	}
	d.Annotations[FormalSenderFenceAnnotation] = operation
	for i := range d.Spec.Template.Spec.Containers {
		c := &d.Spec.Template.Spec.Containers[i]
		if c.Name == "intake-guard" {
			continue
		}
		found := false
		for j, e := range c.Env {
			if e.Name == "NM_FORMAL_DISABLED" {
				c.Env[j] = corev1.EnvVar{Name: "NM_FORMAL_DISABLED", Value: "true"}
				found = true
			}
		}
		if !found {
			c.Env = append(c.Env, corev1.EnvVar{Name: "NM_FORMAL_DISABLED", Value: "true"})
		}
	}
}
func (r *NotificationManagerReconciler) holdReplicas(ctx context.Context, d *appsv1.Deployment, count int32, fence string) error {
	before := d.DeepCopy()
	d.Spec.Replicas = &count
	if fence != "" {
		setFormalFence(d, fence)
	}
	if reflect.DeepEqual(before.Spec, d.Spec) && reflect.DeepEqual(before.Annotations, d.Annotations) {
		return nil
	}
	return r.Update(ctx, d)
}
func (r *NotificationManagerReconciler) stopVerified(ctx context.Context, nm *v2beta2.NotificationManager, s *SenderHandoffState, d *appsv1.Deployment, evidence []SenderPodEvidence) (bool, error) {
	if s.ProcessUncertainty != "" {
		return false, fmt.Errorf("persistent sender process uncertainty: %s", s.ProcessUncertainty)
	}
	// Observe every live Pod as well as sealed UIDs; a replica added by another
	// controller invalidates completion until normally stopped and accounted for.
	live, err := r.deploymentPods(ctx, d)
	if err != nil {
		return false, err
	}
	expected := map[string]bool{}
	for _, e := range evidence {
		expected[e.UID] = true
	}
	for _, p := range live {
		if !expected[string(p.UID)] {
			// Seeing an unsealed process is a durable failure, not a transient error
			// which disappears when another controller removes its API object.
			e := SenderPodEvidence{Name: p.Name, UID: string(p.UID), NodeName: p.Spec.NodeName}
			if p.Spec.NodeName != "" {
				var node corev1.Node
				if r.Get(ctx, client.ObjectKey{Name: p.Spec.NodeName}, &node) == nil {
					e.NodeUID = string(node.UID)
				}
			}
			s.UnexpectedPods = append(s.UnexpectedPods, e)
			s.ProcessUncertainty = "new sender Pod appeared after sealed stop intent; manual process recovery required"
			if err := r.saveState(ctx, nm, s); err != nil {
				return false, err
			}
			return false, errors.New(s.ProcessUncertainty)
		}
	}
	for i := range evidence {
		e := &evidence[i]
		if err := r.nodeReachable(ctx, e.NodeName, e.NodeUID); err != nil {
			return false, err
		}
		var p corev1.Pod
		err := r.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: e.Name}, &p)
		if err != nil {
			if client.IgnoreNotFound(err) != nil {
				return false, err
			}
			if e.TerminatedAt == nil {
				return false, errors.New("sender Pod disappeared without normal kubelet termination evidence")
			}
			continue
		}
		if string(p.UID) != e.UID {
			return false, errors.New("sender Pod UID changed during stop")
		}
		if p.DeletionTimestamp == nil {
			return false, nil
		}
		if e.ManagedNormalStopIntentAt == nil || e.ConfiguredGraceSeconds <= 0 || e.NormalDeleteRequestedAt == nil || e.NormalDeleteGraceSeconds <= 0 || (e.NormalDeleteResult != "acknowledged" && e.NormalDeleteResult != "response-unknown") {
			return false, errors.New("sender lacks a persisted UID-fenced positive-grace normal Delete request")
		}
		// Kubelet may shorten the API deletion grace to zero after every process has
		// ended. That mutable field is neither normal-exit proof nor force-exit proof.
		// Only the sealed delete intent, matching container identities and fresh node
		// witness may release this finalizer.
		observed := int64(-1)
		if p.DeletionGracePeriodSeconds != nil {
			observed = *p.DeletionGracePeriodSeconds
		}
		e.ObservedGraceSeconds = &observed
		if len(p.Status.ContainerStatuses) != len(p.Spec.Containers) {
			return false, nil
		}
		terminated := map[string]bool{}
		for _, c := range p.Status.ContainerStatuses {
			if c.State.Terminated == nil || c.State.Terminated.FinishedAt.IsZero() || c.ContainerID == "" {
				return false, nil
			}
			requested := s.OldStopRequestedAt
			if string(d.UID) == s.CandidateUID {
				requested = s.CandidateStopRequestedAt
			}
			if string(d.UID) == s.ExecutorUID {
				requested = s.ExecutorStopRequestedAt
			}
			if requested == nil || c.State.Terminated.FinishedAt.Time.Before(requested.Time.Add(-time.Second)) {
				return false, errors.New("container termination predates guarded stop intent")
			}
			if e.ContainerIDs[c.Name] != c.ContainerID {
				s.ProcessUncertainty = "runtime container identity changed after sealed stop intent"
				if err := r.saveState(ctx, nm, s); err != nil {
					return false, err
				}
				return false, errors.New(s.ProcessUncertainty)
			}
			terminated[c.Name] = true
		}
		for _, c := range p.Spec.Containers {
			if !terminated[c.Name] {
				return false, errors.New("container termination evidence does not match sealed sender containers")
			}
		}
		if e.TerminatedAt == nil {
			now := metav1.Now()
			e.NodeReadyVerifiedAt = &now
			var lease coordinationv1.Lease
			if err := r.Get(ctx, client.ObjectKey{Namespace: "kube-node-lease", Name: e.NodeName}, &lease); err != nil {
				return false, err
			}
			if lease.Spec.RenewTime == nil {
				return false, errors.New("node lease renew time missing at termination evidence write")
			}
			renew := *lease.Spec.RenewTime
			e.NodeLeaseRenewedAt = &renew
			e.NormalGraceSeconds = e.NormalDeleteGraceSeconds
			e.ContainerExits = []SenderContainerExit{}
			for _, c := range p.Status.ContainerStatuses {
				ended := c.State.Terminated
				e.ContainerExits = append(e.ContainerExits, SenderContainerExit{Name: c.Name, ContainerID: c.ContainerID, ExitCode: ended.ExitCode, Reason: ended.Reason, FinishedAt: ended.FinishedAt})
				if e.TerminatedAt == nil || ended.FinishedAt.After(e.TerminatedAt.Time) {
					finished := ended.FinishedAt
					e.TerminatedAt = &finished
				}
			}
			if err := r.saveState(ctx, nm, s); err != nil {
				return false, err
			}
		}
		// Persist first, then release. A restart cannot mistake a missing API object
		// for termination observed before the evidence write.
		if controllerutil.RemoveFinalizer(&p, SenderStopFinalizer) {
			if err := r.Update(ctx, &p); err != nil {
				return false, err
			}
		}
		return false, nil
	}
	return len(live) == 0, nil
}
func (r *NotificationManagerReconciler) formalReady(ctx context.Context, d *appsv1.Deployment, nm *v2beta2.NotificationManager) error {
	port := nm.Spec.PortName
	if port == "" {
		port = defaultPortName
	}
	if !deploymentReady(d) || !formalCandidateContract(d, port) {
		return errors.New("candidate formal listener/guard contract or Deployment readiness not satisfied")
	}
	pods, err := r.deploymentPods(ctx, d)
	if err != nil {
		return err
	}
	if len(pods) == 0 {
		return errors.New("candidate has no actual Pods")
	}
	for _, p := range pods {
		ready := false
		for _, c := range p.Status.Conditions {
			if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
				ready = true
			}
		}
		if !ready || p.DeletionTimestamp != nil || net.ParseIP(p.Status.PodIP) == nil {
			return errors.New("candidate selected Pod is not Ready or has no valid Pod IP")
		}
		if r.FormalReadyProbe != nil {
			if err := r.FormalReadyProbe(ctx, &p); err != nil {
				return err
			}
			continue
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+net.JoinHostPort(p.Status.PodIP, "19093")+"/-/formal-ready", nil)
		if err != nil {
			return err
		}
		// Never follow redirects to a different endpoint or environment.
		hc := http.Client{Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		resp, err := hc.Do(req)
		if err != nil {
			return fmt.Errorf("actual guarded formal readiness unavailable: %w", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("actual guarded formal readiness HTTP %d", resp.StatusCode)
		}
	}
	return nil
}
func (r *NotificationManagerReconciler) reconcileHandoff(ctx context.Context, nm *v2beta2.NotificationManager, h *SenderHandoff) (ctrl.Result, error) {
	again := ctrl.Result{RequeueAfter: time.Second}
	s, err := handoffState(nm)
	if err != nil {
		return again, err
	}
	candidate, err := r.candidate(ctx, nm, h)
	if err != nil {
		return again, err
	}
	var original appsv1.Deployment
	if err := r.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: nm.Name + "-deployment"}, &original); err != nil {
		return again, err
	}
	if string(original.UID) != h.OriginalDeploymentUID {
		return again, errors.New("original Deployment UID differs from sealed intent")
	}
	var stable corev1.Service
	if err := r.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: nm.Name + "-svc"}, &stable); err != nil {
		return again, err
	}
	if string(stable.UID) != h.StableServiceUID {
		return again, errors.New("stable Service UID differs from sealed intent")
	}
	var executor *appsv1.Deployment
	if h.ExecutorDeployment != "" {
		executor = &appsv1.Deployment{}
		if err := r.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: h.ExecutorDeployment}, executor); err != nil {
			return again, err
		}
		if string(executor.UID) != h.ExecutorDeploymentUID || executor.Spec.Replicas == nil {
			return again, errors.New("executor UID/replica count differs from sealed intent")
		}
	}
	if s == nil {
		if !reflect.DeepEqual(stable.Spec.Selector, *r.makeCommonLabels(nm)) {
			return again, errors.New("new handoff requires stable Service exclusively on original sender")
		}
		if h.Phase != "stopped" {
			return again, errors.New("managed handoff must start with stopped, not an unproven retired/rollback")
		}
		if err := r.formalReady(ctx, candidate, nm); err != nil {
			return again, err
		}
		if !deploymentReady(&original) {
			return again, errors.New("original sender must be Ready before a new managed stop")
		}
		oldPods, err := r.capturePods(ctx, &original)
		if err != nil {
			return again, err
		}
		if original.Spec.Replicas == nil || candidate.Spec.Replicas == nil {
			return again, errors.New("sealed sender replica counts missing")
		}
		s = &SenderHandoffState{OperationID: h.OperationID, CandidateUID: h.DeploymentUID, CandidateName: h.Deployment, CandidateSelector: h.Selector, OriginalUID: h.OriginalDeploymentUID, StableServiceUID: h.StableServiceUID, OriginalReplicas: *original.Spec.Replicas, CandidateReplicas: *candidate.Spec.Replicas, Stage: "stop-requested", StartedAt: metav1.Now(), OldPods: oldPods, StopTimeoutSeconds: h.StopTimeoutSeconds, MaxGapSeconds: h.MaxGapSeconds}
		if executor != nil {
			s.ExecutorName = h.ExecutorDeployment
			s.ExecutorUID = h.ExecutorDeploymentUID
			s.ExecutorReplicas = *executor.Spec.Replicas
		}
		if err := r.saveState(ctx, nm, s); err != nil {
			return again, err
		}
	}
	if s.OperationID != h.OperationID || s.CandidateUID != h.DeploymentUID || s.CandidateName != h.Deployment || s.ExecutorName != h.ExecutorDeployment || s.ExecutorUID != h.ExecutorDeploymentUID || !reflect.DeepEqual(s.CandidateSelector, h.Selector) || s.OriginalUID != h.OriginalDeploymentUID || s.StableServiceUID != h.StableServiceUID || s.StopTimeoutSeconds != h.StopTimeoutSeconds || s.MaxGapSeconds != h.MaxGapSeconds {
		return again, errors.New("handoff immutable operation/identity/budget differs from persistent state")
	}
	if s.ProcessUncertainty != "" {
		// Keep the sealed stop intent enforced while waiting for manual recovery.
		if err := r.holdReplicas(ctx, &original, 0, ""); err != nil {
			return again, err
		}
		if s.CandidateFenced {
			if err := r.holdReplicas(ctx, candidate, 0, s.OperationID); err != nil {
				return again, err
			}
		}
		if s.ExecutorFenced && executor != nil {
			if err := r.holdExecutor(ctx, executor, 0, s.OperationID); err != nil {
				return again, err
			}
		}
		return again, fmt.Errorf("persistent sender process uncertainty: %s", s.ProcessUncertainty)
	}
	if h.Phase == "stopped" && (s.SwitchedAt != nil || reflect.DeepEqual(stable.Spec.Selector, h.Selector) || stable.Annotations[SenderHandoffAnnotation] == h.OperationID+":retired") {
		return again, errors.New("retired operation cannot regress to stopped; explicit rollback required")
	}
	if s.Stage == "test-restored" && h.Phase != "test-restored" {
		return again, errors.New("completed test restoration cannot replay an earlier operation phase")
	}
	fail := func(err error) (ctrl.Result, error) {
		s.LastError = err.Error()
		if saveErr := r.saveState(ctx, nm, s); saveErr != nil {
			return again, saveErr
		}
		return again, err
	}
	if h.Phase == "stopped" || h.Phase == "retired" {
		if s.RollbackStartedAt != nil {
			return fail(errors.New("rolled-back operation cannot reactivate formal sender"))
		}
		if s.OldStopRequestedAt == nil {
			if err := r.finalizeBeforeStop(ctx, s.OldPods); err != nil {
				return fail(err)
			}
			now := metav1.Now()
			s.OldStopRequestedAt = &now
			if err := r.saveState(ctx, nm, s); err != nil {
				return again, err
			}
		}
		if err := r.sealManagedNormalStop(ctx, nm, s, s.OldPods); err != nil {
			return fail(err)
		}
		if err := r.holdReplicas(ctx, &original, 0, ""); err != nil {
			return fail(err)
		}
		if err := r.requestNormalDeletes(ctx, nm, s, s.OldPods); err != nil {
			return fail(err)
		}
		if s.OldExitVerifiedAt == nil {
			done, err := r.stopVerified(ctx, nm, s, &original, s.OldPods)
			if err != nil {
				return fail(err)
			}
			if !done {
				if time.Since(s.OldStopRequestedAt.Time) > time.Duration(s.StopTimeoutSeconds)*time.Second {
					return fail(errors.New("old sender stop timeout; no automatic takeover"))
				}
				return again, nil
			}
			now := metav1.Now()
			s.OldExitVerifiedAt = &now
			s.Stage = "stopped"
			s.LastError = ""
			if err := r.saveState(ctx, nm, s); err != nil {
				return again, err
			}
		}
		// Verify sealed node/process evidence again on the actual switching reconcile.
		done, err := r.stopVerified(ctx, nm, s, &original, s.OldPods)
		if err != nil || !done {
			if err == nil {
				err = errors.New("old sender has not stopped")
			}
			return fail(err)
		}
		if h.Phase == "stopped" {
			return again, r.createManagedHandoffService(ctx, nm, h, *r.makeCommonLabels(nm))
		}
		if s.SwitchedAt == nil {
			// Recover an applied Service update whose response or state write was lost.
			applied, err := r.serviceAppliedAt(ctx, nm, h, h.Selector)
			if err != nil {
				return fail(err)
			}
			if applied != nil {
				s.SwitchedAt = applied
			}
		}
		if s.SwitchedAt == nil {
			if time.Since(s.OldStopRequestedAt.Time) > time.Duration(s.MaxGapSeconds)*time.Second {
				return fail(errors.New("sealed NM gap budget exceeded; manual rollback required"))
			}
			if err := r.formalReady(ctx, candidate, nm); err != nil {
				return fail(err)
			}
		}
		if err := r.createManagedHandoffService(ctx, nm, h, h.Selector); err != nil {
			return fail(err)
		}
		if s.SwitchedAt == nil {
			applied, err := r.serviceAppliedAt(ctx, nm, h, h.Selector)
			if err != nil || applied == nil {
				if err == nil {
					err = errors.New("applied handoff timestamp missing")
				}
				return fail(err)
			}
			s.SwitchedAt = applied
		}
		s.Stage = "retired"
		s.LastError = ""
		return again, r.saveState(ctx, nm, s)
	}
	if h.Phase == "rollback" || h.Phase == "test-restored" {
		if s.RollbackStartedAt == nil {
			if h.Phase != "rollback" {
				return fail(errors.New("test restoration requires completed managed rollback"))
			}
			pods, err := r.capturePods(ctx, candidate)
			if err != nil {
				return fail(err)
			}
			var executorPods []SenderPodEvidence
			if executor != nil {
				executorPods, err = r.capturePods(ctx, executor)
				if err != nil {
					return fail(err)
				}
			}
			now := metav1.Now()
			s.ExecutorPods = executorPods
			s.ExecutorFenced = executor != nil
			s.RollbackStartedAt = &now
			s.CandidatePods = pods
			s.CandidateFenced = true
			s.Stage = "candidate-stop-requested"
			if err := r.saveState(ctx, nm, s); err != nil {
				return again, err
			}
		}
		if s.CandidateStopRequestedAt == nil {
			if err := r.finalizeBeforeStop(ctx, s.CandidatePods); err != nil {
				return fail(err)
			}
			now := metav1.Now()
			s.CandidateStopRequestedAt = &now
			if err := r.saveState(ctx, nm, s); err != nil {
				return again, err
			}
		}
		if err := r.sealManagedNormalStop(ctx, nm, s, s.CandidatePods); err != nil {
			return fail(err)
		}
		if executor != nil {
			if s.ExecutorStopRequestedAt == nil {
				if err := r.finalizeBeforeStop(ctx, s.ExecutorPods); err != nil {
					return fail(err)
				}
				now := metav1.Now()
				s.ExecutorStopRequestedAt = &now
				if err := r.saveState(ctx, nm, s); err != nil {
					return again, err
				}
			}
			if err := r.sealManagedNormalStop(ctx, nm, s, s.ExecutorPods); err != nil {
				return fail(err)
			}
			if err := r.holdExecutor(ctx, executor, 0, s.OperationID); err != nil {
				return fail(err)
			}
			if err := r.requestNormalDeletes(ctx, nm, s, s.ExecutorPods); err != nil {
				return fail(err)
			}
			if s.ExecutorExitVerifiedAt == nil {
				// Candidate is held stopped while writer termination is being verified.
				if err := r.holdReplicas(ctx, candidate, 0, s.OperationID); err != nil {
					return fail(err)
				}
				if err := r.requestNormalDeletes(ctx, nm, s, s.CandidatePods); err != nil {
					return fail(err)
				}
				if err := r.holdReplicas(ctx, &original, 0, ""); err != nil {
					return fail(err)
				}
				done, err := r.stopVerified(ctx, nm, s, executor, s.ExecutorPods)
				if err != nil {
					return fail(err)
				}
				if !done {
					if time.Since(s.ExecutorStopRequestedAt.Time) > time.Duration(s.StopTimeoutSeconds)*time.Second {
						return fail(errors.New("executor stop timeout; original sender remains stopped"))
					}
					return again, nil
				}
				now := metav1.Now()
				s.ExecutorExitVerifiedAt = &now
				if err := r.saveState(ctx, nm, s); err != nil {
					return again, err
				}
			}
		}
		if h.Phase == "test-restored" {
			if s.RolledBackAt == nil || s.CandidateExitVerifiedAt == nil {
				return fail(errors.New("candidate test restoration requires completed rollback exit proof"))
			}
			if !formalDisabled(candidate) {
				return fail(errors.New("test restoration requires NM_FORMAL_DISABLED=true durable startup fence"))
			}
			if err := r.holdReplicas(ctx, candidate, s.CandidateReplicas, s.OperationID); err != nil {
				return fail(err)
			}
			if err := r.holdReplicas(ctx, &original, s.OriginalReplicas, ""); err != nil {
				return fail(err)
			}
			if !deploymentReady(&original) {
				return fail(errors.New("original backend not Ready"))
			}
			s.Stage = "test-restored"
			s.LastError = ""
			if err := r.createManagedHandoffService(ctx, nm, h, *r.makeCommonLabels(nm)); err != nil {
				return fail(err)
			}
			return again, r.saveState(ctx, nm, s)
		}
		if err := r.holdReplicas(ctx, candidate, 0, s.OperationID); err != nil {
			return fail(err)
		}
		if err := r.requestNormalDeletes(ctx, nm, s, s.CandidatePods); err != nil {
			return fail(err)
		}
		if s.CandidateExitVerifiedAt == nil {
			// Keep the old sender stopped until all candidate processes/connections end.
			if err := r.holdReplicas(ctx, &original, 0, ""); err != nil {
				return fail(err)
			}
			done, err := r.stopVerified(ctx, nm, s, candidate, s.CandidatePods)
			if err != nil {
				return fail(err)
			}
			if !done {
				if time.Since(s.CandidateStopRequestedAt.Time) > time.Duration(s.StopTimeoutSeconds)*time.Second {
					return fail(errors.New("candidate stop timeout; original sender remains stopped"))
				}
				return again, nil
			}
			now := metav1.Now()
			s.CandidateExitVerifiedAt = &now
			s.Stage = "original-restoring"
			if err := r.saveState(ctx, nm, s); err != nil {
				return again, err
			}
		}
		// A rollback may arrive during stopped while the original kubelet is
		// still terminating. Finish and release its protected evidence before
		// restoring replicas, rather than abandoning finalizers on old Pods.
		if s.OldExitVerifiedAt == nil {
			if err := r.holdReplicas(ctx, &original, 0, ""); err != nil {
				return fail(err)
			}
			done, err := r.stopVerified(ctx, nm, s, &original, s.OldPods)
			if err != nil {
				return fail(err)
			}
			if !done {
				if time.Since(s.RollbackStartedAt.Time) > time.Duration(s.StopTimeoutSeconds)*time.Second {
					return fail(errors.New("original stop not proven during rollback; manual process recovery required"))
				}
				return again, nil
			}
			now := metav1.Now()
			s.OldExitVerifiedAt = &now
			if err := r.saveState(ctx, nm, s); err != nil {
				return again, err
			}
		}
		if err := r.holdReplicas(ctx, &original, s.OriginalReplicas, ""); err != nil {
			return fail(err)
		}
		if !deploymentReady(&original) {
			if time.Since(s.RollbackStartedAt.Time) > time.Duration(s.MaxGapSeconds)*time.Second {
				return fail(errors.New("rollback gap budget exceeded; original backend is not Ready"))
			}
			return again, nil
		}
		if err := r.createManagedHandoffService(ctx, nm, h, *r.makeCommonLabels(nm)); err != nil {
			return fail(err)
		}
		if s.RolledBackAt == nil {
			applied, err := r.serviceAppliedAt(ctx, nm, h, *r.makeCommonLabels(nm))
			if err != nil || applied == nil {
				if err == nil {
					err = errors.New("applied rollback timestamp missing")
				}
				return fail(err)
			}
			s.RolledBackAt = applied
		}
		s.Stage = "rollback"
		s.LastError = ""
		return again, r.saveState(ctx, nm, s)
	}
	return fail(errors.New("unsupported handoff phase"))
}
func formalDisabled(d *appsv1.Deployment) bool {
	seen := false
	for _, c := range d.Spec.Template.Spec.Containers {
		if c.Name == "intake-guard" {
			continue
		}
		found := false
		for _, e := range c.Env {
			if e.Name == "NM_FORMAL_DISABLED" && e.Value == "true" && e.ValueFrom == nil {
				found = true
			}
		}
		if !found {
			return false
		}
		seen = true
	}
	return seen
}
func specFingerprint(nm *v2beta2.NotificationManager) string {
	b, _ := json.Marshal(nm.Spec)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// Removing the instruction is never an implicit rollback or fence release.
func (r *NotificationManagerReconciler) reconcileMissingIntent(ctx context.Context, nm *v2beta2.NotificationManager, s *SenderHandoffState) (ctrl.Result, error) {
	again := ctrl.Result{RequeueAfter: time.Second}
	if s.ExecutorFenced && s.ExecutorName != "" {
		var executor appsv1.Deployment
		if err := r.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: s.ExecutorName}, &executor); err != nil {
			return again, err
		}
		if string(executor.UID) != s.ExecutorUID {
			return again, errors.New("fenced executor UID changed")
		}
		if err := r.holdExecutor(ctx, &executor, 0, s.OperationID); err != nil {
			return again, err
		}
	}
	if s.CandidateFenced {
		var candidate appsv1.Deployment
		if err := r.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: s.CandidateName}, &candidate); err != nil {
			return again, err
		}
		if string(candidate.UID) != s.CandidateUID {
			return again, errors.New("fenced candidate UID changed")
		}
		if err := r.holdReplicas(ctx, &candidate, 0, s.OperationID); err != nil {
			return again, err
		}
	}
	if s.RolledBackAt == nil {
		var old appsv1.Deployment
		if err := r.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: nm.Name + "-deployment"}, &old); err != nil {
			return again, err
		}
		if string(old.UID) != s.OriginalUID {
			return again, errors.New("sealed original UID changed")
		}
		if err := r.holdReplicas(ctx, &old, 0, ""); err != nil {
			return again, err
		}
	}
	return again, errors.New("persistent handoff intent removed; restore explicit operation phase before reconciliation")
}

func (r *NotificationManagerReconciler) serviceAppliedAt(ctx context.Context, nm *v2beta2.NotificationManager, h *SenderHandoff, selector map[string]string) (*metav1.Time, error) {
	var svc corev1.Service
	if err := r.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: nm.Name + "-svc"}, &svc); err != nil {
		if client.IgnoreNotFound(err) != nil {
			return nil, err
		}
		return nil, nil
	}
	if svc.Annotations[SenderHandoffAnnotation] != h.OperationID+":"+h.Phase || !reflect.DeepEqual(svc.Spec.Selector, selector) {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339Nano, svc.Annotations[SenderHandoffAppliedAtAnnotation])
	if err != nil {
		return nil, errors.New("applied Service operation timestamp invalid")
	}
	stamp := metav1.NewTime(t)
	return &stamp, nil
}

func (r *NotificationManagerReconciler) holdExecutor(ctx context.Context, d *appsv1.Deployment, count int32, operation string) error {
	before := d.DeepCopy()
	d.Spec.Replicas = &count
	if d.Annotations == nil {
		d.Annotations = map[string]string{}
	}
	d.Annotations[FormalSenderFenceAnnotation] = operation
	for i := range d.Spec.Template.Spec.Containers {
		c := &d.Spec.Template.Spec.Containers[i]
		if c.Name == "intake-guard" {
			continue
		}
		found := false
		for j, e := range c.Env {
			if e.Name == "JEV_FORMAL_WRITER_DISABLED" {
				c.Env[j] = corev1.EnvVar{Name: "JEV_FORMAL_WRITER_DISABLED", Value: "true"}
				found = true
			}
		}
		if !found {
			c.Env = append(c.Env, corev1.EnvVar{Name: "JEV_FORMAL_WRITER_DISABLED", Value: "true"})
		}
	}
	if reflect.DeepEqual(before.Spec, d.Spec) && reflect.DeepEqual(before.Annotations, d.Annotations) {
		return nil
	}
	return r.Update(ctx, d)
}

// Preserve the normal positive-grace deletion protocol before sending a UID/RV
// preconditioned Delete. Scale0 comes first so ReplicaSets cannot replace it.
func (r *NotificationManagerReconciler) requestNormalDeletes(ctx context.Context, nm *v2beta2.NotificationManager, s *SenderHandoffState, evidence []SenderPodEvidence) error {
	for i := range evidence {
		e := &evidence[i]
		if e.NormalDeleteResult == "acknowledged" || e.NormalDeleteResult == "response-unknown" {
			continue
		}
		var p corev1.Pod
		if err := r.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: e.Name}, &p); err != nil {
			if e.TerminatedAt != nil && client.IgnoreNotFound(err) == nil {
				continue
			}
			return err
		}
		if string(p.UID) != e.UID {
			return errors.New("sender UID changed before normal Delete")
		}
		if p.DeletionTimestamp != nil && e.NormalDeleteRequestedAt == nil && (p.DeletionGracePeriodSeconds == nil || *p.DeletionGracePeriodSeconds <= 0) {
			// A normally scaled-down Pod may already be terminal before the follow-up
			// request. Still require sealed runtime identity and real kubelet exit;
			// an API-zero-grace object containing a live process cannot bootstrap proof.
			ended := len(p.Status.ContainerStatuses) == len(p.Spec.Containers)
			for _, c := range p.Status.ContainerStatuses {
				if c.State.Terminated == nil || c.ContainerID == "" || e.ContainerIDs[c.Name] != c.ContainerID {
					ended = false
				}
			}
			if !ended {
				return errors.New("zero-grace sender still has an unproven live process")
			}
		}
		grace := e.ConfiguredGraceSeconds
		if e.ManagedNormalStopIntentAt == nil {
			return errors.New("normal stop intent was not sealed before replicas were stopped")
		}
		if grace <= 0 {
			return errors.New("managed stop requires positive configured Pod termination grace")
		}
		now := metav1.Now()
		e.NormalDeleteRequestedAt = &now
		e.NormalDeleteGraceSeconds = grace
		e.NormalDeleteResourceVersion = p.ResourceVersion
		e.NormalDeleteResult = "pending"
		if err := r.saveState(ctx, nm, s); err != nil {
			return err
		}
		uid := p.UID
		rv := p.ResourceVersion
		err := r.Delete(ctx, &p, &client.DeleteOptions{GracePeriodSeconds: &grace, Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}})
		if err == nil {
			e.NormalDeleteResult = "acknowledged"
		} else if apierrors.IsConflict(err) || apierrors.IsForbidden(err) || apierrors.IsInvalid(err) {
			e.NormalDeleteResult = "rejected"
		} else {
			e.NormalDeleteResult = "response-unknown"
		}
		if saveErr := r.saveState(ctx, nm, s); saveErr != nil {
			return saveErr
		}
		if err != nil && e.NormalDeleteResult != "response-unknown" {
			return err
		}
	}
	return nil
}

// This is the pre-stop plan, not a claim about when the later API Delete RPC
// completed. ReplicaSet/kubelet deletion may finish before that RPC executes.
func (r *NotificationManagerReconciler) sealManagedNormalStop(ctx context.Context, nm *v2beta2.NotificationManager, s *SenderHandoffState, evidence []SenderPodEvidence) error {
	for i := range evidence {
		e := &evidence[i]
		if e.ManagedNormalStopIntentAt != nil {
			continue
		}
		var p corev1.Pod
		if err := r.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: e.Name}, &p); err != nil {
			return err
		}
		if string(p.UID) != e.UID || p.DeletionTimestamp != nil {
			return errors.New("cannot seal a normal stop plan for a changed/already-deleting sender")
		}
		grace := int64(30)
		if p.Spec.TerminationGracePeriodSeconds != nil {
			grace = *p.Spec.TerminationGracePeriodSeconds
		}
		if grace <= 0 {
			return errors.New("sender has no positive configured normal termination grace")
		}
		now := metav1.Now()
		e.ManagedNormalStopIntentAt = &now
		e.ConfiguredGraceSeconds = grace
		if err := r.saveState(ctx, nm, s); err != nil {
			return err
		}
	}
	return nil
}
