package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/kubesphere/notification-manager/apis/v2beta2"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const SenderHandoffAnnotation = "notification.kubesphere.io/sender-handoff"
const SenderHandoffAppliedAtAnnotation = "notification.kubesphere.io/sender-handoff-applied-at"
const SenderHandoffStateAnnotation = "notification.kubesphere.io/sender-handoff-state"
const FormalSenderFenceAnnotation = "notification.kubesphere.io/formal-sender-fence"
const SenderStopFinalizer = "notification.kubesphere.io/sender-stop-evidence"
const reconciledSpecAnnotation = "notification.kubesphere.io/reconciled-spec"

type SenderHandoff struct {
	OperationID           string            `json:"operation_id"`
	Phase                 string            `json:"phase"` // stopped, retired, rollback, test-restored
	Deployment            string            `json:"deployment"`
	DeploymentUID         string            `json:"deployment_uid"`
	Selector              map[string]string `json:"selector"`
	StableServiceUID      string            `json:"stable_service_uid"`
	OriginalDeploymentUID string            `json:"original_deployment_uid"`
	StopTimeoutSeconds    int64             `json:"stop_timeout_seconds"`
	MaxGapSeconds         int64             `json:"max_gap_seconds"`
	// Kept for older clients; free text never substitutes for process evidence.
	ExecutorDeployment    string                  `json:"executor_deployment,omitempty"`
	ExecutorDeploymentUID string                  `json:"executor_deployment_uid,omitempty"`
	DrainEvidence         string                  `json:"drain_evidence,omitempty"`
	PreservedTest         *PreservedTestReference `json:"preserved_test,omitempty"`
}

func handoffIntent(nm *v2beta2.NotificationManager) (*SenderHandoff, error) {
	raw := nm.Annotations[SenderHandoffAnnotation]
	if raw == "" {
		return nil, nil
	}
	if len(raw) > 16<<10 {
		return nil, errors.New("handoff intent too large")
	}
	var h SenderHandoff
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&h); err != nil {
		return nil, err
	}
	if err := dec.Decode(new(interface{})); err != io.EOF {
		return nil, errors.New("handoff intent must contain one JSON object")
	}
	if h.OperationID == "" || len(h.OperationID) > 128 || h.Deployment == "" || h.DeploymentUID == "" || len(h.Selector) == 0 || h.OriginalDeploymentUID == "" || h.StableServiceUID == "" {
		return nil, errors.New("handoff requires operation, original/candidate UID and disjoint selector")
	}
	if h.Phase != "stopped" && h.Phase != "retired" && h.Phase != "rollback" && h.Phase != "test-restored" {
		return nil, errors.New("invalid handoff phase; use managed stopped before retired")
	}
	if (h.ExecutorDeployment == "") != (h.ExecutorDeploymentUID == "") {
		return nil, errors.New("executor stop identity must include both deployment name and UID")
	}
	if h.ExecutorDeployment != "" && (h.ExecutorDeployment == h.Deployment || h.ExecutorDeployment == nm.Name+"-deployment") {
		return nil, errors.New("executor must be independent from both notification senders")
	}
	if h.StopTimeoutSeconds < 1 || h.StopTimeoutSeconds > 600 || h.MaxGapSeconds < 1 || h.MaxGapSeconds > 600 {
		return nil, errors.New("handoff requires bounded stop_timeout_seconds/max_gap_seconds in 1..600")
	}
	return &h, nil
}
func labelsMatch(selector, labels map[string]string) bool {
	for k, v := range selector {
		if labels[k] != v {
			return false
		}
	}
	return true
}
func deploymentReady(d *appsv1.Deployment) bool {
	return d.Spec.Replicas != nil && *d.Spec.Replicas > 0 && d.Status.ObservedGeneration >= d.Generation && d.Status.ReadyReplicas >= *d.Spec.Replicas && d.Status.UpdatedReplicas >= *d.Spec.Replicas
}

func (r *NotificationManagerReconciler) candidate(ctx context.Context, nm *v2beta2.NotificationManager, h *SenderHandoff) (*appsv1.Deployment, error) {
	old := *r.makeCommonLabels(nm)
	if labelsMatch(h.Selector, old) || h.Deployment == nm.Name+"-deployment" {
		return nil, errors.New("candidate must have independent deployment and disjoint selector")
	}
	var candidate appsv1.Deployment
	if err := r.Get(ctx, types.NamespacedName{Namespace: r.Namespace, Name: h.Deployment}, &candidate); err != nil {
		return nil, err
	}
	if string(candidate.UID) != h.DeploymentUID || !labelsMatch(h.Selector, candidate.Spec.Template.Labels) {
		return nil, errors.New("candidate UID/selector mismatch")
	}
	var deployments appsv1.DeploymentList
	if err := r.List(ctx, &deployments, client.InNamespace(r.Namespace)); err != nil {
		return nil, err
	}
	for _, d := range deployments.Items {
		if d.Name != h.Deployment && labelsMatch(h.Selector, d.Spec.Template.Labels) {
			return nil, fmt.Errorf("candidate selector also selects deployment %s", d.Name)
		}
	}
	return &candidate, nil
}
func formalCandidateContract(d *appsv1.Deployment, port string) bool {
	// The deployed guard owns the stable public port; the sender's two listeners
	// remain loopback-only. A readiness probe must explicitly test formal state.
	for _, c := range d.Spec.Template.Spec.Containers {
		compat, sources, primary := false, false, false
		for _, e := range c.Env {
			if e.Name == "NM_FORMAL_COMPAT_LISTEN_ADDRESS" && (e.Value == "127.0.0.1:19097") {
				compat = true
			}
			if e.Name == "NM_FORMAL_COMPAT_SOURCE_CIDRS" && (e.Value != "" || e.ValueFrom != nil) {
				sources = true
			}
		}
		for i, a := range c.Args {
			if a == "--webhook.address=127.0.0.1:19096" || (a == "--webhook.address" && i+1 < len(c.Args) && c.Args[i+1] == "127.0.0.1:19096") {
				primary = true
			}
		}
		if !compat || !sources || !primary {
			continue
		}
		for _, guard := range d.Spec.Template.Spec.Containers {
			if guard.Name != "intake-guard" || guard.ReadinessProbe == nil || guard.ReadinessProbe.HTTPGet == nil {
				continue
			}
			probe := guard.ReadinessProbe.HTTPGet
			if (probe.Path != "/-/formal-ready" && probe.Path != "/-/ready") || probe.Host != "" || (probe.Scheme != "" && probe.Scheme != corev1.URISchemeHTTP) {
				continue
			}
			for _, p := range guard.Ports {
				if p.Name == port && p.ContainerPort == 19093 && (p.Protocol == "" || p.Protocol == corev1.ProtocolTCP) && (probe.Port == intstr.FromInt(19093) || probe.Port == intstr.FromString(port) || probe.Port == intstr.FromInt(19094) || probe.Port == intstr.FromString("test-intake")) {
					return true
				}
			}
		}
	}
	return false
}
func (r *NotificationManagerReconciler) createManagedHandoffService(ctx context.Context, nm *v2beta2.NotificationManager, h *SenderHandoff, selector map[string]string) error {
	var svc corev1.Service
	key := types.NamespacedName{Namespace: r.Namespace, Name: nm.Name + "-svc"}
	if err := r.Get(ctx, key, &svc); err != nil {
		return err
	}
	if string(svc.UID) != h.StableServiceUID {
		return errors.New("stable Service UID changed; restore sealed original entry before handoff")
	}
	if labelsMatch(selector, svc.Spec.Selector) && labelsMatch(svc.Spec.Selector, selector) && svc.Annotations[SenderHandoffAnnotation] == h.OperationID+":"+h.Phase {
		return nil
	}
	svc.Spec.Selector = selector
	if svc.Annotations == nil {
		svc.Annotations = map[string]string{}
	}
	svc.Annotations[SenderHandoffAnnotation] = h.OperationID + ":" + h.Phase
	svc.Annotations[SenderHandoffAppliedAtAnnotation] = time.Now().UTC().Truncate(time.Second).Format(time.RFC3339)
	return r.Update(ctx, &svc)
}
