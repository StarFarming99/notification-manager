package controllers

import (
	"context"
	"errors"
	"net"
	"reflect"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// PreservedTestReference seals a separately running base-notification test
// sender. It is not a claim that the old shared c62 ledger was migrated.
type PreservedTestReference struct {
	Deployment    string            `json:"deployment"`
	DeploymentUID string            `json:"deployment_uid"`
	Service       string            `json:"service"`
	ServiceUID    string            `json:"service_uid"`
	Selector      map[string]string `json:"selector"`
}

func profiledCandidate(d *appsv1.Deployment) bool {
	for _, c := range d.Spec.Template.Spec.Containers {
		for _, e := range c.Env {
			if e.Name == "NM_DELIVERY_PROFILES_FILE" && (e.Value != "" || e.ValueFrom != nil) {
				return true
			}
		}
	}
	return false
}

func claimNames(d *appsv1.Deployment) map[string]bool {
	result := map[string]bool{}
	for _, volume := range d.Spec.Template.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil {
			result[volume.PersistentVolumeClaim.ClaimName] = true
		}
	}
	return result
}

func (r *NotificationManagerReconciler) preservedTestReady(ctx context.Context, ref *PreservedTestReference, original, candidate *appsv1.Deployment) error {
	if ref == nil || ref.Deployment == "" || ref.DeploymentUID == "" || ref.Service == "" || ref.ServiceUID == "" || len(ref.Selector) == 0 {
		return errors.New("BLOCKED: a sealed independent test sender is required; shared test/formal rollback cannot preserve base notifications")
	}
	if ref.Deployment == original.Name || ref.Deployment == candidate.Name || ref.DeploymentUID == string(original.UID) || ref.DeploymentUID == string(candidate.UID) || labelsMatch(ref.Selector, original.Spec.Template.Labels) || labelsMatch(ref.Selector, candidate.Spec.Template.Labels) {
		return errors.New("BLOCKED: protected test sender must be independent of both formal senders")
	}
	var test appsv1.Deployment
	if err := r.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: ref.Deployment}, &test); err != nil {
		return err
	}
	if string(test.UID) != ref.DeploymentUID || !deploymentReady(&test) || test.DeletionTimestamp != nil || test.Spec.Selector == nil || !reflect.DeepEqual(ref.Selector, test.Spec.Selector.MatchLabels) || len(test.Spec.Selector.MatchExpressions) > 0 || !labelsMatch(ref.Selector, test.Spec.Template.Labels) {
		return errors.New("BLOCKED: protected test sender identity, selector or readiness changed")
	}
	var deployments appsv1.DeploymentList
	if err := r.List(ctx, &deployments, client.InNamespace(r.Namespace)); err != nil {
		return err
	}
	for _, other := range deployments.Items {
		if other.Name != test.Name && labelsMatch(ref.Selector, other.Spec.Template.Labels) {
			return errors.New("BLOCKED: protected test selector also selects another Deployment")
		}
	}
	claims := claimNames(&test)
	if len(claims) == 0 {
		return errors.New("BLOCKED: protected test sender requires its own persistent ledger")
	}
	for _, other := range []*appsv1.Deployment{original, candidate} {
		for name := range claimNames(other) {
			if claims[name] {
				return errors.New("BLOCKED: protected test sender shares a ledger PVC with a formal sender")
			}
		}
	}
	var svc corev1.Service
	if err := r.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: ref.Service}, &svc); err != nil {
		return err
	}
	if string(svc.UID) != ref.ServiceUID || svc.DeletionTimestamp != nil || !reflect.DeepEqual(svc.Spec.Selector, ref.Selector) || net.ParseIP(svc.Spec.ClusterIP) == nil {
		return errors.New("BLOCKED: protected test Service identity or selector changed")
	}
	port := false
	for _, p := range svc.Spec.Ports {
		if p.Port == 19094 && (p.Protocol == "" || p.Protocol == corev1.ProtocolTCP) {
			port = true
		}
	}
	if !port {
		return errors.New("BLOCKED: protected test Service must preserve its test-only 19094 entry")
	}
	pods, err := r.deploymentPods(ctx, &test)
	if err != nil {
		return err
	}
	readyPods := map[string]string{}
	for _, p := range pods {
		ready := false
		for _, condition := range p.Status.Conditions {
			if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
				ready = true
			}
		}
		if ready && p.DeletionTimestamp == nil && net.ParseIP(p.Status.PodIP) != nil {
			readyPods[string(p.UID)] = p.Status.PodIP
		}
	}
	var endpoints corev1.Endpoints
	if err := r.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: ref.Service}, &endpoints); err != nil {
		return err
	}
	matched := map[string]bool{}
	for _, subset := range endpoints.Subsets {
		if len(subset.NotReadyAddresses) > 0 {
			return errors.New("BLOCKED: protected test Service includes unready endpoints")
		}
		testPort := false
		for _, port := range subset.Ports {
			if port.Port == 19094 && (port.Protocol == "" || port.Protocol == corev1.ProtocolTCP) {
				testPort = true
			}
		}
		if len(subset.Addresses) > 0 && !testPort {
			return errors.New("BLOCKED: protected test endpoints do not expose the test-only port")
		}
		for _, address := range subset.Addresses {
			if address.TargetRef == nil || address.TargetRef.Kind != "Pod" || readyPods[string(address.TargetRef.UID)] != address.IP {
				return errors.New("BLOCKED: protected test Service points outside the sealed Ready sender")
			}
			matched[string(address.TargetRef.UID)] = true
		}
	}
	if len(matched) == 0 || len(matched) != len(readyPods) {
		return errors.New("BLOCKED: protected test Service has no complete set of Ready sender endpoints")
	}
	return nil
}
