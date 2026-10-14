package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/kubesphere/notification-manager/apis/v2beta2"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func handoffFixture(t *testing.T, phase string) (*NotificationManagerReconciler, *v2beta2.NotificationManager, SenderHandoff) {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = coordinationv1.AddToScheme(scheme)
	_ = v2beta2.AddToScheme(scheme)
	one := int32(1)
	image := "fixture"
	controller := true
	oldLabels := map[string]string{"app": "notification-manager", "notification-manager": "notification-manager"}
	nm := &v2beta2.NotificationManager{ObjectMeta: metav1.ObjectMeta{Name: "notification-manager", UID: "nm-uid"}, Spec: v2beta2.NotificationManagerSpec{Image: &image, Replicas: &one, Receivers: &v2beta2.ReceiversSpec{}}}
	h := SenderHandoff{OperationID: "promotion-1", Phase: phase, Deployment: "new-nm", DeploymentUID: "new-uid", OriginalDeploymentUID: "old-uid", StableServiceUID: "svc-uid", Selector: map[string]string{"nm-delivery-instance": "candidate"}, StopTimeoutSeconds: 30, MaxGapSeconds: 90}
	if phase != "" {
		raw, _ := json.Marshal(h)
		nm.Annotations = map[string]string{SenderHandoffAnnotation: string(raw)}
	}
	d := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: h.Deployment, Namespace: "monitoring", UID: types.UID(h.DeploymentUID), Generation: 1}, Spec: appsv1.DeploymentSpec{Replicas: &one, Selector: &metav1.LabelSelector{MatchLabels: h.Selector}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: h.Selector}, Spec: corev1.PodSpec{Containers: []corev1.Container{
		{Name: "nm", Image: "fixture", Args: []string{"--webhook.address=127.0.0.1:19096"}, Env: []corev1.EnvVar{{Name: "NM_FORMAL_COMPAT_LISTEN_ADDRESS", Value: "127.0.0.1:19097"}, {Name: "NM_FORMAL_COMPAT_SOURCE_CIDRS", Value: "127.0.0.1/32"}}},
		{Name: "intake-guard", Image: "fixture", ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/-/ready", Port: intstr.FromInt(19094)}}}, Ports: []corev1.ContainerPort{{Name: "webhook", ContainerPort: 19093}, {Name: "test-intake", ContainerPort: 19094}}},
	}}}}, Status: appsv1.DeploymentStatus{ObservedGeneration: 1, ReadyReplicas: 1, UpdatedReplicas: 1}}
	old := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: nm.Name + "-deployment", Namespace: "monitoring", UID: "old-uid", Generation: 1}, Spec: appsv1.DeploymentSpec{Replicas: &one, Selector: &metav1.LabelSelector{MatchLabels: oldLabels}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: oldLabels, Annotations: map[string]string{"installed": "unchanged"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "notification-manager", Image: "rc7", Args: []string{"--installed-options"}}}}}}, Status: appsv1.DeploymentStatus{ObservedGeneration: 1, ReadyReplicas: 1, UpdatedReplicas: 1}}
	owner := func(kind, name string, uid types.UID) []metav1.OwnerReference {
		return []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: kind, Name: name, UID: uid, Controller: &controller}}
	}
	oldRS := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "old-rs", Namespace: "monitoring", UID: "old-rs-uid", OwnerReferences: owner("Deployment", old.Name, old.UID)}}
	newRS := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "new-rs", Namespace: "monitoring", UID: "new-rs-uid", OwnerReferences: owner("Deployment", d.Name, d.UID)}}
	oldPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "old-pod", Namespace: "monitoring", UID: "old-pod-uid", Labels: oldLabels, OwnerReferences: owner("ReplicaSet", oldRS.Name, oldRS.UID)}, Spec: corev1.PodSpec{NodeName: "node1", Containers: old.Spec.Template.Spec.Containers}, Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.11", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}, ContainerStatuses: []corev1.ContainerStatus{{Name: "notification-manager", ContainerID: "containerd://local-fixture-notification-manager", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}}}}}}
	newPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "new-pod", Namespace: "monitoring", UID: "new-pod-uid", Labels: h.Selector, OwnerReferences: owner("ReplicaSet", newRS.Name, newRS.UID)}, Spec: corev1.PodSpec{NodeName: "node1", Containers: d.Spec.Template.Spec.Containers}, Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.12", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	for _, c := range newPod.Spec.Containers {
		newPod.Status.ContainerStatuses = append(newPod.Status.ContainerStatuses, corev1.ContainerStatus{Name: c.Name, ContainerID: "containerd://local-fixture-" + c.Name, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}}})
	}

	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node1", UID: "node-uid"}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}
	holder := "node1"
	renew := metav1.NewMicroTime(time.Now())
	lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: "node1", Namespace: "kube-node-lease", UID: "lease-uid"}, Spec: coordinationv1.LeaseSpec{HolderIdentity: &holder, RenewTime: &renew}}
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: nm.Name + "-svc", Namespace: "monitoring", UID: "svc-uid", Annotations: map[string]string{"keep": "metadata"}}, Spec: corev1.ServiceSpec{ClusterIP: "10.0.0.10", Selector: oldLabels, Ports: []corev1.ServicePort{{Name: "webhook", Port: 19093, TargetPort: intstr.FromString("webhook")}}}}
	r := &NotificationManagerReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(nm, d, old, oldRS, newRS, oldPod, newPod, node, lease, svc).Build(), Namespace: "monitoring", Scheme: scheme, Log: logr.Discard(), FormalReadyProbe: func(context.Context, *corev1.Pod) error { return nil }}
	return r, nm, h
}
func reconcileHandoffTest(r *NotificationManagerReconciler) error {
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "notification-manager"}})
	return err
}
func setPhase(t *testing.T, r *NotificationManagerReconciler, h *SenderHandoff, phase string) {
	t.Helper()
	var nm v2beta2.NotificationManager
	if err := r.Get(context.Background(), client.ObjectKey{Name: "notification-manager"}, &nm); err != nil {
		t.Fatal(err)
	}
	h.Phase = phase
	raw, _ := json.Marshal(h)
	nm.Annotations[SenderHandoffAnnotation] = string(raw)
	if err := r.Update(context.Background(), &nm); err != nil {
		t.Fatal(err)
	}
}
func stateOf(t *testing.T, r *NotificationManagerReconciler) *SenderHandoffState {
	t.Helper()
	var nm v2beta2.NotificationManager
	_ = r.Get(context.Background(), client.ObjectKey{Name: "notification-manager"}, &nm)
	s, err := handoffState(&nm)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func getDep(t *testing.T, r *NotificationManagerReconciler, name string) *appsv1.Deployment {
	t.Helper()
	var d appsv1.Deployment
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: r.Namespace, Name: name}, &d); err != nil {
		t.Fatal(err)
	}
	return &d
}
func normalTerminate(t *testing.T, r *NotificationManagerReconciler, name string, grace int64) {
	t.Helper()
	ctx := context.Background()
	var p corev1.Pod
	if err := r.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: name}, &p); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(ctx, &p); err != nil {
		t.Fatal(err)
	}
	_ = r.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: name}, &p)
	p.DeletionGracePeriodSeconds = &grace
	if err := r.Update(ctx, &p); err != nil {
		t.Fatal(err)
	}
	p.Status.ContainerStatuses = nil
	for _, c := range p.Spec.Containers {
		p.Status.ContainerStatuses = append(p.Status.ContainerStatuses, corev1.ContainerStatus{Name: c.Name, ContainerID: "containerd://local-fixture-" + c.Name, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, Reason: "Completed", FinishedAt: metav1.Now()}}})
	}
	if err := r.Status().Update(ctx, &p); err != nil {
		t.Fatal(err)
	}
}
func stopOld(t *testing.T, r *NotificationManagerReconciler) {
	t.Helper()
	if err := reconcileHandoffTest(r); err != nil {
		t.Fatal(err)
	}
	normalTerminate(t, r, "old-pod", 30)
	for i := 0; i < 3; i++ {
		if err := reconcileHandoffTest(r); err != nil {
			t.Fatal(err)
		}
	}
	if stateOf(t, r).OldExitVerifiedAt == nil {
		t.Fatal("no old termination evidence")
	}
}
func assertSelector(t *testing.T, r *NotificationManagerReconciler, key string) {
	t.Helper()
	var svc corev1.Service
	_ = r.Get(context.Background(), client.ObjectKey{Namespace: r.Namespace, Name: "notification-manager-svc"}, &svc)
	if svc.Spec.Selector[key] == "" {
		t.Fatal("unexpected stable selector", svc.Spec.Selector)
	}
	if svc.Spec.ClusterIP != "10.0.0.10" || svc.UID != "svc-uid" || svc.Annotations["keep"] != "metadata" {
		t.Fatal("lost stable Service identity")
	}
}

func TestManagedStoppedBeforeRetiredAndRestart(t *testing.T) {
	r, _, h := handoffFixture(t, "stopped")
	initial := getDep(t, r, "notification-manager-deployment").DeepCopy()
	if err := reconcileHandoffTest(r); err != nil {
		t.Fatal(err)
	}
	assertSelector(t, r, "notification-manager")
	old := getDep(t, r, initial.Name)
	if *old.Spec.Replicas != 0 {
		t.Fatal("old not held zero")
	}
	initial.Spec.Replicas = old.Spec.Replicas
	if !reflect.DeepEqual(initial.Spec, old.Spec) {
		t.Fatal("stop altered original template")
	}
	setPhase(t, r, &h, "retired")
	if err := reconcileHandoffTest(r); err != nil {
		t.Fatal(err)
	}
	if stateOf(t, r).SwitchedAt != nil {
		t.Fatal("retired accepted running old")
	}
	assertSelector(t, r, "notification-manager")
	setPhase(t, r, &h, "stopped")
	normalTerminate(t, r, "old-pod", 30)
	for i := 0; i < 3; i++ {
		if err := reconcileHandoffTest(r); err != nil {
			t.Fatal(err)
		}
	}
	// Restart the reconciler instance; state is in the original CR, never memory.
	r = &NotificationManagerReconciler{Client: r.Client, Namespace: r.Namespace, Scheme: r.Scheme, Log: logr.Discard(), FormalReadyProbe: r.FormalReadyProbe}
	setPhase(t, r, &h, "retired")
	for i := 0; i < 2; i++ {
		if err := reconcileHandoffTest(r); err != nil {
			t.Fatal(err)
		}
	}
	assertSelector(t, r, "nm-delivery-instance")
	if *getDep(t, r, initial.Name).Spec.Replicas != 0 {
		t.Fatal("reconcile resurrected original")
	}
	s := stateOf(t, r)
	if s.Stage != "retired" || s.OldPods[0].TerminatedAt == nil || s.LossCount != nil {
		t.Fatal("false stop/loss evidence", s)
	}
}
func TestManagedNormalExitRejectsUnknownNodeForcedAndMissingPod(t *testing.T) {
	for _, kind := range []string{"node-notready", "stale-lease", "node-uid", "forced", "missing"} {
		t.Run(kind, func(t *testing.T) {
			r, _, h := handoffFixture(t, "stopped")
			if err := reconcileHandoffTest(r); err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			switch kind {
			case "node-notready":
				var n corev1.Node
				_ = r.Get(ctx, client.ObjectKey{Name: "node1"}, &n)
				n.Status.Conditions[0].Status = corev1.ConditionUnknown
				_ = r.Status().Update(ctx, &n)
			case "stale-lease":
				var l coordinationv1.Lease
				_ = r.Get(ctx, client.ObjectKey{Namespace: "kube-node-lease", Name: "node1"}, &l)
				past := metav1.NewMicroTime(time.Now().Add(-2 * time.Minute))
				l.Spec.RenewTime = &past
				_ = r.Update(ctx, &l)
			case "node-uid":
				var n corev1.Node
				_ = r.Get(ctx, client.ObjectKey{Name: "node1"}, &n)
				n.UID = "replacement-node"
				_ = r.Update(ctx, &n)
			case "forced":
				var p corev1.Pod
				_ = r.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: "old-pod"}, &p)
				zero := int64(0)
				p.DeletionGracePeriodSeconds = &zero
				_ = r.Update(ctx, &p)
				var live v2beta2.NotificationManager
				_ = r.Get(ctx, client.ObjectKey{Name: "notification-manager"}, &live)
				state, _ := handoffState(&live)
				state.OldPods[0].NormalDeleteRequestedAt = nil
				state.OldPods[0].NormalDeleteResult = ""
				_ = r.saveState(ctx, &live, state)
			case "missing":
				var p corev1.Pod
				_ = r.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: "old-pod"}, &p)
				p.Finalizers = nil
				_ = r.Update(ctx, &p)
				_ = r.Delete(ctx, &p)
			}
			setPhase(t, r, &h, "retired")
			if err := reconcileHandoffTest(r); err == nil {
				t.Fatal("unsafe process state accepted")
			}
			assertSelector(t, r, "notification-manager")
			if stateOf(t, r).OldExitVerifiedAt != nil {
				t.Fatal("unknown/force status fabricated exit")
			}
		})
	}
}
func TestManagedRollbackStopsCandidateBeforeOldAndKeepsFence(t *testing.T) {
	r, _, h := handoffFixture(t, "stopped")
	stopOld(t, r)
	setPhase(t, r, &h, "retired")
	if err := reconcileHandoffTest(r); err != nil {
		t.Fatal(err)
	}
	setPhase(t, r, &h, "rollback")
	if err := reconcileHandoffTest(r); err != nil {
		t.Fatal(err)
	}
	if *getDep(t, r, "notification-manager-deployment").Spec.Replicas != 0 {
		t.Fatal("old restored while candidate still live")
	}
	d := getDep(t, r, h.Deployment)
	if *d.Spec.Replicas != 0 || !formalDisabled(d) || d.Annotations[FormalSenderFenceAnnotation] != h.OperationID {
		t.Fatal("candidate stop fence missing")
	}
	assertSelector(t, r, "nm-delivery-instance")
	normalTerminate(t, r, "new-pod", 30)
	for i := 0; i < 3; i++ {
		if err := reconcileHandoffTest(r); err != nil {
			t.Fatal(err)
		}
	}
	if stateOf(t, r).CandidateExitVerifiedAt == nil {
		t.Fatal("candidate exit missing")
	}
	if *getDep(t, r, "notification-manager-deployment").Spec.Replicas != 1 {
		t.Fatal("old not restored after candidate exit")
	}
	// Another controller tries to restore candidate replicas. Persistent intent wins.
	d = getDep(t, r, h.Deployment)
	one := int32(1)
	d.Spec.Replicas = &one
	_ = r.Update(context.Background(), d)
	if err := reconcileHandoffTest(r); err != nil {
		t.Fatal(err)
	}
	if *getDep(t, r, h.Deployment).Spec.Replicas != 0 {
		t.Fatal("candidate resurrected by external replicas")
	}
	assertSelector(t, r, "notification-manager")
	if stateOf(t, r).RolledBackAt == nil {
		t.Fatal("rollback completion missing")
	}
}
func TestManagedEmptyIntentUpgradePreservesInstalledWorkload(t *testing.T) {
	r, nm, _ := handoffFixture(t, "")
	before := getDep(t, r, "notification-manager-deployment").DeepCopy()
	var beforeSvc corev1.Service
	_ = r.Get(context.Background(), client.ObjectKey{Namespace: r.Namespace, Name: nm.Name + "-svc"}, &beforeSvc)
	for i := 0; i < 3; i++ {
		if err := reconcileHandoffTest(r); err != nil {
			t.Fatal(err)
		}
	}
	after := getDep(t, r, before.Name)
	if !reflect.DeepEqual(before.Spec, after.Spec) || before.ResourceVersion != after.ResourceVersion {
		t.Fatal("empty upgrade rewrote original Deployment")
	}
	var afterSvc corev1.Service
	_ = r.Get(context.Background(), client.ObjectKey{Namespace: r.Namespace, Name: nm.Name + "-svc"}, &afterSvc)
	if !reflect.DeepEqual(beforeSvc, afterSvc) {
		t.Fatal("empty upgrade changed original Service")
	}
}
func TestManagedRejectsTestOnlyReadinessAndNotReadyCandidate(t *testing.T) {
	for _, kind := range []string{"formal-disabled", "guard-missing", "public-private-listener", "uid", "selector", "notready"} {
		t.Run(kind, func(t *testing.T) {
			r, _, h := handoffFixture(t, "stopped")
			d := getDep(t, r, h.Deployment)
			switch kind {
			case "formal-disabled":
				r.FormalReadyProbe = func(context.Context, *corev1.Pod) error { return errors.New("formal state inactive") }
			case "guard-missing":
				d.Spec.Template.Spec.Containers = d.Spec.Template.Spec.Containers[:1]
			case "public-private-listener":
				d.Spec.Template.Spec.Containers[0].Env[0].Value = ":19097"
			case "uid":
				d.UID = "unexpected"
			case "selector":
				h.Selector = map[string]string{"app": "notification-manager", "notification-manager": "notification-manager"}
				setPhase(t, r, &h, "stopped")
			case "notready":
				d.Status.ReadyReplicas = 0
				_ = r.Status().Update(context.Background(), d)
			}
			if kind != "notready" {
				_ = r.Update(context.Background(), d)
			}
			if err := reconcileHandoffTest(r); err == nil {
				t.Fatal("unsafe candidate accepted")
			}
			if *getDep(t, r, "notification-manager-deployment").Spec.Replicas != 1 {
				t.Fatal("invalid candidate stopped original")
			}
			assertSelector(t, r, "notification-manager")
		})
	}
}
func TestManagedBoundedStopAndIdentityCannotBeChanged(t *testing.T) {
	r, _, h := handoffFixture(t, "stopped")
	if err := reconcileHandoffTest(r); err != nil {
		t.Fatal(err)
	}
	var nm v2beta2.NotificationManager
	_ = r.Get(context.Background(), client.ObjectKey{Name: "notification-manager"}, &nm)
	s, _ := handoffState(&nm)
	past := metav1.NewTime(time.Now().Add(-time.Minute))
	s.OldStopRequestedAt = &past
	_ = r.saveState(context.Background(), &nm, s)
	if err := reconcileHandoffTest(r); err == nil || !strings.Contains(err.Error(), "timeout") {
		t.Fatal("unbounded stop", err)
	}
	assertSelector(t, r, "notification-manager")
	h.OperationID = "new-operation"
	setPhase(t, r, &h, "retired")
	if err := reconcileHandoffTest(r); err == nil {
		t.Fatal("operation replaced sealed evidence")
	}
}
func TestManagedDeletedOriginalIsNotResurrected(t *testing.T) {
	r, _, h := handoffFixture(t, "stopped")
	stopOld(t, r)
	old := getDep(t, r, "notification-manager-deployment")
	_ = r.Delete(context.Background(), old)
	setPhase(t, r, &h, "retired")
	if err := reconcileHandoffTest(r); err == nil {
		t.Fatal("deleted original fabricated")
	}
	var d appsv1.Deployment
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: r.Namespace, Name: old.Name}, &d); err == nil {
		t.Fatal("retired sender Deployment recreated")
	}
}
func TestManagedRemovingIntentCannotReleasePersistentFence(t *testing.T) {
	r, nm, h := handoffFixture(t, "stopped")
	stopOld(t, r)
	setPhase(t, r, &h, "retired")
	_ = reconcileHandoffTest(r)
	_ = r.Get(context.Background(), client.ObjectKey{Name: nm.Name}, nm)
	delete(nm.Annotations, SenderHandoffAnnotation)
	_ = r.Update(context.Background(), nm)
	if err := reconcileHandoffTest(r); err == nil {
		t.Fatal("removed intent implied rollback")
	}
	if *getDep(t, r, "notification-manager-deployment").Spec.Replicas != 0 {
		t.Fatal("old resurrected")
	}
	assertSelector(t, r, "nm-delivery-instance")
}
func TestManagedTestRestoreRequiresDurableStartupFence(t *testing.T) {
	r, _, h := handoffFixture(t, "stopped")
	stopOld(t, r)
	setPhase(t, r, &h, "retired")
	_ = reconcileHandoffTest(r)
	setPhase(t, r, &h, "rollback")
	_ = reconcileHandoffTest(r)
	normalTerminate(t, r, "new-pod", 30)
	for i := 0; i < 3; i++ {
		_ = reconcileHandoffTest(r)
	}
	setPhase(t, r, &h, "test-restored")
	d := getDep(t, r, h.Deployment)
	d.Spec.Template.Spec.Containers[0].Env = d.Spec.Template.Spec.Containers[0].Env[:2]
	_ = r.Update(context.Background(), d)
	if err := reconcileHandoffTest(r); err == nil {
		t.Fatal("test restore unfenced")
	}
	setFormalFence(d, h.OperationID)
	_ = r.Update(context.Background(), d)
	if err := reconcileHandoffTest(r); err != nil {
		t.Fatal(err)
	}
	if *getDep(t, r, h.Deployment).Spec.Replicas != 1 || !formalDisabled(getDep(t, r, h.Deployment)) {
		t.Fatal("test-only restart failed")
	}
	assertSelector(t, r, "notification-manager")
}

// A response can be lost after the Kubernetes API has accepted the selector
// mutation. Retrying must read that operation, never reconstruct its time.
type lostServiceResponseClient struct {
	client.Client
	lost bool
}

func (c *lostServiceResponseClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if err := c.Client.Update(ctx, obj, opts...); err != nil {
		return err
	}
	if svc, ok := obj.(*corev1.Service); ok && !c.lost && strings.HasSuffix(svc.Annotations[SenderHandoffAnnotation], ":retired") {
		c.lost = true
		return errors.New("simulated lost Service response")
	}
	return nil
}
func TestManagedServiceResponseLossRecoversAppliedOperation(t *testing.T) {
	r, _, h := handoffFixture(t, "stopped")
	stopOld(t, r)
	r.Client = &lostServiceResponseClient{Client: r.Client}
	setPhase(t, r, &h, "retired")
	if err := reconcileHandoffTest(r); err == nil {
		t.Fatal("lost response not exercised")
	}
	assertSelector(t, r, "nm-delivery-instance")
	if stateOf(t, r).SwitchedAt != nil {
		t.Fatal("failed response invented state completion")
	}
	var svc corev1.Service
	_ = r.Get(context.Background(), client.ObjectKey{Namespace: r.Namespace, Name: "notification-manager-svc"}, &svc)
	applied := svc.Annotations[SenderHandoffAppliedAtAnnotation]
	if err := reconcileHandoffTest(r); err != nil {
		t.Fatal(err)
	}
	s := stateOf(t, r)
	want, _ := time.Parse(time.RFC3339Nano, applied)
	if s.SwitchedAt == nil || !s.SwitchedAt.Time.Equal(want) {
		t.Fatal("did not reuse persisted actual selector application timestamp")
	}
}
func TestManagedServiceReplacementAndDirectRetiredRejected(t *testing.T) {
	r, _, _ := handoffFixture(t, "retired")
	if err := reconcileHandoffTest(r); err == nil {
		t.Fatal("retired allowed without managed stop evidence")
	}
	if *getDep(t, r, "notification-manager-deployment").Spec.Replicas != 1 {
		t.Fatal("invalid direct retired stopped original")
	}
	r, _, _ = handoffFixture(t, "stopped")
	var svc corev1.Service
	_ = r.Get(context.Background(), client.ObjectKey{Namespace: r.Namespace, Name: "notification-manager-svc"}, &svc)
	svc.UID = "replacement"
	_ = r.Update(context.Background(), &svc)
	if err := reconcileHandoffTest(r); err == nil {
		t.Fatal("replacement Service allowed")
	}
	if *getDep(t, r, "notification-manager-deployment").Spec.Replicas != 1 {
		t.Fatal("service replacement stopped original")
	}
}

func TestManagedRollbackDuringStopFinishesBothProtectedTerminations(t *testing.T) {
	r, _, h := handoffFixture(t, "stopped")
	if err := reconcileHandoffTest(r); err != nil {
		t.Fatal(err)
	}
	setPhase(t, r, &h, "rollback")
	if err := reconcileHandoffTest(r); err != nil {
		t.Fatal(err)
	}
	normalTerminate(t, r, "new-pod", 30)
	for i := 0; i < 3; i++ {
		if err := reconcileHandoffTest(r); err != nil {
			t.Fatal(err)
		}
	}
	if *getDep(t, r, "notification-manager-deployment").Spec.Replicas != 0 {
		t.Fatal("rollback abandoned original protected termination")
	}
	normalTerminate(t, r, "old-pod", 30)
	for i := 0; i < 3; i++ {
		if err := reconcileHandoffTest(r); err != nil {
			t.Fatal(err)
		}
	}
	if stateOf(t, r).OldExitVerifiedAt == nil || stateOf(t, r).CandidateExitVerifiedAt == nil {
		t.Fatal("rollback stop evidence missing")
	}
	assertSelector(t, r, "notification-manager")
	var pods corev1.PodList
	_ = r.List(context.Background(), &pods, client.InNamespace(r.Namespace))
	for _, p := range pods.Items {
		if len(p.Finalizers) > 0 {
			t.Fatal("protected terminated Pod leaked", p.Name)
		}
	}
}

func TestManagedUnsealedSenderProcessRemainsBlockedAfterObjectDisappears(t *testing.T) {
	r, _, h := handoffFixture(t, "stopped")
	if err := reconcileHandoffTest(r); err != nil {
		t.Fatal(err)
	}
	var original corev1.Pod
	_ = r.Get(context.Background(), client.ObjectKey{Namespace: r.Namespace, Name: "old-pod"}, &original)
	extra := original.DeepCopy()
	extra.Name = "unsealed-old-pod"
	extra.UID = "unsealed-old-uid"
	extra.ResourceVersion = ""
	extra.Finalizers = nil
	if err := r.Create(context.Background(), extra); err != nil {
		t.Fatal(err)
	}
	if err := reconcileHandoffTest(r); err == nil {
		t.Fatal("unsealed sender was accepted")
	}
	s := stateOf(t, r)
	if s.ProcessUncertainty == "" || len(s.UnexpectedPods) != 1 {
		t.Fatal("unsealed process uncertainty not persisted")
	}
	if err := r.Delete(context.Background(), extra); err != nil {
		t.Fatal(err)
	}
	normalTerminate(t, r, "old-pod", 30)
	setPhase(t, r, &h, "retired")
	for i := 0; i < 2; i++ {
		if err := reconcileHandoffTest(r); err == nil {
			t.Fatal("forgot unsealed process after API disappearance")
		}
	}
	assertSelector(t, r, "notification-manager")
}

func TestManagedStaleStoppedIntentCannotRevertRetiredBackend(t *testing.T) {
	r, _, h := handoffFixture(t, "stopped")
	stopOld(t, r)
	setPhase(t, r, &h, "retired")
	if err := reconcileHandoffTest(r); err != nil {
		t.Fatal(err)
	}
	setPhase(t, r, &h, "stopped")
	if err := reconcileHandoffTest(r); err == nil {
		t.Fatal("stale stopped reverted a retired operation")
	}
	assertSelector(t, r, "nm-delivery-instance")
	if stateOf(t, r).Stage != "retired" {
		t.Fatal("stale intent changed persistent stage")
	}
}

func TestManagedRollbackWaitsForSealedExecutorAndKeepsItsFence(t *testing.T) {
	r, _, h := handoffFixture(t, "stopped")
	ctx := context.Background()
	one := int32(1)
	controller := true
	h.ExecutorDeployment = "fixture-executor"
	h.ExecutorDeploymentUID = "executor-uid"
	labels := map[string]string{"fixture-executor": "independent"}
	d := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: h.ExecutorDeployment, Namespace: r.Namespace, UID: types.UID(h.ExecutorDeploymentUID)}, Spec: appsv1.DeploymentSpec{Replicas: &one, Selector: &metav1.LabelSelector{MatchLabels: labels}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "executor", Image: "fixture"}}}}}}
	rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "executor-rs", Namespace: r.Namespace, UID: "executor-rs-uid", OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: d.Name, UID: d.UID, Controller: &controller}}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "executor-pod", Namespace: r.Namespace, UID: "executor-pod-uid", Labels: labels, OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID, Controller: &controller}}}, Spec: corev1.PodSpec{NodeName: "node1", Containers: d.Spec.Template.Spec.Containers}}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "executor", ContainerID: "containerd://local-fixture-executor", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}}}}

	for _, obj := range []client.Object{d, rs, pod} {
		if err := r.Create(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}
	setPhase(t, r, &h, "stopped")
	stopOld(t, r)
	setPhase(t, r, &h, "retired")
	if err := reconcileHandoffTest(r); err != nil {
		t.Fatal(err)
	}
	setPhase(t, r, &h, "rollback")
	if err := reconcileHandoffTest(r); err != nil {
		t.Fatal(err)
	}
	if *getDep(t, r, h.ExecutorDeployment).Spec.Replicas != 0 || *getDep(t, r, h.Deployment).Spec.Replicas != 0 || *getDep(t, r, "notification-manager-deployment").Spec.Replicas != 0 {
		t.Fatal("rollback failed to hold all senders and writer stopped")
	}
	normalTerminate(t, r, "new-pod", 30)
	for i := 0; i < 3; i++ {
		if err := reconcileHandoffTest(r); err != nil {
			t.Fatal(err)
		}
	}
	if *getDep(t, r, "notification-manager-deployment").Spec.Replicas != 0 {
		t.Fatal("old resumed while executor process remained live")
	}
	normalTerminate(t, r, "executor-pod", 30)
	for i := 0; i < 6; i++ {
		if err := reconcileHandoffTest(r); err != nil {
			t.Fatal(err)
		}
	}
	s := stateOf(t, r)
	if s.ExecutorExitVerifiedAt == nil || len(s.ExecutorPods) != 1 || s.ExecutorPods[0].TerminatedAt == nil || !s.ExecutorFenced {
		t.Fatal("executor stop proof/fence missing")
	}
	assertSelector(t, r, "notification-manager")
	d = getDep(t, r, h.ExecutorDeployment)
	if d.Spec.Template.Spec.Containers[0].Env[0].Name != "JEV_FORMAL_WRITER_DISABLED" || d.Spec.Template.Spec.Containers[0].Env[0].Value != "true" {
		t.Fatal("executor formal startup fence missing")
	}
	d.Spec.Replicas = &one
	_ = r.Update(ctx, d)
	if err := reconcileHandoffTest(r); err != nil {
		t.Fatal(err)
	}
	if *getDep(t, r, h.ExecutorDeployment).Spec.Replicas != 0 {
		t.Fatal("executor resurrected")
	}
	setPhase(t, r, &h, "test-restored")
	if err := reconcileHandoffTest(r); err != nil {
		t.Fatal(err)
	}
	if *getDep(t, r, h.ExecutorDeployment).Spec.Replicas != 0 {
		t.Fatal("executor automatically restarted with candidate test")
	}
}

func TestManagedKubeletTerminalZeroGraceUsesSealedPlanAndRuntimeWitness(t *testing.T) {
	r, _, h := handoffFixture(t, "stopped")
	if err := reconcileHandoffTest(r); err != nil {
		t.Fatal(err)
	}
	before := stateOf(t, r)
	e := before.OldPods[0]
	if e.ManagedNormalStopIntentAt == nil || e.ConfiguredGraceSeconds <= 0 || e.NormalDeleteResult != "acknowledged" {
		t.Fatal("normal intent was not durable before stop")
	}
	normalTerminate(t, r, "old-pod", 0)
	for i := 0; i < 3; i++ {
		if err := reconcileHandoffTest(r); err != nil {
			t.Fatal(err)
		}
	}
	setPhase(t, r, &h, "retired")
	if err := reconcileHandoffTest(r); err != nil {
		t.Fatal(err)
	}
	e = stateOf(t, r).OldPods[0]
	if e.TerminatedAt == nil || e.ObservedGraceSeconds == nil || *e.ObservedGraceSeconds != 0 || e.NormalGraceSeconds != 30 || e.ContainerExits[0].ContainerID != e.ContainerIDs["notification-manager"] {
		t.Fatal("zero-grace kubelet completion lacks independent runtime proof")
	}
	assertSelector(t, r, "nm-delivery-instance")
}

type deleteRecoveryClient struct {
	client.Client
	kind  string
	calls int
	first bool
}

func (c *deleteRecoveryClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	c.calls++
	options := client.DeleteOptions{}
	options.ApplyOptions(opts)
	if options.GracePeriodSeconds == nil || *options.GracePeriodSeconds <= 0 || options.Preconditions == nil || options.Preconditions.UID == nil || options.Preconditions.ResourceVersion == nil {
		return errors.New("missing positive grace/UID/RV delete preconditions")
	}
	if !c.first {
		c.first = true
		if c.kind == "conflict" {
			return apierrors.NewConflict(schema.GroupResource{Resource: "pods"}, obj.GetName(), errors.New("synthetic concurrent RV change"))
		}
	}
	if err := c.Client.Delete(ctx, obj, opts...); err != nil {
		return err
	}
	if c.kind == "response-loss" && c.calls == 1 {
		return errors.New("synthetic Delete response lost after API accepted")
	}
	return nil
}
func TestManagedNormalDeleteConflictAndResponseLossRestartRecovery(t *testing.T) {
	for _, kind := range []string{"conflict", "response-loss"} {
		t.Run(kind, func(t *testing.T) {
			r, _, _ := handoffFixture(t, "stopped")
			wrapped := &deleteRecoveryClient{Client: r.Client, kind: kind}
			r.Client = wrapped
			err := reconcileHandoffTest(r)
			if kind == "conflict" && err == nil {
				t.Fatal("conflict path untested")
			}
			if kind == "response-loss" && err != nil {
				t.Fatal(err)
			}
			oldRequest := stateOf(t, r).OldStopRequestedAt.DeepCopy()
			r = &NotificationManagerReconciler{Client: r.Client, Namespace: r.Namespace, Scheme: r.Scheme, Log: logr.Discard(), FormalReadyProbe: r.FormalReadyProbe}
			if kind == "conflict" {
				if err := reconcileHandoffTest(r); err != nil {
					t.Fatal(err)
				}
			}
			var pod corev1.Pod
			_ = r.Get(context.Background(), client.ObjectKey{Namespace: r.Namespace, Name: "old-pod"}, &pod)
			zero := int64(0)
			pod.DeletionGracePeriodSeconds = &zero
			_ = r.Update(context.Background(), &pod)
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "notification-manager", ContainerID: "containerd://local-fixture-notification-manager", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, FinishedAt: metav1.Now()}}}}
			_ = r.Status().Update(context.Background(), &pod)
			for i := 0; i < 3; i++ {
				if err := reconcileHandoffTest(r); err != nil {
					t.Fatal(err)
				}
			}
			s := stateOf(t, r)
			if s.OldExitVerifiedAt == nil || !s.OldStopRequestedAt.Equal(oldRequest) {
				t.Fatal("restart forgot verified exit or reset gap clock")
			}
			if kind == "response-loss" && (wrapped.calls != 1 || s.OldPods[0].NormalDeleteResult != "response-unknown") {
				t.Fatal("ambiguous Delete blindly resent or mislabeled ACK")
			}
		})
	}
}
func TestManagedRuntimeContainerReplacementCannotBecomeExitProof(t *testing.T) {
	r, _, h := handoffFixture(t, "stopped")
	if err := reconcileHandoffTest(r); err != nil {
		t.Fatal(err)
	}
	normalTerminate(t, r, "old-pod", 0)
	var p corev1.Pod
	_ = r.Get(context.Background(), client.ObjectKey{Namespace: r.Namespace, Name: "old-pod"}, &p)
	p.Status.ContainerStatuses[0].ContainerID = "containerd://unsealed-replacement"
	_ = r.Status().Update(context.Background(), &p)
	setPhase(t, r, &h, "retired")
	if err := reconcileHandoffTest(r); err == nil {
		t.Fatal("replacement container was treated as sealed process")
	}
	if stateOf(t, r).ProcessUncertainty == "" {
		t.Fatal("container replacement uncertainty not durable")
	}
	assertSelector(t, r, "notification-manager")
}
