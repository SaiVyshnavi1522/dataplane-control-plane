package provisioner

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/SaiVyshnavi1522/dataplane-control-plane/internal/model"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func kubernetesFixture() (*Kubernetes, model.Cluster) {
	return &Kubernetes{
		client:       fake.NewClientset(),
		namespace:    "dataplane-clusters",
		image:        "opensearchproject/opensearch:3.8.0",
		storage:      resource.MustParse("2Gi"),
		storageClass: "standard",
		pollInterval: time.Millisecond,
	}, model.Cluster{
		ID:           "01jordersabc1234567890123",
		Name:         "orders-search",
		DesiredNodes: 1,
	}
}

func TestKubernetesReconciliationCreatesPersistentWorkload(t *testing.T) {
	kube, cluster := kubernetesFixture()
	ctx := context.Background()
	if err := kube.ensureNamespace(ctx); err != nil {
		t.Fatalf("reconcile namespace: %v", err)
	}
	if err := kube.applyService(ctx, cluster); err != nil {
		t.Fatalf("reconcile service: %v", err)
	}
	if err := kube.applyStatefulSet(ctx, cluster); err != nil {
		t.Fatalf("reconcile StatefulSet: %v", err)
	}

	statefulSet, err := kube.client.AppsV1().StatefulSets(kube.namespace).Get(ctx, resourceName(cluster), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get StatefulSet: %v", err)
	}
	if len(statefulSet.Spec.VolumeClaimTemplates) != 1 {
		t.Fatalf("volume claim templates=%d, want 1", len(statefulSet.Spec.VolumeClaimTemplates))
	}
	claim := statefulSet.Spec.VolumeClaimTemplates[0]
	size := claim.Spec.Resources.Requests[corev1.ResourceStorage]
	if claim.Name != dataVolumeName || size.Cmp(resource.MustParse("2Gi")) != 0 {
		t.Fatalf("claim name=%s size=%s", claim.Name, size.String())
	}
	if claim.Spec.StorageClassName == nil || *claim.Spec.StorageClassName != "standard" {
		t.Fatalf("storage class=%v", claim.Spec.StorageClassName)
	}
	if statefulSet.Spec.Template.Spec.SecurityContext == nil || statefulSet.Spec.Template.Spec.SecurityContext.FSGroup == nil || *statefulSet.Spec.Template.Spec.SecurityContext.FSGroup != 1000 {
		t.Fatal("pod filesystem security context was not configured")
	}
	if len(statefulSet.Spec.Template.Spec.Volumes) != 0 {
		t.Fatal("pod contains an ephemeral volume instead of a claim template")
	}
}

func TestKubernetesReconciliationCorrectsMutableDrift(t *testing.T) {
	kube, cluster := kubernetesFixture()
	ctx := context.Background()
	if err := kube.ensureNamespace(ctx); err != nil {
		t.Fatal(err)
	}
	if err := kube.applyService(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	if err := kube.applyStatefulSet(ctx, cluster); err != nil {
		t.Fatal(err)
	}

	name := resourceName(cluster)
	statefulSet, _ := kube.client.AppsV1().StatefulSets(kube.namespace).Get(ctx, name, metav1.GetOptions{})
	statefulSet.Spec.Template.Spec.Containers[0].Image = "opensearchproject/opensearch:older"
	_, _ = kube.client.AppsV1().StatefulSets(kube.namespace).Update(ctx, statefulSet, metav1.UpdateOptions{})
	service, _ := kube.client.CoreV1().Services(kube.namespace).Get(ctx, name, metav1.GetOptions{})
	service.Spec.PublishNotReadyAddresses = false
	_, _ = kube.client.CoreV1().Services(kube.namespace).Update(ctx, service, metav1.UpdateOptions{})

	cluster.DesiredNodes = 2
	if err := kube.applyService(ctx, cluster); err != nil {
		t.Fatalf("repair service drift: %v", err)
	}
	if err := kube.applyStatefulSet(ctx, cluster); err != nil {
		t.Fatalf("repair StatefulSet drift: %v", err)
	}
	statefulSet, _ = kube.client.AppsV1().StatefulSets(kube.namespace).Get(ctx, name, metav1.GetOptions{})
	service, _ = kube.client.CoreV1().Services(kube.namespace).Get(ctx, name, metav1.GetOptions{})
	if *statefulSet.Spec.Replicas != 2 || statefulSet.Spec.Template.Spec.Containers[0].Image != kube.image {
		t.Fatalf("StatefulSet drift remains: replicas=%d image=%s", *statefulSet.Spec.Replicas, statefulSet.Spec.Template.Spec.Containers[0].Image)
	}
	if !service.Spec.PublishNotReadyAddresses {
		t.Fatal("service drift remains")
	}
}

func TestKubernetesReconciliationRejectsForeignResource(t *testing.T) {
	kube, cluster := kubernetesFixture()
	foreign := kube.desiredService(cluster)
	foreign.Labels[managedByLabel] = "another-controller"
	_, err := kube.client.CoreV1().Services(kube.namespace).Create(context.Background(), foreign, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := kube.applyService(context.Background(), cluster); !errors.Is(err, ErrResourceOwnership) {
		t.Fatalf("error=%v, want ownership error", err)
	}
}

func TestKubernetesDeletionRemovesManagedClaimsAndService(t *testing.T) {
	kube, cluster := kubernetesFixture()
	ctx := context.Background()
	statefulSet := kube.desiredStatefulSet(cluster)
	service := kube.desiredService(cluster)
	claim := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name:      "data-" + resourceName(cluster) + "-0",
		Namespace: kube.namespace,
		Labels:    managedLabels(cluster),
	}}
	_, _ = kube.client.AppsV1().StatefulSets(kube.namespace).Create(ctx, statefulSet, metav1.CreateOptions{})
	_, _ = kube.client.CoreV1().Services(kube.namespace).Create(ctx, service, metav1.CreateOptions{})
	_, _ = kube.client.CoreV1().PersistentVolumeClaims(kube.namespace).Create(ctx, claim, metav1.CreateOptions{})

	if err := kube.Delete(ctx, cluster); err != nil {
		t.Fatalf("delete resources: %v", err)
	}
	if _, err := kube.client.AppsV1().StatefulSets(kube.namespace).Get(ctx, statefulSet.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("StatefulSet lookup error=%v, want not found", err)
	}
	if _, err := kube.client.CoreV1().Services(kube.namespace).Get(ctx, service.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("Service lookup error=%v, want not found", err)
	}
	if _, err := kube.client.CoreV1().PersistentVolumeClaims(kube.namespace).Get(ctx, claim.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("claim lookup error=%v, want not found", err)
	}
}

func TestKubernetesReadinessRequiresCurrentRevision(t *testing.T) {
	kube, cluster := kubernetesFixture()
	statefulSet := kube.desiredStatefulSet(cluster)
	statefulSet.Generation = 2
	statefulSet.Status = appsv1.StatefulSetStatus{
		ObservedGeneration: 2,
		ReadyReplicas:      1,
		CurrentRevision:    "revision-2",
		UpdateRevision:     "revision-2",
	}
	_, err := kube.client.AppsV1().StatefulSets(kube.namespace).Create(context.Background(), statefulSet, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := kube.waitReady(context.Background(), cluster); err != nil {
		t.Fatalf("wait ready: %v", err)
	}
}
