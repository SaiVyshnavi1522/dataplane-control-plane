package provisioner

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/SaiVyshnavi1522/dataplane-control-plane/internal/model"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	managedByLabel = "app.kubernetes.io/managed-by"
	clusterLabel   = "dataplane.io/cluster"
	managedByValue = "dataplane-control-plane"
	dataVolumeName = "data"
)

var ErrResourceOwnership = errors.New("Kubernetes resource is not owned by this cluster")

type KubernetesOptions struct {
	Namespace    string
	Image        string
	StorageSize  string
	StorageClass string
}

type Kubernetes struct {
	client       kubernetes.Interface
	namespace    string
	image        string
	storage      resource.Quantity
	storageClass string
	pollInterval time.Duration
}

func NewKubernetes(kubeconfig string, options KubernetesOptions) (*Kubernetes, error) {
	storage, err := resource.ParseQuantity(options.StorageSize)
	if err != nil || storage.Sign() <= 0 {
		return nil, fmt.Errorf("OPENSEARCH_STORAGE_SIZE must be a positive Kubernetes quantity: %q", options.StorageSize)
	}
	cfg, err := kubeConfig(kubeconfig)
	if err != nil {
		return nil, err
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &Kubernetes{
		client:       client,
		namespace:    options.Namespace,
		image:        options.Image,
		storage:      storage,
		storageClass: options.StorageClass,
		pollInterval: 5 * time.Second,
	}, nil
}

func kubeConfig(path string) (*rest.Config, error) {
	if path == "" {
		if cfg, err := rest.InClusterConfig(); err == nil {
			return cfg, nil
		}
		return clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
	}
	return clientcmd.BuildConfigFromFlags("", path)
}

func (k *Kubernetes) Provision(ctx context.Context, cluster model.Cluster) error {
	if err := k.ensureNamespace(ctx); err != nil {
		return fmt.Errorf("reconcile namespace: %w", err)
	}
	if err := k.applyService(ctx, cluster); err != nil {
		return fmt.Errorf("reconcile service: %w", err)
	}
	if err := k.applyStatefulSet(ctx, cluster); err != nil {
		return fmt.Errorf("reconcile StatefulSet: %w", err)
	}
	return k.waitReady(ctx, cluster)
}

func (k *Kubernetes) Scale(ctx context.Context, cluster model.Cluster) error {
	if err := k.applyService(ctx, cluster); err != nil {
		return fmt.Errorf("reconcile service: %w", err)
	}
	if err := k.applyStatefulSet(ctx, cluster); err != nil {
		return fmt.Errorf("reconcile StatefulSet: %w", err)
	}
	return k.waitReady(ctx, cluster)
}

func (k *Kubernetes) Delete(ctx context.Context, cluster model.Cluster) error {
	name := resourceName(cluster)
	policy := metav1.DeletePropagationForeground
	if err := k.client.AppsV1().StatefulSets(k.namespace).Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &policy}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete StatefulSet: %w", err)
	}
	if err := k.waitStatefulSetDeleted(ctx, name); err != nil {
		return err
	}

	selector := labels.SelectorFromSet(map[string]string{clusterLabel: cluster.ID}).String()
	claims, err := k.client.CoreV1().PersistentVolumeClaims(k.namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return fmt.Errorf("list persistent volume claims: %w", err)
	}
	for index := range claims.Items {
		claim := &claims.Items[index]
		if !ownedBy(claim.Labels, cluster.ID) {
			return fmt.Errorf("%w: PersistentVolumeClaim %s/%s", ErrResourceOwnership, k.namespace, claim.Name)
		}
		if err := k.client.CoreV1().PersistentVolumeClaims(k.namespace).Delete(ctx, claim.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete PersistentVolumeClaim %s: %w", claim.Name, err)
		}
	}
	if err := k.client.CoreV1().Services(k.namespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete Service: %w", err)
	}
	return nil
}

func (k *Kubernetes) ensureNamespace(ctx context.Context) error {
	namespaces := k.client.CoreV1().Namespaces()
	existing, err := namespaces.Get(ctx, k.namespace, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = namespaces.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
			Name:   k.namespace,
			Labels: map[string]string{managedByLabel: managedByValue},
		}}, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	before := existing.DeepCopy()
	existing.Labels = mergeLabels(existing.Labels, map[string]string{managedByLabel: managedByValue})
	if reflect.DeepEqual(before.Labels, existing.Labels) {
		return nil
	}
	_, err = namespaces.Update(ctx, existing, metav1.UpdateOptions{})
	return err
}

func (k *Kubernetes) applyService(ctx context.Context, cluster model.Cluster) error {
	desired := k.desiredService(cluster)
	services := k.client.CoreV1().Services(k.namespace)
	existing, err := services.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = services.Create(ctx, desired, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	if !ownedBy(existing.Labels, cluster.ID) {
		return fmt.Errorf("%w: Service %s/%s", ErrResourceOwnership, k.namespace, desired.Name)
	}
	before := existing.DeepCopy()
	existing.Labels = mergeLabels(existing.Labels, desired.Labels)
	existing.Spec.PublishNotReadyAddresses = desired.Spec.PublishNotReadyAddresses
	existing.Spec.Selector = desired.Spec.Selector
	existing.Spec.Ports = desired.Spec.Ports
	if reflect.DeepEqual(before, existing) {
		return nil
	}
	_, err = services.Update(ctx, existing, metav1.UpdateOptions{})
	return err
}

func (k *Kubernetes) desiredService(cluster model.Cluster) *corev1.Service {
	name := resourceName(cluster)
	resourceLabels := managedLabels(cluster)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: k.namespace, Labels: resourceLabels},
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: true,
			Selector:                 resourceLabels,
			Ports: []corev1.ServicePort{
				{Name: "http", Port: 9200, TargetPort: intstr.FromInt(9200)},
				{Name: "transport", Port: 9300, TargetPort: intstr.FromInt(9300)},
			},
		},
	}
}

func (k *Kubernetes) applyStatefulSet(ctx context.Context, cluster model.Cluster) error {
	desired := k.desiredStatefulSet(cluster)
	statefulSets := k.client.AppsV1().StatefulSets(k.namespace)
	existing, err := statefulSets.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = statefulSets.Create(ctx, desired, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	if !ownedBy(existing.Labels, cluster.ID) {
		return fmt.Errorf("%w: StatefulSet %s/%s", ErrResourceOwnership, k.namespace, desired.Name)
	}
	if err := validateStorageTemplate(existing, desired); err != nil {
		return err
	}

	before := existing.DeepCopy()
	existing.Labels = mergeLabels(existing.Labels, desired.Labels)
	existing.Spec.Replicas = desired.Spec.Replicas
	existing.Spec.Template = desired.Spec.Template
	existing.Spec.UpdateStrategy = desired.Spec.UpdateStrategy
	existing.Spec.PodManagementPolicy = desired.Spec.PodManagementPolicy
	existing.Spec.PersistentVolumeClaimRetentionPolicy = desired.Spec.PersistentVolumeClaimRetentionPolicy
	if reflect.DeepEqual(before, existing) {
		return nil
	}
	_, err = statefulSets.Update(ctx, existing, metav1.UpdateOptions{})
	return err
}

func (k *Kubernetes) desiredStatefulSet(cluster model.Cluster) *appsv1.StatefulSet {
	name := resourceName(cluster)
	resourceLabels := managedLabels(cluster)
	replicas := int32(cluster.DesiredNodes)
	retention := appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
		WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
		WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
	}
	podSecurity := int64(1000)
	claim := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: dataVolumeName, Labels: resourceLabels},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: k.storage}},
		},
	}
	if k.storageClass != "" {
		claim.Spec.StorageClassName = &k.storageClass
	}
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: k.namespace, Labels: resourceLabels},
		Spec: appsv1.StatefulSetSpec{
			ServiceName:                          name,
			Replicas:                             &replicas,
			Selector:                             &metav1.LabelSelector{MatchLabels: resourceLabels},
			PodManagementPolicy:                  appsv1.ParallelPodManagement,
			UpdateStrategy:                       appsv1.StatefulSetUpdateStrategy{Type: appsv1.RollingUpdateStatefulSetStrategyType},
			PersistentVolumeClaimRetentionPolicy: &retention,
			VolumeClaimTemplates:                 []corev1.PersistentVolumeClaim{claim},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: resourceLabels},
				Spec: corev1.PodSpec{
					SecurityContext: &corev1.PodSecurityContext{
						RunAsUser:    &podSecurity,
						RunAsGroup:   &podSecurity,
						FSGroup:      &podSecurity,
						RunAsNonRoot: boolPointer(true),
					},
					TerminationGracePeriodSeconds: int64Pointer(30),
					Containers: []corev1.Container{{
						Name:  "opensearch",
						Image: k.image,
						Ports: []corev1.ContainerPort{
							{Name: "http", ContainerPort: 9200},
							{Name: "transport", ContainerPort: 9300},
							{Name: "metrics", ContainerPort: 9600},
						},
						Env: []corev1.EnvVar{
							{Name: "cluster.name", Value: name},
							{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
							{Name: "node.name", Value: "$(POD_NAME)"},
							{Name: "discovery.seed_hosts", Value: name},
							{Name: "cluster.initial_cluster_manager_nodes", Value: name + "-0"},
							{Name: "DISABLE_SECURITY_PLUGIN", Value: "true"},
							{Name: "OPENSEARCH_JAVA_OPTS", Value: "-Xms512m -Xmx512m"},
						},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m"), corev1.ResourceMemory: resource.MustParse("768Mi")},
							Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1536Mi")},
						},
						VolumeMounts: []corev1.VolumeMount{{Name: dataVolumeName, MountPath: "/usr/share/opensearch/data"}},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler:        corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/_cluster/health", Port: intstr.FromInt(9200)}},
							InitialDelaySeconds: 25,
							PeriodSeconds:       10,
							TimeoutSeconds:      3,
							FailureThreshold:    12,
						},
					}},
				},
			},
		},
	}
}

func (k *Kubernetes) waitReady(ctx context.Context, cluster model.Cluster) error {
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	ticker := time.NewTicker(k.pollInterval)
	defer ticker.Stop()
	name := resourceName(cluster)
	var lastError error
	for {
		statefulSet, err := k.client.AppsV1().StatefulSets(k.namespace).Get(waitCtx, name, metav1.GetOptions{})
		if err != nil {
			lastError = err
		} else if statefulSet.Status.ObservedGeneration >= statefulSet.Generation &&
			statefulSet.Status.ReadyReplicas == int32(cluster.DesiredNodes) &&
			statefulSet.Status.CurrentRevision == statefulSet.Status.UpdateRevision {
			return nil
		}
		select {
		case <-waitCtx.Done():
			if lastError != nil {
				return fmt.Errorf("wait for StatefulSet %s: %w (last API error: %v)", name, waitCtx.Err(), lastError)
			}
			return fmt.Errorf("wait for StatefulSet %s: %w", name, waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func (k *Kubernetes) waitStatefulSetDeleted(ctx context.Context, name string) error {
	ticker := time.NewTicker(k.pollInterval)
	defer ticker.Stop()
	for {
		_, err := k.client.AppsV1().StatefulSets(k.namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("wait for StatefulSet deletion: %w", err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for StatefulSet deletion: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func validateStorageTemplate(existing, desired *appsv1.StatefulSet) error {
	if len(existing.Spec.VolumeClaimTemplates) != 1 || len(desired.Spec.VolumeClaimTemplates) != 1 {
		return fmt.Errorf("StatefulSet storage template drift requires controlled migration")
	}
	current := existing.Spec.VolumeClaimTemplates[0]
	wanted := desired.Spec.VolumeClaimTemplates[0]
	currentSize := current.Spec.Resources.Requests[corev1.ResourceStorage]
	wantedSize := wanted.Spec.Resources.Requests[corev1.ResourceStorage]
	if current.Name != wanted.Name || currentSize.Cmp(wantedSize) != 0 || !reflect.DeepEqual(current.Spec.StorageClassName, wanted.Spec.StorageClassName) {
		return fmt.Errorf("StatefulSet storage template drift requires controlled migration")
	}
	return nil
}

func managedLabels(cluster model.Cluster) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name": "opensearch",
		managedByLabel:           managedByValue,
		clusterLabel:             cluster.ID,
	}
}

func ownedBy(resourceLabels map[string]string, clusterID string) bool {
	return resourceLabels[managedByLabel] == managedByValue && resourceLabels[clusterLabel] == clusterID
}

func mergeLabels(existing, managed map[string]string) map[string]string {
	result := make(map[string]string, len(existing)+len(managed))
	for key, value := range existing {
		result[key] = value
	}
	for key, value := range managed {
		result[key] = value
	}
	return result
}

func resourceName(cluster model.Cluster) string {
	id := strings.ToLower(cluster.ID)
	if len(id) > 12 {
		id = id[:12]
	}
	return "os-" + id
}

func boolPointer(value bool) *bool    { return &value }
func int64Pointer(value int64) *int64 { return &value }
