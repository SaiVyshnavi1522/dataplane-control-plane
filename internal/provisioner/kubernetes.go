package provisioner

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/example/dataplane-control-plane/internal/model"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type Kubernetes struct {
	client    kubernetes.Interface
	namespace string
	image     string
}

func NewKubernetes(kubeconfig, namespace, image string) (*Kubernetes, error) {
	cfg, err := kubeConfig(kubeconfig)
	if err != nil {
		return nil, err
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &Kubernetes{client: client, namespace: namespace, image: image}, nil
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

func (k *Kubernetes) Provision(ctx context.Context, c model.Cluster) error {
	if err := k.ensureNamespace(ctx); err != nil {
		return err
	}
	if err := k.applyService(ctx, c); err != nil {
		return err
	}
	if err := k.applyStatefulSet(ctx, c); err != nil {
		return err
	}
	return k.waitReady(ctx, c)
}

func (k *Kubernetes) Scale(ctx context.Context, c model.Cluster) error {
	stsName := resourceName(c)
	sts, err := k.client.AppsV1().StatefulSets(k.namespace).Get(ctx, stsName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	replicas := int32(c.DesiredNodes)
	sts.Spec.Replicas = &replicas
	if _, err := k.client.AppsV1().StatefulSets(k.namespace).Update(ctx, sts, metav1.UpdateOptions{}); err != nil {
		return err
	}
	return k.waitReady(ctx, c)
}

func (k *Kubernetes) Delete(ctx context.Context, c model.Cluster) error {
	name := resourceName(c)
	policy := metav1.DeletePropagationForeground
	if err := k.client.AppsV1().StatefulSets(k.namespace).Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &policy}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err := k.client.CoreV1().Services(k.namespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func (k *Kubernetes) ensureNamespace(ctx context.Context) error {
	_, err := k.client.CoreV1().Namespaces().Get(ctx, k.namespace, metav1.GetOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	_, err = k.client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: k.namespace}}, metav1.CreateOptions{})
	return err
}

func (k *Kubernetes) applyService(ctx context.Context, c model.Cluster) error {
	name := resourceName(c)
	labels := map[string]string{"app.kubernetes.io/name": "opensearch", "dataplane.io/cluster": c.ID}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: k.namespace, Labels: labels},
		Spec: corev1.ServiceSpec{
			ClusterIP:                "None",
			PublishNotReadyAddresses: true,
			Selector:                 labels,
			Ports: []corev1.ServicePort{
				{Name: "http", Port: 9200, TargetPort: intstr.FromInt(9200)},
				{Name: "transport", Port: 9300, TargetPort: intstr.FromInt(9300)},
			},
		},
	}
	_, err := k.client.CoreV1().Services(k.namespace).Create(ctx, svc, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func (k *Kubernetes) applyStatefulSet(ctx context.Context, c model.Cluster) error {
	name := resourceName(c)
	labels := map[string]string{"app.kubernetes.io/name": "opensearch", "dataplane.io/cluster": c.ID}
	replicas := int32(c.DesiredNodes)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: k.namespace, Labels: labels},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: name,
			Replicas:    &replicas,
			Selector:    &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "opensearch",
						Image: k.image,
						Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 9200}, {Name: "transport", ContainerPort: 9300}, {Name: "metrics", ContainerPort: 9600}},
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
						VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/usr/share/opensearch/data"}},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler:        corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/_cluster/health", Port: intstr.FromInt(9200)}},
							InitialDelaySeconds: 25, PeriodSeconds: 10, TimeoutSeconds: 3, FailureThreshold: 12,
						},
					}},
					Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}},
				},
			},
		},
	}
	_, err := k.client.AppsV1().StatefulSets(k.namespace).Create(ctx, sts, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		existing, getErr := k.client.AppsV1().StatefulSets(k.namespace).Get(ctx, name, metav1.GetOptions{})
		if getErr != nil {
			return getErr
		}
		existing.Spec.Replicas = &replicas
		_, err = k.client.AppsV1().StatefulSets(k.namespace).Update(ctx, existing, metav1.UpdateOptions{})
	}
	return err
}

func (k *Kubernetes) waitReady(ctx context.Context, c model.Cluster) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	name := resourceName(c)
	for {
		sts, err := k.client.AppsV1().StatefulSets(k.namespace).Get(ctx, name, metav1.GetOptions{})
		if err == nil && sts.Status.ReadyReplicas >= int32(c.DesiredNodes) {
			return nil
		}
		select {
		case <-ctx.Done():
			if err != nil && !errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("wait for %s: %w", name, err)
			}
			return fmt.Errorf("wait for %s: %w", name, ctx.Err())
		case <-ticker.C:
		}
	}
}

func resourceName(c model.Cluster) string {
	id := strings.ToLower(c.ID)
	if len(id) > 12 {
		id = id[:12]
	}
	return "os-" + id
}
