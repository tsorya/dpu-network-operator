/*
Copyright 2021.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	nmoapiv1beta1 "github.com/medik8s/node-maintenance-operator/api/v1beta1"
	"github.com/openshift/dpu-network-operator/pkg/utils"
	"github.com/sirupsen/logrus"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	machineryLabels "k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type DpuNodeController struct {
	client.Client
	Scheme    *runtime.Scheme
	Log       logrus.FieldLogger
	Namespace string
}

const (
	dpuNodeLabel      = "node-role.kubernetes.io/dpu-worker"
	deploymentPrefix  = "dpu-drain-blocker-"
	maintenancePrefix = "dpu-tenant-"
)

//+kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch;update;patch
//+kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=security.openshift.io,resources=securitycontextconstraints,resourceNames=hostnetwork,verbs=use

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.9.2/pkg/reconcile
func (r *DpuNodeController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithFields(
		logrus.Fields{
			"node_name":          req.Name,
			"operator namespace": r.Namespace,
		})

	namespace := r.Namespace
	node := &corev1.Node{}
	if err := r.Get(ctx, req.NamespacedName, node); err != nil {
		log.WithError(err).Errorf("Failed to get node %s", req.Name)
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// TODO need to run only on dpu nodes, better to move logic to setup manager
	if _, hasDpuLabel := node.Labels[dpuNodeLabel]; !hasDpuLabel {
		// TODO make it debug
		log.Infof("Node %s is not dpu, skip", node.Name)
		return ctrl.Result{}, nil
	}

	if err := r.ensureDeployment(log, node, namespace); err != nil {
		return ctrl.Result{}, err
	}

	// TODO: need to handle it some other way
	if TenantRestConfig == nil {
		r.getKubeconfig()
	}

	dependsOnTenant := TenantRestConfig != nil
	allowedDrain, requiresRetry := false, false
	var tenantErr error
	if dependsOnTenant {
		allowedDrain, requiresRetry, tenantErr = r.isTenantDrainedIfRequired(node)
	}

	if err := r.ensurePDB(log, node, namespace, allowedDrain); err != nil {
		return ctrl.Result{}, err
	}

	if requiresRetry || tenantErr != nil {
		return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
	}

	return ctrl.Result{}, nil
}

func (r *DpuNodeController) ensureDeployment(log logrus.FieldLogger, node *corev1.Node, namespace string) error {
	log.Infof("Ensure blocking deployment")
	expectedDeployment := r.getDeployment(node, namespace)
	deployment := &appsv1.Deployment{}
	err := r.Get(context.TODO(), types.NamespacedName{Name: deploymentPrefix + node.Name, Namespace: namespace}, deployment)
	if err != nil {
		if errors.IsNotFound(err) {

			err = r.Create(context.TODO(), expectedDeployment)
			if err != nil {
				log.WithError(err).Errorf("failed to create deployment for node %s", node.Name)
				return err
			}
			log.Infof("Created deployment for %s", node.Name)
		} else {
			return err
		}
	}
	// TODO add deployment manage
	return err
}

func (r *DpuNodeController) ensurePDB(log logrus.FieldLogger, node *corev1.Node, namespace string, allowDrain bool) error {
	//// if node is Unschedulable, we should not fix pdb
	if node.Spec.Unschedulable && !allowDrain {
		return nil
	}

	log.Infof("Ensure blocking pdb")
	expectedPDB := r.getPDB(node, namespace, allowDrain)
	pdb := &policyv1.PodDisruptionBudget{}
	err := r.Get(context.TODO(), types.NamespacedName{Name: expectedPDB.Name, Namespace: expectedPDB.Namespace}, pdb)
	if err != nil {
		if errors.IsNotFound(err) {
			err = r.Create(context.TODO(), expectedPDB)
			if err != nil {
				log.WithError(err).Errorf("failed to create PDB for node %s", node.Name)
				return err
			}
			log.Infof("Created PDB for %s", node.Name)
		} else {
			log.WithError(err).Errorf("Failed to get pdb %s", expectedPDB.Name)
			return err
		}
		return nil
	}
	// if pdb was not changed, nothing to do
	if *pdb.Spec.MaxUnavailable == *expectedPDB.Spec.MaxUnavailable {
		return nil
	}

	// Set expected spec
	pdb.Spec = expectedPDB.Spec
	// This should run only in case pdb was changed and not is schedulable
	// will allow to set back pdb value after reboot
	if allowDrain {
		log.Infof("Drain is allowed, setting max available to 1")
	} else {
		log.Infof("PDB was changed and node is schedulable, changing it back")
	}

	err = r.Update(context.TODO(), pdb)
	if err != nil {
		log.WithError(err).Errorf("Failed update pdb %s to it's previous value", expectedPDB.Name)
	}

	return err
}

func (r *DpuNodeController) getPDB(node *corev1.Node, namespace string, allowDrain bool) *policyv1.PodDisruptionBudget {
	maxUnavailable := int32(0)
	if allowDrain {
		maxUnavailable = 1
	}
	pdb := policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploymentPrefix + node.Name,
			Namespace: namespace,
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: &intstr.IntOrString{
				IntVal: maxUnavailable,
			},
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": deploymentPrefix + node.Name,
				},
			},
		},
	}
	return &pdb
}

func (r *DpuNodeController) getDeployment(node *corev1.Node, namespace string) *appsv1.Deployment {
	replicas := int32(1)
	labels := map[string]string{
		"app": deploymentPrefix + node.Name,
	}
	deployment := appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploymentPrefix + node.Name,
			Namespace: namespace,
			Labels:    labels,
		},

		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					HostNetwork: true,
					Containers: []corev1.Container{
						{
							Name: "sleep-forever",
							// TODO  change image
							Image: "quay.io/itsoiref/sleep:latest",
						},
					},
					NodeSelector: map[string]string{
						"kubernetes.io/hostname": node.Name,
					},
				},
			},
		},
	}

	return &deployment
}

func (r *DpuNodeController) isTenantDrainedIfRequired(node *corev1.Node) (bool, bool, error) {
	tenantClient, err := client.New(TenantRestConfig, client.Options{})
	if err != nil {
		r.Log.WithError(err).Errorf("Fail to create client for the tenant cluster")
		return false, false, err
	}
	nmoapiv1beta1.AddToScheme(tenantClient.Scheme())
	tenantNode, err := r.getMatchedTenantNodeByCM(node)
	if err != nil {
		r.Log.WithError(err).Errorf("failed to get tenant node that matches %s", node.Name)
		return false, false, err
	}
	if tenantNode == "" {
		r.Log.Warnf("Failed to find tenant node")
		return false, false, nil
	}

	shouldBeDrained := node.Spec.Unschedulable == true

	drained, err := r.handleNMO(tenantClient, tenantNode, shouldBeDrained)
	if err != nil {
		r.Log.WithError(err).Errorf("failed to cordon/uncordon tenant node %s", tenantNode)
		return false, false, err
	}

	if drained && shouldBeDrained {
		return true, false, nil
	} else if !drained && shouldBeDrained {
		return false, true, nil
	}
	return false, false, nil
}

func (r *DpuNodeController) getMatchedTenantNode(tenantClient client.Client, node *corev1.Node) (*corev1.Node, error) {
	//TODO: find dpu tenant node
	// maybe possible by hostname? What label should be set?
	nodes := corev1.NodeList{}
	labelSelector := machineryLabels.SelectorFromSet(map[string]string{"dpu-tenant": node.Name})
	listOps := &client.ListOptions{LabelSelector: labelSelector}
	err := tenantClient.List(context.TODO(), &nodes, listOps)
	if err != nil {
		r.Log.WithError(err).Error("Failed to list tenant nodes")
		return nil, err
	}
	if len(nodes.Items) < 1 {
		r.Log.Infof("Failed to find tenant host that matches %s dpu", node.Name)
		return nil, nil
	}

	// TODO: find host
	return &nodes.Items[0], nil
}

func (r *DpuNodeController) getMatchedTenantNodeByCM(node *corev1.Node) (string, error) {
	tenantConfig, err := utils.LoadConfig(node.Name)
	if err != nil {
		r.Log.WithError(err).Errorf("Failed to get matched tenant node from configmap mount file for node %s", node.Name)
		return "", nil
	}
	r.Log.Infof("Found tenant node %s", tenantConfig.TenantHostname)
	return tenantConfig.TenantHostname, nil
}

func (r *DpuNodeController) handleNMO(tenantClient client.Client, tenantNodeHostname string, shouldBeDrained bool) (bool, error) {
	// TODO find nodeMaintenance per this host
	nmName := maintenancePrefix + tenantNodeHostname
	nm := &nmoapiv1beta1.NodeMaintenance{}
	typedNM := types.NamespacedName{Name: nmName, Namespace: utils.TenantNamespace}

	// TODO: maybe need to set different namespace?
	err := tenantClient.Get(context.TODO(), typedNM, nm)
	if err != nil {
		if errors.IsNotFound(err) {
			// Nothing to do
			if !shouldBeDrained {
				return true, nil
			}

			err = tenantClient.Create(context.TODO(), &nmoapiv1beta1.NodeMaintenance{
				TypeMeta: metav1.TypeMeta{},
				ObjectMeta: metav1.ObjectMeta{
					Name:      nmName,
					Namespace: utils.TenantNamespace,
				},
				Spec: nmoapiv1beta1.NodeMaintenanceSpec{
					NodeName: tenantNodeHostname,
					Reason:   "Infra dpu is going to reboot",
				},
			})
			if err != nil {
				r.Log.WithError(err).Errorf("failed to create node maintenance for %s", tenantNodeHostname)
				return false, err
			}
			return false, nil
		} else {
			r.Log.WithError(err).Errorf("failed to get node maintenance %s in tenant cluster", nmName)
			return false, err
		}
	}

	// node is draining
	if shouldBeDrained && nm.Status.Phase == nmoapiv1beta1.MaintenanceRunning {
		return false, nil
	}

	// node should not be drained, delete nodeMaintenance
	if !shouldBeDrained {
		if err := tenantClient.Delete(context.TODO(), nm); err != nil {
			r.Log.WithError(err).Errorf("Failed to delete node maintenance cr %s", nmName)
			return false, err
		}
	}

	return true, err
}

// TODO: this is only temporary for now
// No need to return error as it is used only for manual testing currently
func (r *DpuNodeController) getKubeconfig() {
	if TenantRestConfig != nil {
		return
	}

	logger.Info("tempopary get tenant kubeconfig")
	var err error

	s := &corev1.Secret{}
	err = r.Client.Get(context.TODO(), types.NamespacedName{Name: "tenant-kubeconfig", Namespace: utils.Namespace}, s)
	if err != nil {
		if errors.IsNotFound(err) {
			r.Log.Infof("No secret tenant-kubeconfig in %s", utils.Namespace)
			return
		}
		r.Log.WithError(err).Warnf("Failed to get tenant kubeconfig though it exists")
		if err != nil {
			return
		}
	}

	bytes, ok := s.Data["config"]
	if !ok {
		r.Log.WithError(err).Warnf("Failed to get tenant kubeconfig, key \"config\" doesn't exist")
		return
	}

	TenantRestConfig, err = clientcmd.RESTConfigFromKubeConfig(bytes)
	if err != nil {
		r.Log.WithError(err).Warnf("Failed to create tenant rest config from given kubeconfig")
	}

	return
}

// SetupWithManager sets up the controller with the Manager.
func (r *DpuNodeController) SetupWithManager(mgr ctrl.Manager) error {
	mapToNode := func(obj client.Object) []reconcile.Request {
		name := obj.GetName()
		namespace := obj.GetNamespace()
		if namespace != r.Namespace || !strings.HasPrefix(name, deploymentPrefix) || len(strings.Split(name, deploymentPrefix)) < 2 {
			return []reconcile.Request{}
		}

		r.Log.Infof("Mapping node for obj %s in namespace %s of kind %v", name, name, obj.GetObjectKind())

		reply := make([]reconcile.Request, 0, 1)
		reply = append(reply, reconcile.Request{NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      strings.Split(name, deploymentPrefix)[1],
		}})
		return reply
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Node{}).
		Watches(&source.Kind{Type: &policyv1.PodDisruptionBudget{}}, handler.EnqueueRequestsFromMapFunc(mapToNode)).
		Watches(&source.Kind{Type: &appsv1.Deployment{}}, handler.EnqueueRequestsFromMapFunc(mapToNode)).
		Complete(r)
}
