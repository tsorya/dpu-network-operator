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
	"k8s.io/apimachinery/pkg/api/equality"
	"strings"
	"time"

	nmoapiv1beta1 "github.com/medik8s/node-maintenance-operator/api/v1beta1"
	"github.com/sirupsen/logrus"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/openshift/dpu-network-operator/pkg/utils"
)

type Config struct {
	Image               string `envconfig:"IMAGE" default:"quay.io/centos/centos:stream8"`
	ServiceAccount      string `envconfig:"SERVICE_ACCOUNT" default:"dpu-network-operator-controller-manager"`
	SingleClusterDesign bool   `envconfig:"SINGLE_CLUSTER_DESIGN" default:"false"`
}

type DpuNodeLifecycleController struct {
	client.Client
	Config       *Config
	Scheme       *runtime.Scheme
	Log          logrus.FieldLogger
	tenantClient client.Client
	Namespace    string
}

const (
	dpuNodeLabel            = "node-role.kubernetes.io/dpu-worker"
	deploymentPrefix        = "dpu-drain-blocker-"
	maintenancePrefix       = "dpu-tenant-"
	deploymentReplicaNumber = 1
)

//+kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch;update;patch
//+kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=nodemaintenance.medik8s.io,resources=nodemaintenances,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *DpuNodeLifecycleController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithFields(
		logrus.Fields{
			"node_name":          req.Name,
			"operator namespace": r.Namespace,
		})

	var err error
	if r.tenantClient == nil {
		r.tenantClient, err = r.createTenantClient(log)
		// if no tenant client, nothing to do
		if err != nil || r.tenantClient == nil {
			return ctrl.Result{}, err
		}
	}

	defer func() {
		log.Info("node controller reconcile started ended")
	}()

	log.Info("node controller reconcile started")
	namespace := r.Namespace
	node := &corev1.Node{}
	if err := r.Get(ctx, req.NamespacedName, node); err != nil {
		log.WithError(err).Errorf("Failed to get node %s", req.Name)
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if _, hasDpuLabel := node.Labels[dpuNodeLabel]; !hasDpuLabel {
		log.Debugf("Node %s is not dpu, skip", node.Name)
		return ctrl.Result{}, nil
	}

	if err := r.createDeploymentIfNotExist(log, node, namespace); err != nil {
		return ctrl.Result{}, err
	}

	expectedPDB := r.buildPDB(node, namespace)
	pdb, err := r.getOrCreatePDB(log, node, expectedPDB)
	if err != nil {
		return ctrl.Result{}, err
	}

	tenantShouldBeDrained := r.shouldTenantHostBeDrained(node)
	allowedToDrainDpu, err := r.isTenantDrainedIfRequired(node.Name, tenantShouldBeDrained)
	if err != nil {
		return ctrl.Result{}, err
	}

	if err := r.ensurePDBSpecIsAsExpected(log, pdb, expectedPDB, allowedToDrainDpu); err != nil {
		return ctrl.Result{}, err
	}

	// if tenant should be drained but it was not yet, we should retry reconcile as we don't listen on tenant nodes events
	if !allowedToDrainDpu && tenantShouldBeDrained {
		return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
	}

	return ctrl.Result{}, nil
}

// Create deployment with that will run sleep infinity
// This deployment with help of pdb will block drain of dpu node
func (r *DpuNodeLifecycleController) createDeploymentIfNotExist(log logrus.FieldLogger, node *corev1.Node, namespace string) error {
	log.Infof("Ensure blocking deployment")
	expectedDeployment := r.buildDeployment(node, namespace)
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
	return err
}

// return dpu or create one in case it didn't exist
func (r *DpuNodeLifecycleController) getOrCreatePDB(log logrus.FieldLogger, node *corev1.Node, expectedPDB *policyv1.PodDisruptionBudget) (*policyv1.PodDisruptionBudget, error) {
	log.Infof("Get or create blocking pdb")
	pdb := &policyv1.PodDisruptionBudget{}
	err := r.Get(context.TODO(), types.NamespacedName{Name: expectedPDB.Name, Namespace: expectedPDB.Namespace}, pdb)
	if err != nil {
		if errors.IsNotFound(err) {
			err = r.Create(context.TODO(), expectedPDB)
			if err != nil {
				log.WithError(err).Errorf("failed to create PDB for node %s", node.Name)
				return nil, err
			}
			log.Infof("Created PDB for %s", node.Name)
			pdb = expectedPDB
		} else {
			log.WithError(err).Errorf("Failed to get pdb %s", expectedPDB.Name)
			return nil, err
		}
	}
	return pdb, nil
}

// in case dpu is allowed to drain return 1
func (r *DpuNodeLifecycleController) getExpectedMaxUnavailableValue(allowedToDrain bool) int {
	expectedMaxUnavailableValue := 0
	if allowedToDrain {
		expectedMaxUnavailableValue = deploymentReplicaNumber
	}
	return expectedMaxUnavailableValue
}

// Ensure pdb spec was not change and is same as expected one
// Set MaxUnavailable field in PDB to the expected value
// More than 0 value will allow deployment eviction that will allow dpu to fulfill drain
func (r *DpuNodeLifecycleController) ensurePDBSpecIsAsExpected(log logrus.FieldLogger, pdb, expectedPDB *policyv1.PodDisruptionBudget, allowedToDrain bool) error {
	expectedMaxUnavailableValue := r.getExpectedMaxUnavailableValue(allowedToDrain)
	expectedPDB.Spec.MaxUnavailable.IntVal = int32(r.getExpectedMaxUnavailableValue(allowedToDrain))
	if equality.Semantic.DeepEqual(pdb.Spec, expectedPDB.Spec) {
		log.Infof("No changes is in pdb spec, MaxUnavailable is %d ", expectedMaxUnavailableValue)
		return nil
	}

	// Set expected spec
	pdb.Spec = expectedPDB.Spec
	log.Infof("Setting pdb's spec to %+v", expectedPDB.Spec)
	err := r.Update(context.TODO(), pdb)
	if err != nil {
		log.WithError(err).Errorf("Failed update pdb %s to it's previous value", pdb.Name)
	}

	return err
}

func (r *DpuNodeLifecycleController) buildPDB(node *corev1.Node, namespace string) *policyv1.PodDisruptionBudget {
	maxUnavailable := int32(0)
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

func (r *DpuNodeLifecycleController) buildDeployment(node *corev1.Node, namespace string) *appsv1.Deployment {
	replicas := int32(deploymentReplicaNumber)
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
							Name:    "sleep-forever",
							Image:   r.Config.Image,
							Command: []string{"/bin/sh", "-ec", "sleep infinity"},
						},
					},
					NodeSelector: map[string]string{
						"kubernetes.io/hostname": node.Name,
					},
					ServiceAccountName: r.Config.ServiceAccount,
				},
			},
		},
	}

	return &deployment
}

func (r *DpuNodeLifecycleController) shouldTenantHostBeDrained(node *corev1.Node) bool {
	return node.Spec.Unschedulable == true
}

// Find tenant node that matches dpu
// Verify if it was drained or not
func (r *DpuNodeLifecycleController) isTenantDrainedIfRequired(dpuNodeName string, shouldBeDrained bool) (bool, error) {
	tenantNode, err := utils.GetMatchedTenantNode(dpuNodeName, r.Log)
	if err != nil {
		r.Log.WithError(err).Errorf("failed to get tenant node that matches %s", dpuNodeName)
		return false, err
	}
	if tenantNode == "" {
		r.Log.Warnf("Failed to find tenant node")
		return false, nil
	}

	drained, err := r.handleNMO(tenantNode, shouldBeDrained)
	if err != nil {
		r.Log.WithError(err).Errorf("failed to cordon/uncordon tenant node %s", tenantNode)
		return false, err
	}

	if drained && shouldBeDrained {
		return true, nil
	}

	return false, nil
}

// Create or delete nodeMaintenance cr
// creating CR will say to NM operator to put node to maintenance
// Deleting CR will move node from maintenance
// Currently nodemaintenance operator doesn't save previous status of the node, in that case if node previously
// was drained or cordoned it will become uncordon
func (r *DpuNodeLifecycleController) handleNMO(tenantNodeHostname string, shouldBeDrained bool) (bool, error) {
	nmName := maintenancePrefix + tenantNodeHostname

	nm, err := r.getOrCreateNMCRIfNeeded(nmName, tenantNodeHostname, shouldBeDrained)
	if err != nil {
		return false, err
	}

	// If nm doesn't exist and we should not create it, nothing to do
	if nm == nil && !shouldBeDrained {
		return true, nil
	}

	// node was drained
	if shouldBeDrained && nm.Status.Phase == nmoapiv1beta1.MaintenanceSucceeded {
		r.Log.Infof("Tenant node %s was drained", tenantNodeHostname)
		return true, nil
	}

	// node should not be drained, delete nodeMaintenance
	if !shouldBeDrained {
		r.Log.Infof("Tenant node %s should be uncordon, deleting NM cr", tenantNodeHostname)
		if err := r.tenantClient.Delete(context.TODO(), nm); err != nil {
			r.Log.WithError(err).Errorf("Failed to delete node maintenance cr %s", nmName)
			return false, err
		}
	}

	r.Log.Infof("Tenant node %s was not drained yet", tenantNodeHostname)
	return false, err
}

func (r *DpuNodeLifecycleController) getOrCreateNMCRIfNeeded(name, tenantNodeHostname string, shouldBeCreated bool) (*nmoapiv1beta1.NodeMaintenance, error) {
	nm := &nmoapiv1beta1.NodeMaintenance{}
	typedNM := types.NamespacedName{Name: name, Namespace: utils.TenantNamespace}

	// Create CR if node should be drained and remove if not
	err := r.tenantClient.Get(context.TODO(), typedNM, nm)
	if err != nil {
		if errors.IsNotFound(err) {
			// Nothing to do
			if !shouldBeCreated {
				return nil, nil
			}

			err = r.tenantClient.Create(context.TODO(), r.buildNodeMaintenanceCr(name, tenantNodeHostname))
			if err != nil {
				r.Log.WithError(err).Errorf("failed to create node maintenance for %s", tenantNodeHostname)
				return nil, err
			}
			return nil, nil
		} else {
			r.Log.WithError(err).Errorf("failed to get node maintenance %s in tenant cluster", name)
			return nil, err
		}
	}
	return nm, nil
}

func (r *DpuNodeLifecycleController) buildNodeMaintenanceCr(name, tenantNodeHostname string) *nmoapiv1beta1.NodeMaintenance {
	return &nmoapiv1beta1.NodeMaintenance{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: utils.TenantNamespace,
		},
		Spec: nmoapiv1beta1.NodeMaintenanceSpec{
			NodeName: tenantNodeHostname,
			Reason:   "Infra dpu is going to reboot",
		}}
}

// Return client that will handle hosts with dpu status
func (r *DpuNodeLifecycleController) createTenantClient(log logrus.FieldLogger) (client.Client, error) {

	if r.Config.SingleClusterDesign {
		log.Infof("Single cluster design is on, tenant client is the same as local")
		return r.Client, nil
	}

	tenantKubeconfig, err := r.getTenantRestClientConfig()
	if err != nil {
		log.WithError(err).Info("failed to get tenant kubeconfig")
		return nil, err
	}

	if tenantKubeconfig == nil {
		return nil, nil
	}
	tenantClient, err := client.New(tenantKubeconfig, client.Options{})
	if err != nil {
		r.Log.WithError(err).Errorf("Fail to create client for the tenant cluster")
		return nil, err
	}
	nmoapiv1beta1.AddToScheme(tenantClient.Scheme())
	return tenantClient, err
}

// If TenantRestConfig was set by ovn controller we should use it
// In order to provide an option to run controller without creating ovn offloading cr
// added an option to provide tenant kubeconfig by dedicated secret tenant-kubeconfig
func (r *DpuNodeLifecycleController) getTenantRestClientConfig() (*restclient.Config, error) {

	if utils.TenantRestConfig != nil {
		return utils.TenantRestConfig, nil
	}

	tenantKubeconfigName := "tenant-kubeconfig"
	var err error

	s := &corev1.Secret{}
	err = r.Client.Get(context.TODO(), types.NamespacedName{Name: tenantKubeconfigName, Namespace: utils.Namespace}, s)
	if err != nil {
		if errors.IsNotFound(err) {
			r.Log.Infof("No secret %s in %s, skipping", tenantKubeconfigName, utils.Namespace)
			return nil, nil
		}
		r.Log.WithError(err).Warnf("Failed to get %s though it exists", tenantKubeconfigName)
		if err != nil {
			return nil, err
		}
	}

	bytes, ok := s.Data["config"]
	if !ok {
		r.Log.WithError(err).Warnf("Failed to get %s, key \"config\" doesn't exist", tenantKubeconfigName)
		return nil, err
	}

	return clientcmd.RESTConfigFromKubeConfig(bytes)
}

// SetupWithManager sets up the controller with the Manager.
func (r *DpuNodeLifecycleController) SetupWithManager(mgr ctrl.Manager) error {
	mapToNode := func(obj client.Object) []reconcile.Request {
		name := obj.GetName()
		namespace := obj.GetNamespace()
		if namespace != r.Namespace || !strings.HasPrefix(name, deploymentPrefix) || len(strings.Split(name, deploymentPrefix)) < 2 {
			return []reconcile.Request{}
		}

		r.Log.Infof("Mapping node for obj %s in namespace %s of kind %v", name, namespace, obj.GetObjectKind())

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
