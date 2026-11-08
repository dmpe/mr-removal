/*
Copyright 2026.

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

package controller

import (
	"context"
	"fmt"
	"strings"

	gitlab "gitlab.com/gitlab-org/api/client-go/v3"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	gitlabv1alpha "dmpe.github.io/mr-removal/api/v1alpha"
)

// MRRemovalReconciler reconciles a MRRemoval object
type MRRemovalReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

func (r *MRRemovalReconciler) allMergeRequests(ctx context.Context, git *gitlab.Client, project_id int) ([]*gitlab.BasicMergeRequest, error) {
	log := logf.FromContext(ctx)
	states := []string{"closed", "merged"}

	var allMRs []*gitlab.BasicMergeRequest

	for _, state := range states {
		mrList, _, err := git.MergeRequests.ListProjectMergeRequests(
			project_id,
			&gitlab.ListProjectMergeRequestsOptions{
				State: new(state),
				ListOptions: gitlab.ListOptions{
					PerPage: 100,
				},
			},
		)
		if err != nil {
			log.Error(err, "Failed to list merge requests for project", "project_id", project_id, "state", state)
			return nil, err
		}

		allMRs = append(allMRs, mrList...)
	}
	return allMRs, nil
}

func (r *MRRemovalReconciler) identifyGitLabMRs(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	mrRemoval := &gitlabv1alpha.MRRemoval{}
	err := r.Get(ctx, req.NamespacedName, mrRemoval)
	if err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	gl_url := mrRemoval.Spec.GitLabURL
	gl_token := mrRemoval.Spec.GitLabToken
	project_id := mrRemoval.Spec.GitLabProjectID
	gitlab_host := strings.TrimRight(gl_url, "/") + "/api/v4"

	// create gitlab client
	git, err := gitlab.NewClient(gl_token, gitlab.WithBaseURL(gitlab_host))
	if err != nil {
		log.Error(err, "Failed to create client")
	}

	allMRs, err := r.allMergeRequests(ctx, git, project_id)
	if err != nil {
		log.Error(err, "Failed to combine all project merge requests")
	}

	// list all cluster namespaces
	all_namespaces := &corev1.NamespaceList{}
	err := r.List(ctx, all_namespaces)
	if err != nil {
		return ctrl.Result{}, err
	}
	return all_namespaces, err
}

// +kubebuilder:rbac:groups=gitlab.dmpe.github.io,resources=mrremovals,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gitlab.dmpe.github.io,resources=mrremovals/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gitlab.dmpe.github.io,resources=mrremovals/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the MRRemoval object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.25.0/pkg/reconcile
func (r *MRRemovalReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = logf.FromContext(ctx)

	// iterate over each namespace and attempt to remove it
	all_namespaces := identifyGitLabMRs(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}

	for key, ns := range all_namespaces.Items {
		fmt.Println("Namespace: ", key, " ->", ns.Name)
		fmt.Println("Will now attempt to remove it")
		err := r.removeNamespace(ctx, ns.Name)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *MRRemovalReconciler) removeNamespace(ctx context.Context, namespace string) error {
	log := logf.FromContext(ctx)
	ns := &corev1.Namespace{}
	err := r.Get(ctx, client.ObjectKey{Name: namespace}, ns)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Namespace already deleted", "namespace", namespace)
			return nil
		}
		return fmt.Errorf("failed to get namespace %s: %w", namespace, err)
	}

	err = r.Delete(ctx, ns)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}

		return fmt.Errorf("failed to delete namespace %s: %w", namespace, err)
	}

	log.Info("Namespace deletion initiated", "namespace", namespace)
	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *MRRemovalReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&gitlabv1alpha.MRRemoval{}).
		Named("mrremoval").
		Complete(r)
}
