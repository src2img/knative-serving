/*
Copyright 2018 The Knative Authors

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

package revision

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"

	"golang.org/x/time/rate"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	caching "knative.dev/caching/pkg/apis/caching/v1alpha1"
	"knative.dev/pkg/kmeta"
	"knative.dev/pkg/kmp"
	"knative.dev/pkg/logging"
	autoscalingv1alpha1 "knative.dev/serving/pkg/apis/autoscaling/v1alpha1"
	v1 "knative.dev/serving/pkg/apis/serving/v1"
	"knative.dev/serving/pkg/reconciler/revision/config"
	"knative.dev/serving/pkg/reconciler/revision/resources"
)

var (
	deploymentUpdatesLimiter                       *rate.Limiter
	deploymentUpdatesUnscheduledPodsCheckNamespace string
)

func init() {
	deploymentUpdatesMaxPerTimeSlice := 0
	deploymentUpdatesTimeSliceDurationSeconds := 0.0

	var err error
	if value, found := os.LookupEnv("DEPLOYMENT_UPDATES_TIME_SLICE_DURATION_SECONDS"); found {
		if deploymentUpdatesTimeSliceDurationSeconds, err = strconv.ParseFloat(value, 64); err != nil {
			log.Fatalf("Non-numeric value for DEPLOYMENT_UPDATES_TIME_SLICE_DURATION_SECONDS: %s", value)
		}
	}

	if value, found := os.LookupEnv("DEPLOYMENT_UPDATES_MAX_PER_TIME_SLICE"); found {
		if deploymentUpdatesMaxPerTimeSlice, err = strconv.Atoi(value); err != nil {
			log.Fatalf("Non-numeric value for DEPLOYMENT_UPDATES_MAX_PER_TIME_SLICE: %s", value)
		}
	}

	if deploymentUpdatesMaxPerTimeSlice != 0 && deploymentUpdatesTimeSliceDurationSeconds != 0.0 {
		deploymentUpdatesLimiter = rate.NewLimiter(rate.Limit(float64(deploymentUpdatesMaxPerTimeSlice)/deploymentUpdatesTimeSliceDurationSeconds), deploymentUpdatesMaxPerTimeSlice)
	} else {
		deploymentUpdatesLimiter = rate.NewLimiter(rate.Inf, math.MaxInt)
	}

	deploymentUpdatesUnscheduledPodsCheckNamespace = os.Getenv("DEPLOYMENT_UPDATES_UNSCHEDULED_PODS_CHECK_NAMESPACE")
}

func (c *Reconciler) createDeployment(ctx context.Context, rev *v1.Revision) (*appsv1.Deployment, error) {
	cfgs := config.FromContext(ctx)

	deployment, err := resources.MakeDeployment(rev, cfgs)

	if err != nil {
		return nil, fmt.Errorf("failed to make deployment: %w", err)
	}

	return c.kubeclient.AppsV1().Deployments(deployment.Namespace).Create(ctx, deployment, metav1.CreateOptions{})
}

func (c *Reconciler) checkAndUpdateDeployment(ctx context.Context, rev *v1.Revision, have *appsv1.Deployment) (*appsv1.Deployment, bool, error) {
	logger := logging.FromContext(ctx)
	cfgs := config.FromContext(ctx)

	deployment, err := resources.MakeDeployment(rev, cfgs)
	if err != nil {
		return nil, false, fmt.Errorf("failed to update deployment: %w", err)
	}

	// Check if the hash matches what we currently have in the system
	if deployment.Annotations["serving.knative.dev/deployment-spec-hash"] == have.Annotations["serving.knative.dev/deployment-spec-hash"] {
		logger.Infof("Not updating deployment %s/%s because of an empty diff\n", deployment.Namespace, deployment.Name)
		return have, false, nil
	}

	// We always update deployments that:
	// - are scaled to 0 because this is a cheap update
	// - are related to a revision that is not actively routed, these are usually old revisions in the
	//   process of being scaled down which we cannot delay
	if *have.Spec.Replicas > 0 && rev.GetRoutingState() == v1.RoutingStateActive {
		if !deploymentUpdatesLimiter.Allow() {
			logger.Infof("Not updating deployment %s/%s now because of too many concurrent deployment updates\n", deployment.Namespace, deployment.Name)
			return nil, true, nil
		}

		if deploymentUpdatesUnscheduledPodsCheckNamespace != "" {
			pods, err := c.kubeclient.CoreV1().Pods(deploymentUpdatesUnscheduledPodsCheckNamespace).List(ctx, metav1.ListOptions{
				FieldSelector: "spec.nodeName=",
			})
			if err != nil {
				return nil, false, err
			}

			if len(pods.Items) > 0 {
				logger.Infof("Not updating deployment %s/%s now because there are unscheduled pods in namespace %s\n", deployment.Namespace, deployment.Name, deploymentUpdatesUnscheduledPodsCheckNamespace)
				return nil, true, nil
			}
		}
	}

	// Preserve the current scale of the Deployment.
	deployment.Spec.Replicas = have.Spec.Replicas

	// Preserve the label selector since it's immutable.
	// TODO(dprotaso): determine other immutable properties.
	deployment.Spec.Selector = have.Spec.Selector

	// Otherwise attempt an update (with ONLY the spec changes).
	desiredDeployment := have.DeepCopy()
	desiredDeployment.Spec = deployment.Spec
	desiredDeployment.Annotations["serving.knative.dev/deployment-spec-hash"] = deployment.Annotations["serving.knative.dev/deployment-spec-hash"]

	// Carry over new labels.
	desiredDeployment.Labels = kmeta.UnionMaps(deployment.Labels, desiredDeployment.Labels)

	logger.Infof("Updating deployment %s/%s\n", deployment.Namespace, deployment.Name)

	d, err := c.kubeclient.AppsV1().Deployments(deployment.Namespace).Update(ctx, desiredDeployment, metav1.UpdateOptions{})
	if err != nil {
		return nil, false, err
	}

	// If what comes back from the update (with defaults applied by the API server) is the same
	// as what we have then nothing changed.
	if equality.Semantic.DeepEqual(have.Spec, d.Spec) {
		return d, false, nil
	}
	diff, err := kmp.SafeDiff(have.Spec, d.Spec)
	if err != nil {
		return nil, false, err
	}

	// If what comes back has a different spec, then signal the change.
	logger.Info("Reconciled deployment diff (-desired, +observed): ", diff)
	return d, false, nil
}

func (c *Reconciler) createImageCache(ctx context.Context, rev *v1.Revision, containerName, imageDigest string) (*caching.Image, error) {
	image := resources.MakeImageCache(rev, containerName, imageDigest)
	return c.cachingclient.CachingV1alpha1().Images(image.Namespace).Create(ctx, image, metav1.CreateOptions{})
}

func (c *Reconciler) createPA(
	ctx context.Context,
	rev *v1.Revision,
	deployment *appsv1.Deployment,
) (*autoscalingv1alpha1.PodAutoscaler, error) {
	pa := resources.MakePA(rev, deployment)
	return c.client.AutoscalingV1alpha1().PodAutoscalers(pa.Namespace).Create(ctx, pa, metav1.CreateOptions{})
}
