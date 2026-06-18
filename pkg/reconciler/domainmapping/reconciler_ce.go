package domainmapping

import (
	"context"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"knative.dev/pkg/kmeta"
	"knative.dev/serving/pkg/apis/serving/v1beta1"
)

func (r *Reconciler) disableDomainMapping(ctx context.Context, dm *v1beta1.DomainMapping) error {
	// delete the KIngress
	if err := r.netclient.NetworkingV1alpha1().Ingresses(dm.GetNamespace()).Delete(ctx, kmeta.ChildName(dm.GetName(), ""), metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
		return err
	}

	return nil
}
