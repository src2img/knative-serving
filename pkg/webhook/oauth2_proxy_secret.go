/*
Copyright 2025 IBM Corp.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package webhook

import (
	"context"
	"fmt"

	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/serving/pkg/reconciler/revision/resources"
)

// validateOauth2ProxyConfigSecret rejects resources whose revision template references an
// oauth2-proxy config secret that is not of type resources.Oauth2ProxyConfigSecretType.
// It is a no-op if no secret is referenced.
// A referenced secret that does not exist is rejected, so the secret must be created first.
func validateOauth2ProxyConfigSecret(ctx context.Context, uns *unstructured.Unstructured) error {
	// Structural problems of the resource are reported by the other validations.
	name, found, err := unstructured.NestedString(uns.UnstructuredContent(),
		"spec", "template", "metadata", "annotations", resources.Oauth2ProxyConfigSecretAnnotation)
	if err != nil || !found || name == "" {
		return nil
	}

	namespace := uns.GetNamespace()
	secret, err := kubeclient.Get(ctx).CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrs.IsNotFound(err) {
		return fmt.Errorf("secret %q referenced by annotation %q was not found in namespace %q",
			name, resources.Oauth2ProxyConfigSecretAnnotation, namespace)
	} else if err != nil {
		return fmt.Errorf("failed to get secret %s/%s referenced by annotation %q: %w",
			namespace, name, resources.Oauth2ProxyConfigSecretAnnotation, err)
	}

	if secret.Type != resources.Oauth2ProxyConfigSecretType {
		return fmt.Errorf("secret %q referenced by annotation %q must be of type %q, but is of type %q",
			name, resources.Oauth2ProxyConfigSecretAnnotation, resources.Oauth2ProxyConfigSecretType, secret.Type)
	}
	return nil
}
