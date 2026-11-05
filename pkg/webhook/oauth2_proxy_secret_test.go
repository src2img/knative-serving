/*
Copyright 2025 IBM Corp.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package webhook

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	fakekubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	"knative.dev/pkg/logging"
	logtesting "knative.dev/pkg/logging/testing"
	"knative.dev/serving/pkg/reconciler/revision/resources"
)

func TestValidateOauth2ProxyConfigSecret(t *testing.T) {
	const ns = "foo"

	secretOfType := func(typ corev1.SecretType) *corev1.Secret {
		return &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "oidc-secret", Namespace: ns},
			Type:       typ,
		}
	}
	service := func(annotations map[string]interface{}) map[string]interface{} {
		return map[string]interface{}{
			"metadata": map[string]interface{}{"name": "svc", "namespace": ns},
			"spec": map[string]interface{}{
				"template": map[string]interface{}{
					"metadata": map[string]interface{}{"annotations": annotations},
				},
			},
		}
	}
	withSecret := map[string]interface{}{resources.Oauth2ProxyConfigSecretAnnotation: "oidc-secret"}

	tests := []struct {
		name   string
		secret *corev1.Secret
		data   map[string]interface{}
		want   string
	}{{
		name:   "secret of the correct type",
		secret: secretOfType(resources.Oauth2ProxyConfigSecretType),
		data:   service(withSecret),
	}, {
		name:   "opaque secret",
		secret: secretOfType(corev1.SecretTypeOpaque),
		data:   service(withSecret),
		want:   `must be of type "codeengine.cloud.ibm.com/oauth2"`,
	}, {
		name:   "other secret type",
		secret: secretOfType(corev1.SecretTypeTLS),
		data:   service(withSecret),
		want:   `must be of type "codeengine.cloud.ibm.com/oauth2"`,
	}, {
		name: "secret does not exist",
		data: service(withSecret),
		want: `was not found in namespace "foo"`,
	}, {
		name: "no annotation",
		data: service(map[string]interface{}{}),
	}, {
		name: "no template",
		data: map[string]interface{}{"spec": map[string]interface{}{}},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, kube := fakekubeclient.With(context.Background())
			ctx = logging.WithLogger(ctx, logtesting.TestLogger(t))
			if test.secret != nil {
				if _, err := kube.CoreV1().Secrets(ns).Create(ctx, test.secret, metav1.CreateOptions{}); err != nil {
					t.Fatal("Failed to create secret:", err) // pragma: allowlist secret
				}
			}

			uns := &unstructured.Unstructured{}
			uns.SetUnstructuredContent(test.data)

			for name, validate := range map[string]func(context.Context, *unstructured.Unstructured) error{
				"service":       ValidateService,
				"configuration": ValidateConfiguration,
			} {
				got := validate(ctx, uns)
				if test.want == "" {
					if got != nil {
						t.Errorf("%s: Validate got=%v, want no error", name, got)
					}
				} else if got == nil || !strings.Contains(got.Error(), test.want) {
					t.Errorf("%s: Validate got=%v, want error containing %q", name, got, test.want)
				}
			}
		})
	}
}
