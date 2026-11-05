/*
Copyright 2026 IBM Corp.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package resources

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	v1 "knative.dev/serving/pkg/apis/serving/v1"
)

func TestIsOIDCAuthenticationEnabled(t *testing.T) {
	tests := []struct {
		name        string
		env         string
		annotations map[string]string
		want        bool
	}{{
		name:        "env enabled and secret annotation set",
		env:         "true",
		annotations: map[string]string{Oauth2ProxyConfigSecretAnnotation: "my-secret"},
		want:        true,
	}, {
		name:        "env disabled and secret annotation set",
		env:         "false",
		annotations: map[string]string{Oauth2ProxyConfigSecretAnnotation: "my-secret"},
		want:        false,
	}, {
		name:        "env unset and secret annotation set",
		env:         "",
		annotations: map[string]string{Oauth2ProxyConfigSecretAnnotation: "my-secret"},
		want:        false,
	}, {
		name:        "env enabled and annotation missing",
		env:         "true",
		annotations: map[string]string{"unrelated": "value"},
		want:        false,
	}, {
		name:        "env enabled and secret annotation empty",
		env:         "true",
		annotations: map[string]string{Oauth2ProxyConfigSecretAnnotation: ""},
		want:        false,
	}, {
		name:        "env enabled and only configmap annotation set",
		env:         "true",
		annotations: map[string]string{Oauth2ProxyConfigConfigMapAnnotation: "my-configmap"},
		want:        false,
	}, {
		name:        "env enabled and nil annotations",
		env:         "true",
		annotations: nil,
		want:        false,
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rev := &v1.Revision{ObjectMeta: metav1.ObjectMeta{Annotations: test.annotations}}
			t.Setenv(OIDCAuthenticationEnvVar, test.env)

			if got := IsOIDCAuthenticationEnabled(rev); got != test.want {
				t.Errorf("IsOIDCAuthenticationEnabled() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestGetOauth2ProxyPort(t *testing.T) {
	tests := []struct {
		name string
		rev  *v1.Revision
		want int32
	}{{
		name: "user container and user sidecar use 4180 and 4181",
		rev: &v1.Revision{
			Spec: v1.RevisionSpec{
				PodSpec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "user-container",
						Ports: []corev1.ContainerPort{{ContainerPort: 4180}},
					}, {
						Name:  "user-sidecar",
						Ports: []corev1.ContainerPort{{ContainerPort: 4181}},
					}},
				},
			},
		},
		want: 4182,
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := getOauth2ProxyPort(test.rev); got != test.want {
				t.Errorf("getOauth2ProxyPort() = %d, want %d", got, test.want)
			}
		})
	}
}

func TestBuildOauth2ProxyEnvFromSecret(t *testing.T) {
	tests := []struct {
		name       string
		secretName string
		want       []corev1.EnvFromSource
	}{{
		name:       "empty name returns nil",
		secretName: "",
		want:       nil,
	}, {
		name:       "non-empty name returns secret ref",
		secretName: "my-secret",
		want: []corev1.EnvFromSource{{
			SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: "my-secret"},
			},
		}},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := buildOauth2ProxyEnvFromSecret(test.secretName)
			if diff := cmp.Diff(test.want, got); diff != "" {
				t.Errorf("buildOauth2ProxyEnvFromSecret() (-want, +got) =\n%s", diff)
			}
		})
	}
}

const testOauth2ProxyImage = "oauth2-proxy-image"

var (
	testSecretRef = corev1.EnvFromSource{SecretRef: &corev1.SecretEnvSource{
		LocalObjectReference: corev1.LocalObjectReference{Name: "my-secret"},
	}}
	testConfigMapRef = corev1.EnvFromSource{ConfigMapRef: &corev1.ConfigMapEnvSource{
		LocalObjectReference: corev1.LocalObjectReference{Name: "my-configmap"},
	}}
)

// oauth2ProxyContainer builds the expected sidecar for the given proxy port, upstream user port and envFrom.
func oauth2ProxyContainer(proxyPort, userPort int32, envFrom []corev1.EnvFromSource) corev1.Container {
	return corev1.Container{
		Name:  Oauth2ProxyContainerName,
		Image: testOauth2ProxyImage,
		Ports: []corev1.ContainerPort{{
			Name:          "ce-oauth2-port",
			ContainerPort: proxyPort,
		}},
		Env: []corev1.EnvVar{{
			Name:  "OAUTH2_PROXY_HTTP_ADDRESS",
			Value: fmt.Sprintf("http://0.0.0.0:%d", proxyPort),
		}, {
			Name:  "OAUTH2_PROXY_UPSTREAMS",
			Value: fmt.Sprintf("http://127.0.0.1:%d", userPort),
		}},
		EnvFrom: envFrom,
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
				Path:   "/ready",
				Port:   intstr.FromInt32(proxyPort),
				Scheme: corev1.URISchemeHTTP,
			}},
			PeriodSeconds:    1,
			TimeoutSeconds:   1,
			SuccessThreshold: 1,
			FailureThreshold: 3,
		},
		Lifecycle:       userLifecycle,
		SecurityContext: queueSecurityContext,
	}
}

func TestMakeOauth2ProxyContainer(t *testing.T) {
	wantContainer := oauth2ProxyContainer
	secretRef, configMapRef := testSecretRef, testConfigMapRef
	userContainer := func(ports ...corev1.ContainerPort) []corev1.Container {
		return []corev1.Container{{Name: servingContainerName, Image: "busybox", Ports: ports}}
	}

	tests := []struct {
		name string
		rev  *v1.Revision
		want corev1.Container
	}{{
		name: "secret annotation only",
		rev: revision("bar", "foo",
			withContainers(userContainer()),
			WithRevisionAnnotations(map[string]string{Oauth2ProxyConfigSecretAnnotation: "my-secret"}),
		),
		want: wantContainer(4180, v1.DefaultUserPort, []corev1.EnvFromSource{secretRef}),
	}, {
		name: "secret and configmap annotations, secret takes precedence",
		rev: revision("bar", "foo",
			withContainers(userContainer()),
			WithRevisionAnnotations(map[string]string{
				Oauth2ProxyConfigSecretAnnotation:    "my-secret",
				Oauth2ProxyConfigConfigMapAnnotation: "my-configmap",
			}),
		),
		want: wantContainer(4180, v1.DefaultUserPort, []corev1.EnvFromSource{configMapRef, secretRef}),
	}, {
		name: "custom user port is used as upstream",
		rev: revision("bar", "foo",
			withContainers(userContainer(corev1.ContainerPort{ContainerPort: 8888})),
			WithRevisionAnnotations(map[string]string{Oauth2ProxyConfigSecretAnnotation: "my-secret"}),
		),
		want: wantContainer(4180, 8888, []corev1.EnvFromSource{secretRef}),
	}, {
		name: "user port collides with default proxy port",
		rev: revision("bar", "foo",
			withContainers(userContainer(corev1.ContainerPort{ContainerPort: 4180})),
			WithRevisionAnnotations(map[string]string{Oauth2ProxyConfigSecretAnnotation: "my-secret"}),
		),
		want: wantContainer(4181, 4180, []corev1.EnvFromSource{secretRef}),
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(Oauth2ProxyImageEnvVar, testOauth2ProxyImage)

			got, err := MakeOauth2ProxyContainer(test.rev, revConfig())
			if err != nil {
				t.Fatal("MakeOauth2ProxyContainer returned error:", err)
			}
			if diff := cmp.Diff(test.want, got); diff != "" {
				t.Errorf("MakeOauth2ProxyContainer() (-want, +got) =\n%s", diff)
			}
		})
	}
}

func TestRewriteQueueProxyUserPort(t *testing.T) {
	queueContainer := &corev1.Container{
		Name: QueueContainerName,
		Env: []corev1.EnvVar{
			{Name: "SERVING_NAMESPACE", Value: "foo"},
			{Name: "USER_PORT", Value: "8080"},
			{Name: "SERVING_SERVICE", Value: "bar"},
		},
	}

	rewriteQueueProxyUserPort(queueContainer, 9095)

	want := []corev1.EnvVar{
		{Name: "SERVING_NAMESPACE", Value: "foo"},
		{Name: "USER_PORT", Value: "9095"},
		{Name: "SERVING_SERVICE", Value: "bar"},
	}
	if diff := cmp.Diff(want, queueContainer.Env); diff != "" {
		t.Errorf("rewriteQueueProxyUserPort() env (-want, +got) =\n%s", diff)
	}
}

// userContainerOnPort returns a user container that listens on the given port.
func userContainerOnPort(port int32) corev1.Container {
	return corev1.Container{
		Name:           servingContainerName,
		Image:          "busybox",
		Ports:          []corev1.ContainerPort{{ContainerPort: port}},
		ReadinessProbe: withTCPReadinessProbe(int(port)),
	}
}

// withVarLogMount adds the var-log volume mount and pod info envs that makePodSpec
// adds to every non queue-proxy container when var-log collection is enabled.
func withVarLogMount(containerName string) containerOption {
	return func(c *corev1.Container) {
		c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{
			Name:        varLogVolume.Name,
			MountPath:   "/var/log",
			SubPathExpr: "$(K_INTERNAL_POD_NAMESPACE)_$(K_INTERNAL_POD_NAME)_" + containerName,
		})
		c.Env = append(c.Env, buildVarLogSubpathEnvs()...)
	}
}
