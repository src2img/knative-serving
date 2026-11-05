/*
Copyright 2026 IBM Corp.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package resources

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	v1 "knative.dev/serving/pkg/apis/serving/v1"
	"knative.dev/serving/pkg/reconciler/revision/config"
)

const (
	// OIDCAuthenticationEnvVar is the environment variable, set on the controller and webhook, that enables the OIDC authentication feature when it has a true value.
	OIDCAuthenticationEnvVar = "CE_OIDC_AUTHENTICATION"

	// Oauth2ProxyImageEnvVar is the environment variable, set on the controller, that holds the oauth2-proxy sidecar image.
	Oauth2ProxyImageEnvVar = "OAUTH2_PROXY_IMAGE"

	// Oauth2ProxyConfigSecretAnnotation is the Code Engine per-ksvc annotation that names
	// the Kubernetes Secret holding the oauth2-proxy configuration.
	// When present, the revision reconciler injects the ce-oauth2-proxy sidecar.
	Oauth2ProxyConfigSecretAnnotation = "codeengine.cloud.ibm.com/oauth2-proxy-config-secret"

	// Oauth2ProxyConfigConfigMapAnnotation is the annotation that holds the name of Kubernetes Configmap holding the oauth2-proxy additional/optional configuration
	Oauth2ProxyConfigConfigMapAnnotation = "codeengine.cloud.ibm.com/oauth2-proxy-config-configmap"

	// Oauth2ProxyConfigSecretType is the Kubernetes Secret type that the secret referenced by Oauth2ProxyConfigSecretAnnotation must have.
	Oauth2ProxyConfigSecretType corev1.SecretType = "codeengine.cloud.ibm.com/oauth2"

	Oauth2ProxyContainerName = "ce-oauth2-proxy"
)

const (
	oauth2ProxyContainerPortName = "ce-oauth2-port"

	oauth2ProxyHTTPAddressEnv = "OAUTH2_PROXY_HTTP_ADDRESS"
	oauth2ProxyUpstreamsEnv   = "OAUTH2_PROXY_UPSTREAMS"
)

type oauth2ProxyRevisionConfig struct {
	ConfigSecretName    string
	ConfigConfigMapName string
}

// IsOIDCAuthenticationEnabled reports whether the oauth2-proxy sidecar should be injected
// for this revision. It checks 2 conditions:
//  1. codeengine-level feature is enabled via the CE_OIDC_AUTHENTICATION environment variable.
//  2. Oauth2ProxyConfigSecretAnnotation that holds config secret name. (This is the per-revision).
func IsOIDCAuthenticationEnabled(rev *v1.Revision) bool {
	enabled, _ := strconv.ParseBool(os.Getenv(OIDCAuthenticationEnvVar))

	return enabled &&
		rev.Annotations[Oauth2ProxyConfigSecretAnnotation] != ""
}

func MakeOauth2ProxyContainer(rev *v1.Revision, _ *config.Config) (corev1.Container, error) {
	oauth2ProxyImage := os.Getenv(Oauth2ProxyImageEnvVar)
	if oauth2ProxyImage == "" {
		return corev1.Container{}, errors.New(Oauth2ProxyImageEnvVar + " environment variable cannot be empty or unset")
	}

	oauth2ProxyPort := getOauth2ProxyPort(rev)
	oauth2ProxyConfig := oauth2ProxyRevisionConfig{
		ConfigSecretName:    rev.Annotations[Oauth2ProxyConfigSecretAnnotation],
		ConfigConfigMapName: rev.Annotations[Oauth2ProxyConfigConfigMapAnnotation],
	}

	// getUserPort is a pure read of rev.Spec hence safe to call independently here.
	// userPort never changes after container construction, so this value is always correct.
	userPort := getUserPort(rev)

	oauth2ProxyContainer := corev1.Container{
		Name:  Oauth2ProxyContainerName,
		Image: oauth2ProxyImage,
		Ports: []corev1.ContainerPort{{
			Name:          oauth2ProxyContainerPortName,
			ContainerPort: oauth2ProxyPort,
		}},
		Env: buildOauth2ProxyEnv(oauth2ProxyPort, userPort),

		// The current append order causes the secret env have precendence over configmap.
		EnvFrom: append(
			buildOauth2ProxyEnvFromConfigMap(oauth2ProxyConfig.ConfigConfigMapName),
			buildOauth2ProxyEnvFromSecret(oauth2ProxyConfig.ConfigSecretName)...,
		),
		ReadinessProbe: buildOauth2ProxyReadinessProbe(oauth2ProxyPort),

		Lifecycle:       userLifecycle,
		SecurityContext: queueSecurityContext,
	}

	return oauth2ProxyContainer, nil
}

func buildOauth2ProxyEnv(oauth2ProxyPort, userPort int32) []corev1.EnvVar {
	return []corev1.EnvVar{{
		Name: oauth2ProxyHTTPAddressEnv,
		// This has to be 0.0.0.0 to allow the kubernetes readiness probes to work
		Value: fmt.Sprintf("http://0.0.0.0:%d", oauth2ProxyPort),
	}, {
		Name:  oauth2ProxyUpstreamsEnv,
		Value: fmt.Sprintf("http://127.0.0.1:%d", userPort),
	}}
}

func buildOauth2ProxyEnvFromSecret(name string) []corev1.EnvFromSource {
	if name == "" {
		return nil
	}
	return []corev1.EnvFromSource{{
		SecretRef: &corev1.SecretEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: name},
		},
	}}
}

func buildOauth2ProxyEnvFromConfigMap(name string) []corev1.EnvFromSource {
	if name == "" {
		return nil
	}
	return []corev1.EnvFromSource{{
		ConfigMapRef: &corev1.ConfigMapEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: name},
		},
	}}
}

func buildOauth2ProxyReadinessProbe(port int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
			Path:   "/ready",
			Port:   intstr.FromInt32(port),
			Scheme: corev1.URISchemeHTTP,
		}},
		PeriodSeconds:    1,
		TimeoutSeconds:   1,
		SuccessThreshold: 1,
		FailureThreshold: 3,
	}
}

func getOauth2ProxyPort(rev *v1.Revision) int32 {

	port := int32(4180)
	for {
		used := false
		for _, container := range append(rev.Spec.InitContainers, rev.Spec.Containers...) {
			for _, containerPort := range container.Ports {
				if containerPort.ContainerPort == port {
					used = true
					break
				}
			}
			if used {
				break
			}
		}
		if !used {
			return port
		}
		port++
	}
}

// rewriteQueueProxyUserPort overwrites the USER_PORT env var on the queue-proxy container
// so that it points at newPort instead of the original user-container port. This is required when the oauth2-proxy sidecar is injected: the flow chain becomes queue-proxy → oauth2-proxy → user container, so queue-proxy must forward to oauth2-proxy.
// Note: the queue-proxy still probes the user-container.
func rewriteQueueProxyUserPort(queueContainer *corev1.Container, newPort int32) {
	for i := range queueContainer.Env {
		if queueContainer.Env[i].Name == "USER_PORT" {
			queueContainer.Env[i].Value = strconv.Itoa(int(newPort))
			return
		}
	}
}
