from kubernetes.client import models as k8s

def env(name, value):
    return k8s.V1EnvVar(name=name, value=value)


def secret_env(name, secret, key):
    return k8s.V1EnvVar(
        name=name,
        value_from=k8s.V1EnvVarSource(
            secret_key_ref=k8s.V1SecretKeySelector(name=secret, key=key)
        ),
    )