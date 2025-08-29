import os
import typing as t

import pytest
import warnings
from urllib3.exceptions import NotOpenSSLWarning
from kubernetes import client, config


@pytest.fixture(scope="session")
def kube_clients() -> t.Tuple[client.CoreV1Api, client.AppsV1Api]:
    """Provide initialized CoreV1Api and AppsV1Api clients.

    Loads kubeconfig from the default location (or the path set in the
    KUBECONFIG environment variable). Falls back to in-cluster config when
    running inside a Kubernetes Pod.
    """
    try:
        # Respect explicit kubeconfig if provided
        kubeconfig_path = os.environ.get("KUBECONFIG")
        if kubeconfig_path:
            config.load_kube_config(config_file=kubeconfig_path)
        else:
            config.load_kube_config()
    except Exception:
        # Fall back to in-cluster configuration (when tests run in a Pod)
        config.load_incluster_config()

    # Silence urllib3's NotOpenSSLWarning on macOS system Python environments
    warnings.filterwarnings("ignore", category=NotOpenSSLWarning)

    core_v1 = client.CoreV1Api()
    apps_v1 = client.AppsV1Api()

    # Quick connectivity check; if unreachable, skip the whole test session gracefully.
    try:
        core_v1.list_namespace(limit=1, _request_timeout=5)
    except Exception as exc:  # Broad by design to catch connection/auth issues
        pytest.skip(
            f"Skipping Kubernetes tests: cannot reach cluster ({exc}). "
            "Ensure KUBECONFIG is set or run inside a cluster.",
            allow_module_level=True,
        )

    return core_v1, apps_v1


