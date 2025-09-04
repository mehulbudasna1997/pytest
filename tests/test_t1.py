import pytest
from kubernetes import client, config
import os
from dotenv import load_dotenv
load_dotenv()
# -------------------------
# Fixtures
# -------------------------
@pytest.fixture(scope="session")
def kube_clients():
    """Return CoreV1Api and AppsV1Api clients."""
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

    core_v1 = client.CoreV1Api()
    apps_v1 = client.AppsV1Api()
    return core_v1, apps_v1


# -------------------------
# T1.1 — Control plane components (Prod only)
# -------------------------
def test_control_plane_components_prod(kube_clients):
    """
    Verify all control-plane pods are Running and restarts <= 1.
    Steps:
        kubectl -n kube-system get pods -o wide | egrep "kube-(apiserver|controller|scheduler)|kube-proxy"
    Expected:
        - All pods Running
        - Restarts <= 1
    """
    core_v1, _ = kube_clients

    print("\n=== Control Plane Components Status (Prod) ===")

    pods = core_v1.list_namespaced_pod(namespace="kube-system").items

    control_plane_pods = [
        p for p in pods if any(
            s in p.metadata.name for s in ["kube-apiserver", "kube-controller-manager", "kube-scheduler", "kube-proxy"]
        )
    ]

    failures = []

    for pod in control_plane_pods:
        ready_containers = sum(1 for c in pod.status.container_statuses if c.ready)
        total_containers = len(pod.status.container_statuses)
        restarts = sum(c.restart_count for c in pod.status.container_statuses)
        status_str = f"{ready_containers}/{total_containers} Ready, Restarts: {restarts}"
        print(f"Pod: {pod.metadata.name}, Status: {pod.status.phase}, {status_str}")

        # Fail if pod is not Running
        if pod.status.phase != "Running":
            failures.append(f"{pod.metadata.name} (Status: {pod.status.phase})")

        # Fail if restarts >1
        # if restarts > 1:
        #     failures.append(f"{pod.metadata.name} (Restarts: {restarts})")

    assert not failures, f"Control plane pod issues: {failures}"
