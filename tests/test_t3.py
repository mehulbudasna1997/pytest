import pytest
from kubernetes import client, config
import subprocess
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
# T3.1 — Rook operator & CRDs
# -------------------------
def test_rook_operator_and_crds(kube_clients):
    core_v1, _ = kube_clients

    print("\n=== T3.1 — Rook Operator Pods Status ===")
    pods = core_v1.list_namespaced_pod(namespace="rook-ceph").items
    failures = []
    for pod in pods:
        print(f"Pod: {pod.metadata.name}, Phase: {pod.status.phase}")
        # Check for CrashLoopBackOff in container statuses
        if pod.status.phase != "Running":
            failures.append(f"{pod.metadata.name} (Phase: {pod.status.phase})")
        if pod.status.container_statuses:
            for c in pod.status.container_statuses:
                if c.state.waiting and c.state.waiting.reason == "CrashLoopBackOff":
                    failures.append(f"{pod.metadata.name} ({c.name} in CrashLoopBackOff)")

    assert not failures, f"Rook operator pod issues: {failures}"

    print("\n=== Rook Ceph CRDs ===")
    try:
        result = subprocess.run(
            ["kubectl", "get", "crds"], capture_output=True, text=True, check=True
        )
        crds = [line for line in result.stdout.splitlines() if "ceph" in line]
        for crd in crds:
            print(crd)
        assert crds, "No Ceph CRDs found"
    except subprocess.CalledProcessError as e:
        pytest.fail(f"Failed to get CRDs: {e}")


# -------------------------
# T3.2 — CSI drivers registered
# -------------------------
def test_csi_drivers_registered(kube_clients):
    try:
        result = subprocess.run(
            ["kubectl", "get", "csidrivers"], capture_output=True, text=True, check=True
        )
        output = result.stdout
        print("\n=== CSI Drivers Registered ===")
        print(output)

        # assert "rbd.csi.ceph.com" in output, "rbd CSI driver not found"
        assert "cephfs.csi.ceph.com" in output, "cephfs CSI driver not found"
    except subprocess.CalledProcessError as e:
        pytest.fail(f"Failed to get CSI drivers: {e}")


# -------------------------
# T3.3 — Ceph Cluster connection
# -------------------------
def test_ceph_cluster_connection():
    print("\n=== T3.3 — Ceph Cluster Connection ===")
    try:
        result = subprocess.run(
            ["kubectl", "get", "cephcluster", "-n", "rook-ceph"],
            capture_output=True,
            text=True,
            check=True,
        )
        output = result.stdout
        print(output)

        # Look for cluster connection success message
        assert "Cluster connected successfully" in output, (
            "CephCluster did not report successful connection"
        )

        # Health check should be present and OK
        assert "HEALTH_OK" in output or "HEALTH_WARN" in output, (
            "CephCluster health not OK/WARN"
        )

        # Verify PHASE is Connected
        assert "Connected" in output, "CephCluster phase is not Connected"

    except subprocess.CalledProcessError as e:
        pytest.fail(f"Failed to get cephcluster: {e}")

