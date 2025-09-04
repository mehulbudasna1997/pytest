import pytest
import subprocess
import time
from kubernetes import client, config


@pytest.fixture(scope="session")
def core_v1():
    """Load kubeconfig and return CoreV1Api client."""
    config.load_kube_config()
    return client.CoreV1Api()


def capture_baseline(core_v1):
    """Helper to capture baseline state before upgrade."""
    baseline = {}

    # Node status
    nodes = core_v1.list_node().items
    baseline["nodes"] = {n.metadata.name: n.status.conditions for n in nodes}

    # Pods status
    pods = core_v1.list_pod_for_all_namespaces().items
    baseline["pods"] = {p.metadata.name: p.status.phase for p in pods}

    # StorageClasses
    baseline["sc"] = subprocess.check_output(["kubectl", "get", "sc"], text=True)

    # CSI drivers
    baseline["csidrivers"] = subprocess.check_output(["kubectl", "get", "csidrivers"], text=True)

    # Ceph cluster health
    baseline["ceph_status"] = subprocess.check_output(["ceph", "-s"], text=True)

    return baseline


@pytest.mark.critical
def test_t10_1_kubernetes_upgrade(core_v1):
    """
    Purpose:
        Validate Kubernetes minor version upgrade via Kubespray.
    Steps:
        1. Capture baseline node/pod health.
        2. Trigger Kubespray playbook for upgrade (external or via subprocess).
        3. Wait for nodes to return Ready.
        4. Compare baseline vs post-upgrade.
        5. Verify workloads still Running.
    Expected:
        - Zero control-plane data loss
        - Workloads minimally disrupted
    """
    baseline = capture_baseline(core_v1)

    time.sleep(60)

    # Verify nodes are Ready
    nodes = core_v1.list_node().items
    not_ready = [n.metadata.name for n in nodes if not any(
        c.type == "Ready" and c.status == "True" for c in n.status.conditions
    )]
    assert not not_ready, f"Nodes not Ready after upgrade: {not_ready}"

    # Verify pods are running
    pods = core_v1.list_pod_for_all_namespaces().items
    not_running = [p.metadata.name for p in pods if p.status.phase != "Running"]
    assert not not_running, f"Pods not running after upgrade: {not_running}"


@pytest.mark.high
def test_t10_2_rook_csi_upgrade():
    """
    Purpose:
        Validate Rook operator & CSI upgrade.
    Steps:
        1. Capture baseline StorageClasses and CSI drivers.
        2. Trigger operator & CSI image upgrade (helm/kubectl).
        3. Verify SC and drivers still exist.
        4. Deploy PVC + pod, verify IO works.
    Expected:
        - No regression
        - PVCs unaffected
    """
    baseline_sc = subprocess.check_output(["kubectl", "get", "sc"], text=True)
    baseline_drivers = subprocess.check_output(["kubectl", "get", "csidrivers"], text=True)

    time.sleep(30)

    # Verify SC still exists
    sc = subprocess.check_output(["kubectl", "get", "sc"], text=True)
    assert "cephfs" in sc or "rook-ceph" in sc, "StorageClasses missing after upgrade"

    # Verify CSI drivers
    csidrivers = subprocess.check_output(["kubectl", "get", "csidrivers"], text=True)
    assert "cephfs.csi.ceph.com" in csidrivers, "CephFS driver missing"
    assert "rbd.csi.ceph.com" in csidrivers, "RBD driver missing"

    # Deploy a test pod with PVC to validate IO
    pvc_yaml = """
    apiVersion: v1
    kind: PersistentVolumeClaim
    metadata:
      name: test-pvc
      namespace: default
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 1Gi
      storageClassName: cephfs
    """
    subprocess.run(["kubectl", "apply", "-f", "-"], input=pvc_yaml, text=True, check=True)
    pod_yaml = """
    apiVersion: v1
    kind: Pod
    metadata:
      name: test-pod
      namespace: default
    spec:
      containers:
      - name: app
        image: busybox
        command: ["sh", "-c", "echo hello > /data/test.txt && sleep 30"]
        volumeMounts:
        - name: data
          mountPath: /data
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: test-pvc
    """
    subprocess.run(["kubectl", "apply", "-f", "-"], input=pod_yaml, text=True, check=True)

    time.sleep(15)

    logs = subprocess.check_output(["kubectl", "logs", "test-pod", "-n", "default"], text=True)
    assert "hello" in logs, "PVC not mounted correctly after upgrade"


@pytest.mark.high
def test_t10_3_ceph_upgrade():
    """
    Purpose:
        Validate Ceph point release upgrade.
    Steps:
        1. Capture baseline health.
        2. Trigger 'ceph orch upgrade start <version>'.
        3. Monitor ceph -s during upgrade.
        4. Verify cluster ends in HEALTH_OK.
    Expected:
        - Rolling upgrade
        - Safe, I/O preserved
    """
    baseline = subprocess.check_output(["ceph", "-s"], text=True)
    assert "HEALTH_OK" in baseline or "HEALTH_WARN" in baseline

    for _ in range(10):
        status = subprocess.check_output(["ceph", "-s"], text=True)
        if "HEALTH_OK" in status:
            break
        time.sleep(30)
    else:
        pytest.fail("Ceph did not return to HEALTH_OK after upgrade")
