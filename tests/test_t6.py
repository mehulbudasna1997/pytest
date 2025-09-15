import pytest
import subprocess
import time
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import os

# -------------------------
# Fixtures
# -------------------------

@pytest.fixture(scope="session")
def kube_clients():
    """Return CoreV1Api and AppsV1Api clients."""
    kubeconfig_path = os.environ.get("KUBECONFIG")
    try:
        if kubeconfig_path:
            config.load_kube_config(config_file=kubeconfig_path)
        else:
            config.load_kube_config()
    except Exception:
        config.load_incluster_config()

    core_v1 = client.CoreV1Api()
    apps_v1 = client.AppsV1Api()
    return core_v1, apps_v1


@pytest.fixture(scope="session")
def cephfs_namespace(kube_clients):
    """Create one namespace for all CephFS perf tests, cleanup at end."""
    core_v1, _ = kube_clients
    namespace = "test-cephfs-perf"
    ns_body = client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace))

    # Create ns
    try:
        core_v1.create_namespace(ns_body)
        print(f"✅ Created namespace {namespace}")
    except ApiException as e:
        if e.status == 409:
            print(f"⚠️ Namespace {namespace} already exists, reusing")
        else:
            raise

    yield namespace  # provide namespace name to tests

    # Cleanup after all tests in session
    print(f"🧹 Cleaning up namespace {namespace}...")
    try:
        core_v1.delete_namespace(namespace)
    except ApiException as e:
        if e.status != 404:
            raise
    print(f"✅ Namespace {namespace} deleted")


def wait_for_pvc_bound(core_v1, pvc_name, namespace, timeout=180):
    for _ in range(timeout):
        pvc = core_v1.read_namespaced_persistent_volume_claim(pvc_name, namespace)
        if pvc.status.phase == "Bound":
            return True
        time.sleep(2)
    raise TimeoutError(f"PVC {pvc_name} not bound after {timeout}s")


def wait_for_pod_running(core_v1, pod_name, namespace, timeout=180):
    for _ in range(timeout):
        pod = core_v1.read_namespaced_pod(pod_name, namespace)
        if pod.status.phase == "Running":
            return True
        time.sleep(2)
    raise TimeoutError(f"Pod {pod_name} not running after {timeout}s")

# -------------------------
# Tests
# -------------------------

def test_cephfs_single_pod_throughput(kube_clients, cephfs_namespace):
    """T6.1: Measure sequential write/read throughput in single CephFS pod."""
    core_v1, _ = kube_clients
    namespace = cephfs_namespace
    pvc_name = "cephfs-pvc-t61"
    pod_name = "cephfs-pod-t61"

    # Create PVC
    pvc = {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": pvc_name},
        "spec": {
            "accessModes": ["ReadWriteMany"],
            "resources": {"requests": {"storage": "1Gi"}},
            "storageClassName": "cephfs"
        }
    }
    core_v1.create_namespaced_persistent_volume_claim(namespace, pvc)
    wait_for_pvc_bound(core_v1, pvc_name, namespace)

    # Create Pod
    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": pod_name},
        "spec": {
            "containers": [{
                "name": "fio",
                "image": "busybox",
                "command": ["sleep", "3600"],
                "volumeMounts": [{"mountPath": "/data", "name": "cephfs-vol"}]
            }],
            "volumes": [{"name": "cephfs-vol", "persistentVolumeClaim": {"claimName": pvc_name}}]
        }
    }
    core_v1.create_namespaced_pod(namespace, pod)
    wait_for_pod_running(core_v1, pod_name, namespace)

    # Run throughput test (write + read)
    write = subprocess.check_output([
        "kubectl", "-n", namespace, "exec", pod_name, "--",
        "sh", "-c", "dd if=/dev/zero of=/data/testfile bs=1M count=200 oflag=direct 2>&1"
    ]).decode()
    print("WRITE throughput:\n", write)

    read = subprocess.check_output([
        "kubectl", "-n", namespace, "exec", pod_name, "--",
        "sh", "-c", "dd if=/data/testfile of=/dev/null bs=1M 2>&1"
    ]).decode()
    print("READ throughput:\n", read)

    assert "MB/s" in write and "MB/s" in read, "❌ dd did not report throughput"


def test_cephfs_multi_pod_parallel(kube_clients, cephfs_namespace):
    """T6.2: 5 pods writing in parallel to shared CephFS."""
    core_v1, _ = kube_clients
    namespace = cephfs_namespace
    pvc_name = "cephfs-pvc-t62"

    # PVC
    pvc = {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": pvc_name},
        "spec": {
            "accessModes": ["ReadWriteMany"],
            "resources": {"requests": {"storage": "1Gi"}},
            "storageClassName": "cephfs"
        }
    }
    core_v1.create_namespaced_persistent_volume_claim(namespace, pvc)
    wait_for_pvc_bound(core_v1, pvc_name, namespace)

    # Create 5 pods
    pod_names = []
    for i in range(5):
        pod_name = f"cephfs-pod-t62-{i}"
        pod = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": pod_name},
            "spec": {
                "containers": [{
                    "name": "writer",
                    "image": "busybox",
                    "command": ["sleep", "3600"],
                    "volumeMounts": [{"mountPath": "/data", "name": "cephfs-vol"}]
                }],
                "volumes": [{"name": "cephfs-vol", "persistentVolumeClaim": {"claimName": pvc_name}}]
            }
        }
        core_v1.create_namespaced_pod(namespace, pod)
        pod_names.append(pod_name)

    for pod in pod_names:
        wait_for_pod_running(core_v1, pod, namespace)

    # Parallel writes
    for i, pod in enumerate(pod_names):
        subprocess.Popen([
            "kubectl", "-n", namespace, "exec", pod, "--",
            "sh", "-c", f"dd if=/dev/zero of=/data/file{i} bs=1M count=100 oflag=direct"
        ])
    time.sleep(30)  # give them time

    # Verify files exist
    for i, pod in enumerate(pod_names):
        out = subprocess.check_output([
            "kubectl", "-n", namespace, "exec", pod_names[0], "--",
            "ls", f"/data/file{i}"
        ]).decode().strip()
        assert out == f"/data/file{i}" or out == f"file{i}"


def test_cephfs_capacity_warning(kube_clients, cephfs_namespace):
    """T6.3: Simulate filling pool >70% and check warnings."""
    core_v1, _ = kube_clients
    namespace = cephfs_namespace
    pvc_name = "cephfs-pvc-t63"
    pod_name = "cephfs-pod-t63"

    pvc = {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": pvc_name},
        "spec": {
            "accessModes": ["ReadWriteMany"],
            "resources": {"requests": {"storage": "5Gi"}},  # bigger claim
            "storageClassName": "cephfs"
        }
    }
    core_v1.create_namespaced_persistent_volume_claim(namespace, pvc)
    wait_for_pvc_bound(core_v1, pvc_name, namespace)

    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": pod_name},
        "spec": {
            "containers": [{
                "name": "filler",
                "image": "busybox",
                "command": ["sleep", "3600"],
                "volumeMounts": [{"mountPath": "/data", "name": "cephfs-vol"}]
            }],
            "volumes": [{"name": "cephfs-vol", "persistentVolumeClaim": {"claimName": pvc_name}}]
        }
    }
    core_v1.create_namespaced_pod(namespace, pod)
    wait_for_pod_running(core_v1, pod_name, namespace)

    # Simulate filling
    print("Filling PVC with ~4Gi to trigger capacity warnings...")
    subprocess.run([
        "kubectl", "-n", namespace, "exec", pod_name, "--",
        "sh", "-c", "dd if=/dev/zero of=/data/bigfile bs=1M count=4000 oflag=direct"
    ], check=False)

    # Check Ceph health (manual evidence step)
    print("👉 Check Ceph cluster health for nearfull/full warnings")

