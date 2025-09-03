import pytest
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from kubernetes import client, config
from kubernetes.client.rest import ApiException
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

def wait_for_pod_running(core_v1, pod_name, namespace, timeout=120):
    for _ in range(timeout):
        pod = core_v1.read_namespaced_pod(pod_name, namespace)
        if pod.status.phase == "Running":
            return True
        time.sleep(2)
    raise TimeoutError(f"Pod {pod_name} not running after {timeout} seconds")

@pytest.mark.prod
def test_single_pod_storage_throughput(kube_clients):
    core_v1, _ = kube_clients
    namespace = "test-rbd-throughput"
    pod_name = "throughput-pod"
    pvc_name = "throughput-pvc"
    storage_class_rbd = "rook-ceph-block"
    storage_class_cephfs = "rook-cephfs"

    # 1. Create namespace
    ns_body = client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace))
    try:
        core_v1.create_namespace(ns_body)
    except ApiException as e:
        if e.status != 409:
            raise

    # 2. Create RBD PVC
    pvc_rbd = {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": pvc_name},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "1Gi"}},
            "storageClassName": storage_class_rbd
        }
    }
    try:
        core_v1.create_namespaced_persistent_volume_claim(namespace, pvc_rbd)
    except ApiException as e:
        if e.status != 409:
            raise
    time.sleep(5)

    # 3. Deploy Pod mounting PVC
    pod_manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": pod_name},
        "spec": {
            "containers": [
                {
                    "name": "writer",
                    "image": "busybox",
                    "command": ["sleep", "3600"],
                    "volumeMounts": [{"mountPath": "/data", "name": "vol"}]
                }
            ],
            "volumes": [{"name": "vol", "persistentVolumeClaim": {"claimName": pvc_name}}]
        }
    }
    try:
        core_v1.create_namespaced_pod(namespace, pod_manifest)
    except ApiException as e:
        if e.status != 409:
            raise
    wait_for_pod_running(core_v1, pod_name, namespace)

    # 4. Measure sequential write (dd)
    result_write = subprocess.check_output([
        "kubectl", "-n", namespace, "exec", pod_name, "--",
        "dd", "if=/dev/zero", "of=/data/testfile", "bs=1M", "count=500", "oflag=direct"
    ]).decode()
    print("Write throughput:\n", result_write)

    # 5. Measure sequential read
    result_read = subprocess.check_output([
        "kubectl", "-n", namespace, "exec", pod_name, "--",
        "dd", "if=/data/testfile", "of=/dev/null", "bs=1M", "count=500", "iflag=direct"
    ]).decode()
    print("Read throughput:\n", result_read)

def test_multi_pod_mixed_workload(kube_clients):
    core_v1, _ = kube_clients
    namespace = "test-mixed-workload"
    rbd_prefix = "rbd-pod"
    cephfs_prefix = "cephfs-pod"
    pvc_rbd = "rbd-pvc"
    pvc_cephfs = "cephfs-pvc"
    storage_rbd = "rook-ceph-block"
    storage_cephfs = "rook-cephfs"

    # 1. Create namespace
    ns_body = client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace))
    try:
        core_v1.create_namespace(ns_body)
    except ApiException as e:
        if e.status != 409:
            raise

    # 2. Create RBD PVCs (5 pods)
    for i in range(5):
        name = f"{pvc_rbd}-{i}"
        pvc_manifest = {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {"name": name},
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "resources": {"requests": {"storage": "1Gi"}},
                "storageClassName": storage_rbd
            }
        }
        try:
            core_v1.create_namespaced_persistent_volume_claim(namespace, pvc_manifest)
        except ApiException:
            pass

    # 3. Create shared CephFS PVC
    pvc_manifest = {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": pvc_cephfs},
        "spec": {
            "accessModes": ["ReadWriteMany"],
            "resources": {"requests": {"storage": "1Gi"}},
            "storageClassName": storage_cephfs
        }
    }
    try:
        core_v1.create_namespaced_persistent_volume_claim(namespace, pvc_manifest)
    except ApiException:
        pass
    time.sleep(5)

    # 4. Deploy pods
    def deploy_pod(pod_name, pvc_name):
        pod_manifest = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": pod_name},
            "spec": {
                "containers": [
                    {
                        "name": "worker",
                        "image": "busybox",
                        "command": ["sleep", "3600"],
                        "volumeMounts": [{"mountPath": "/data", "name": "vol"}]
                    }
                ],
                "volumes": [{"name": "vol", "persistentVolumeClaim": {"claimName": pvc_name}}]
            }
        }
        try:
            core_v1.create_namespaced_pod(namespace, pod_manifest)
        except ApiException:
            pass
        # wait pod running
        for _ in range(30):
            pod = core_v1.read_namespaced_pod(pod_name, namespace)
            if pod.status.phase == "Running":
                return True
            time.sleep(2)
        raise TimeoutError(f"{pod_name} not running")

    # Deploy RBD and CephFS pods in parallel
    with ThreadPoolExecutor(max_workers=10) as executor:
        for i in range(5):
            executor.submit(deploy_pod, f"{rbd_prefix}-{i}", f"{pvc_rbd}-{i}")
            executor.submit(deploy_pod, f"{cephfs_prefix}-{i}", pvc_cephfs)
    time.sleep(5)

    # 5. Write 100MB each in parallel
    def write_100mb(pod_name):
        result = subprocess.run([
            "kubectl", "-n", namespace, "exec", pod_name, "--",
            "dd", "if=/dev/zero", f"of=/data/{pod_name}_file", "bs=1M", "count=100", "oflag=direct"
        ], capture_output=True)
        if result.returncode != 0:
            raise RuntimeError(f"{pod_name} write failed: {result.stderr.decode()}")
        return result.stdout.decode()

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for i in range(5):
            futures.append(executor.submit(write_100mb, f"{rbd_prefix}-{i}"))
            futures.append(executor.submit(write_100mb, f"{cephfs_prefix}-{i}"))

        for f in futures:
            print(f.result())

def test_rbd_capacity_near_full(kube_clients):
    """
    T6.3 — Capacity & near-full behavior
    Steps:
    1. Deploy pod with RBD-backed PVC.
    2. Write data incrementally to fill >70%.
    3. Observe warnings/throttling behavior.
    Expected: Warnings fire; no write failures before full thresholds.
    """
    core_v1, _ = kube_clients
    namespace = "test-rbd-capacity"
    pvc_name = "capacity-pvc"
    pod_name = "capacity-pod"
    storage_class = "rook-ceph-block"

    # 1. Create namespace
    ns_body = client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace))
    try:
        core_v1.create_namespace(ns_body)
    except ApiException as e:
        if e.status != 409:
            raise

    # 2. Create PVC
    pvc_manifest = {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": pvc_name},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "5Gi"}},  # example size
            "storageClassName": storage_class
        }
    }
    try:
        core_v1.create_namespaced_persistent_volume_claim(namespace, pvc_manifest)
    except ApiException as e:
        if e.status != 409:
            raise
    time.sleep(5)

    # 3. Deploy Pod
    pod_manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": pod_name},
        "spec": {
            "containers": [
                {
                    "name": "writer",
                    "image": "busybox",
                    "command": ["sleep", "3600"],
                    "volumeMounts": [{"mountPath": "/data", "name": "vol"}]
                }
            ],
            "volumes": [{"name": "vol", "persistentVolumeClaim": {"claimName": pvc_name}}]
        }
    }
    try:
        core_v1.create_namespaced_pod(namespace, pod_manifest)
    except ApiException as e:
        if e.status != 409:
            raise

    # Wait for Pod running
    for _ in range(60):
        pod = core_v1.read_namespaced_pod(pod_name, namespace)
        if pod.status.phase == "Running":
            break
        time.sleep(2)
    else:
        raise TimeoutError(f"Pod {pod_name} did not reach Running state")

    # 4. Write incrementally to fill >70%
    for i in range(1, 6):  # 5 chunks of 1Gi each (adjust as needed)
        print(f"Writing chunk {i} of 1Gi")
        result = subprocess.run([
            "kubectl", "-n", namespace, "exec", pod_name, "--",
            "dd", "if=/dev/zero", f"of=/data/file{i}", "bs=1M", "count=1024", "oflag=direct"
        ], capture_output=True)
        print(result.stdout.decode(), result.stderr.decode())
        if result.returncode != 0:
            raise RuntimeError(f"Write failed at chunk {i}: {result.stderr.decode()}")

    # 5. Optional: Check Ceph usage and warnings
    print("Check Ceph pool usage and warnings manually via 'ceph -s' or Ceph dashboard")
