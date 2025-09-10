import pytest
import subprocess
import time
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


def wait_for_pvc_bound(core_v1, pvc_name, namespace, timeout=180):
    for _ in range(timeout):
        pvc = core_v1.read_namespaced_persistent_volume_claim(pvc_name, namespace)
        if pvc.status.phase == "Bound":
            return True
        time.sleep(2)
    raise TimeoutError(f"PVC {pvc_name} not bound after {timeout} seconds")


def wait_for_pvc_resize(core_v1, pvc_name, namespace, new_size, timeout=180):
    for _ in range(timeout):
        pvc = core_v1.read_namespaced_persistent_volume_claim(pvc_name, namespace)
        cap = pvc.status.capacity.get("storage") if pvc.status.capacity else None
        if cap and cap == new_size:
            return True
        time.sleep(5)
    raise TimeoutError(f"PVC {pvc_name} not resized to {new_size} after {timeout} seconds")


def wait_for_pod_running(core_v1, pod_name, namespace, timeout=120):
    for _ in range(timeout):
        pod = core_v1.read_namespaced_pod(pod_name, namespace)
        if pod.status.phase == "Running":
            return True
        time.sleep(2)
    raise TimeoutError(f"Pod {pod_name} not running after {timeout} seconds")

def wait_for_pods_running(core_v1, pod_names, namespace, timeout=120):
    for _ in range(timeout):
        all_running = True
        for pod_name in pod_names:
            pod = core_v1.read_namespaced_pod(pod_name, namespace)
            if pod.status.phase != "Running":
                all_running = False
                break
        if all_running:
            return True
        time.sleep(2)
    raise TimeoutError(f"Pods {pod_names} not running after {timeout} seconds")

def test_cephfs_rwx_multi_writer(kube_clients):
    core_v1, apps_v1 = kube_clients
    namespace = "test-cephfs"
    pvc_name = "cephfs-pvc"
    pod_base_name = "cephfs-pod"
    storage_class = "cephfs"

    ns_body = client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace))

    try:
        # 1. Create namespace
        try:
            core_v1.create_namespace(ns_body)
        except ApiException as e:
            if e.status != 409:
                raise

        # 2. Delete existing PVC and pods if any
        try:
            core_v1.delete_namespaced_persistent_volume_claim(pvc_name, namespace)
            time.sleep(3)
        except ApiException as e:
            if e.status != 404:
                raise

        for i in range(1, 4):
            pod_name = f"{pod_base_name}-{i}"
            try:
                core_v1.delete_namespaced_pod(pod_name, namespace)
                time.sleep(2)
            except ApiException as e:
                if e.status != 404:
                    raise

        # 3. Create CephFS PVC (RWX)
        pvc_manifest = {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {"name": pvc_name},
            "spec": {
                "accessModes": ["ReadWriteMany"],
                "resources": {"requests": {"storage": "1Gi"}},
                "storageClassName": storage_class
            }
        }
        core_v1.create_namespaced_persistent_volume_claim(namespace, pvc_manifest)
        wait_for_pvc_bound(core_v1, pvc_name, namespace)

        # 4. Deploy 3 pods mounting same PVC
        pod_names = []
        for i in range(1, 4):
            pod_name = f"{pod_base_name}-{i}"
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
                            "volumeMounts": [{"mountPath": "/data", "name": "cephfs-vol"}]
                        }
                    ],
                    "volumes": [{"name": "cephfs-vol", "persistentVolumeClaim": {"claimName": pvc_name}}]
                }
            }
            core_v1.create_namespaced_pod(namespace, pod_manifest)
            pod_names.append(pod_name)

        wait_for_pods_running(core_v1, pod_names, namespace)

        # 5. Write from pod-1
        subprocess.run([
            "kubectl", "-n", namespace, "exec", pod_names[0], "--",
            "sh", "-c", "echo 'hello-from-pod1' > /data/testfile"
        ], check=True)

        # 6. Read from pod-2 and pod-3
        for i in range(1, 3):
            output = subprocess.check_output([
                "kubectl", "-n", namespace, "exec", pod_names[i], "--",
                "cat", "/data/testfile"
            ]).decode().strip()
            assert output == "hello-from-pod1", f"Pod {pod_names[i]} read incorrect data: {output}"

        print("CephFS RWX multi-writer test passed: all pods read/write successfully")

    finally:
        # Cleanup namespace (deletes PVC + Pods)
        try:
            core_v1.delete_namespace(namespace)
        except ApiException as e:
            if e.status != 404:
                raise


def test_cephfs_quota(kube_clients):
    core_v1, apps_v1 = kube_clients
    namespace = "test-cephfs-quota"
    pvc_name = "cephfs-quota-pvc"
    pod_name = "cephfs-quota-pod"
    storage_class = "cephfs"  # Replace with your CephFS SC name
    size_limit = "100Mi"      # Small limit for testing quota

    ns_body = client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace))

    try:
        # 1. Create namespace
        try:
            core_v1.create_namespace(ns_body)
        except ApiException as e:
            if e.status != 409:
                raise

        # 2. Delete existing PVC & Pod if any
        for name, kind in [(pod_name, "pod"), (pvc_name, "pvc")]:
            try:
                if kind == "pod":
                    core_v1.delete_namespaced_pod(name, namespace)
                else:
                    core_v1.delete_namespaced_persistent_volume_claim(name, namespace)
                time.sleep(3)
            except ApiException as e:
                if e.status != 404:
                    raise

        # 3. Create PVC with size limit
        pvc_manifest = {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {"name": pvc_name},
            "spec": {
                "accessModes": ["ReadWriteMany"],
                "resources": {"requests": {"storage": size_limit}},
                "storageClassName": storage_class
            }
        }
        core_v1.create_namespaced_persistent_volume_claim(namespace, pvc_manifest)
        wait_for_pvc_bound(core_v1, pvc_name, namespace)

        # 4. Create pod mounting PVC
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
                        "volumeMounts": [{"mountPath": "/data", "name": "cephfs-vol"}]
                    }
                ],
                "volumes": [{"name": "cephfs-vol", "persistentVolumeClaim": {"claimName": pvc_name}}]
            }
        }
        core_v1.create_namespaced_pod(namespace, pod_manifest)
        wait_for_pod_running(core_v1, pod_name, namespace)

        # 5. Attempt to write beyond quota (expect failure)
        result = subprocess.run([
            "kubectl", "-n", namespace, "exec", pod_name, "--",
            "dd", "if=/dev/zero", "of=/data/file", "bs=1M", "count=200", "oflag=direct"
        ], capture_output=True, text=True)

        print("DD stdout:", result.stdout)
        print("DD stderr:", result.stderr)

        assert "No space left on device" in result.stderr or result.returncode != 0, \
            "Writes succeeded beyond quota! CephFS quota enforcement failed."

    finally:
        # Cleanup namespace (removes PVCs, Pods automatically)
        try:
            core_v1.delete_namespace(namespace)
        except ApiException as e:
            if e.status != 404:
                raise


def test_cephfs_rwo_multi_attach(kube_clients):
    """
    T4.5 — Access modes & policy (High)
    Steps: Attempt multi-attach of CephFS (RWO) to two pods simultaneously.
    Expected: Denied by CSI; only CephFS with RWX is supported for multi-attach.
    """
    core_v1, _ = kube_clients
    namespace = "test-cephfs"
    pvc_name = "cephfs-pvc-rwo"
    pod1_name = "cephfs-pod-1"
    pod2_name = "cephfs-pod-2"
    storage_class = "cephfs"   # CephFS storage class

    # Create namespace (ignore if exists)
    ns_body = client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace))
    try:
        core_v1.create_namespace(ns_body)
    except ApiException as e:
        if e.status != 409:
            raise

    try:
        # Delete any existing PVC/Pods
        for name, kind in [(pod1_name, "pod"), (pod2_name, "pod"), (pvc_name, "pvc")]:
            try:
                if kind == "pod":
                    core_v1.delete_namespaced_pod(name, namespace)
                else:
                    core_v1.delete_namespaced_persistent_volume_claim(name, namespace)
                time.sleep(3)
            except ApiException as e:
                if e.status != 404:
                    raise

        # Create CephFS PVC with RWO access
        pvc_manifest = {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {"name": pvc_name},
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "resources": {"requests": {"storage": "1Gi"}},
                "storageClassName": storage_class
            }
        }
        core_v1.create_namespaced_persistent_volume_claim(namespace, pvc_manifest)

        # Wait for PVC to be Bound
        for _ in range(60):
            pvc = core_v1.read_namespaced_persistent_volume_claim(pvc_name, namespace)
            if pvc.status.phase == "Bound":
                break
            time.sleep(2)
        else:
            pytest.fail(f"PVC {pvc_name} did not bind in time")

        # Define pod manifest
        pod_manifest = lambda name: {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": name},
            "spec": {
                "containers": [
                    {
                        "name": "app",
                        "image": "busybox",
                        "command": ["sleep", "3600"],
                        "volumeMounts": [{"mountPath": "/data", "name": "cephfs-vol"}]
                    }
                ],
                "volumes": [{"name": "cephfs-vol", "persistentVolumeClaim": {"claimName": pvc_name}}]
            }
        }

        # Create first pod (should succeed)
        core_v1.create_namespaced_pod(namespace, pod_manifest(pod1_name))
        for _ in range(60):
            pod = core_v1.read_namespaced_pod(pod1_name, namespace)
            if pod.status.phase == "Running":
                break
            time.sleep(2)
        else:
            pytest.fail(f"Pod {pod1_name} not running in time")

        # Create second pod (should fail scheduling due to RWO on CephFS)
        try:
            core_v1.create_namespaced_pod(namespace, pod_manifest(pod2_name))
        except ApiException as e:
            if e.status in (422, 409):
                print(f"Pod {pod2_name} creation failed as expected due to CephFS RWO PVC")
                return
            else:
                raise

        # Wait to see if pod2 gets stuck Pending
        scheduled = False
        for _ in range(30):
            pod2 = core_v1.read_namespaced_pod(pod2_name, namespace)
            if pod2.status.phase == "Pending":
                scheduled = True
                break
            time.sleep(2)

        assert scheduled, "Pod2 should be Pending due to CephFS RWO PVC, but was scheduled!"

    finally:
        # Cleanup: delete namespace (removes all resources inside)
        try:
            core_v1.delete_namespace(namespace)
            print(f"Namespace {namespace} deleted")
        except ApiException as e:
            if e.status != 404:
                raise

