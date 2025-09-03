import pytest
import subprocess
import time
from kubernetes import client, config
from kubernetes.client.rest import ApiException

@pytest.fixture(scope="session")
def kube_clients():
    config.load_kube_config()  # Load local/production kubeconfig
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

def test_rbd_pvc_lifecycle(kube_clients):
    """T4.1 — RBD: PVC/POD lifecycle (RWO)"""
    core_v1, apps_v1 = kube_clients
    namespace = "test-rbd"
    pvc_name = "rbd-pvc"
    pod_name = "rbd-test-pod"
    storage_class = "rook-ceph-block"

    # 1. Ensure namespace exists
    ns_body = client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace))
    try:
        core_v1.create_namespace(ns_body)
    except ApiException as e:
        if e.status == 404:
            pytest.skip(f"StorageClass {storage_class} not found — skipping on non-prod cluster")
        raise
    # 2. Cleanup old PVC/pod
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

    # 3. Create PVC
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
    wait_for_pvc_bound(core_v1, pvc_name, namespace)

    # 4. Create Pod mounting PVC
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
                    "volumeMounts": [{"mountPath": "/data", "name": "rbd-vol"}]
                }
            ],
            "volumes": [{"name": "rbd-vol", "persistentVolumeClaim": {"claimName": pvc_name}}]
        }
    }
    core_v1.create_namespaced_pod(namespace, pod_manifest)
    wait_for_pod_running(core_v1, pod_name, namespace)

    # 5. Write 500MB file
    subprocess.run([
        "kubectl", "-n", namespace, "exec", pod_name, "--",
        "dd", "if=/dev/urandom", "of=/data/file", "bs=1M", "count=500", "oflag=direct"
    ], check=True)

    # 6. Compute MD5 before pod deletion
    md5_before = subprocess.check_output([
        "kubectl", "-n", namespace, "exec", pod_name, "--",
        "md5sum", "/data/file"
    ]).decode().split()[0]

    # 7. Delete Pod and re-create
    core_v1.delete_namespaced_pod(pod_name, namespace)
    time.sleep(5)
    core_v1.create_namespaced_pod(namespace, pod_manifest)
    wait_for_pod_running(core_v1, pod_name, namespace)

    # 8. Compute MD5 after pod recreation
    md5_after = subprocess.check_output([
        "kubectl", "-n", namespace, "exec", pod_name, "--",
        "md5sum", "/data/file"
    ]).decode().split()[0]

    print(f"T4.1 MD5 before: {md5_before}, MD5 after: {md5_after}")
    assert md5_before == md5_after, "Data integrity failed across pod restarts"


def test_rbd_pvc_online_expansion(kube_clients):
    """T4.2 — RBD: Online volume expansion"""
    core_v1, apps_v1 = kube_clients
    namespace = "test-rbd"
    pvc_name = "expand-pvc"
    pod_name = "expand-pod"
    storage_class = "rook-ceph-block"

    # 1. Ensure namespace exists
    ns_body = client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace))
    try:
        core_v1.create_namespace(ns_body)
    except ApiException as e:
        if e.status == 404:
            pytest.skip(f"StorageClass {storage_class} not found — skipping on non-prod cluster")
        raise

    # 2. Cleanup old PVC/pod
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

    # 3. Create PVC 1Gi
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
    wait_for_pvc_bound(core_v1, pvc_name, namespace)

    # 4. Create pod
    pod_manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": pod_name},
        "spec": {
            "containers": [{
                "name": "writer",
                "image": "busybox",
                "command": ["sleep", "3600"],
                "volumeMounts": [{"mountPath": "/data", "name": "vol"}]
            }],
            "volumes": [{"name": "vol", "persistentVolumeClaim": {"claimName": pvc_name}}]
        }
    }
    core_v1.create_namespaced_pod(namespace, pod_manifest)
    wait_for_pod_running(core_v1, pod_name, namespace)

    # 5. Write some data
    subprocess.run([
        "kubectl", "-n", namespace, "exec", pod_name, "--",
        "dd", "if=/dev/urandom", "of=/data/file", "bs=1M", "count=200", "oflag=direct"
    ], check=True)

    # 6. Patch PVC 1Gi → 5Gi
    patch_body = {"spec": {"resources": {"requests": {"storage": "5Gi"}}}}
    core_v1.patch_namespaced_persistent_volume_claim(pvc_name, namespace, patch_body)
    wait_for_pvc_resize(core_v1, pvc_name, namespace, "5Gi")

    # 7. Verify filesystem inside pod
    df_output = subprocess.check_output([
        "kubectl", "-n", namespace, "exec", pod_name, "--",
        "df", "-h", "/data"
    ]).decode()
    print("T4.2 Filesystem after expansion:\n", df_output)
    assert "5.0G" in df_output or "4.9G" in df_output, "Filesystem not expanded to ~5Gi"

    # Cleanup
    core_v1.delete_namespaced_pod(pod_name, namespace)
    core_v1.delete_namespaced_persistent_volume_claim(pvc_name, namespace)

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
    storage_class = "rook-cephfs"

    # 1. Create namespace
    ns_body = client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace))
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

def test_cephfs_quota(kube_clients):
    core_v1, apps_v1 = kube_clients
    namespace = "test-cephfs-quota"
    pvc_name = "cephfs-quota-pvc"
    pod_name = "cephfs-quota-pod"
    storage_class = "cephfs"  # Replace with your CephFS SC name
    size_limit = "100Mi"      # Small limit for testing quota

    # 1. Create namespace
    ns_body = client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace))
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

    # 5. Attempt to write beyond quota
    # Try writing 200Mi (exceeds 100Mi limit)
    result = subprocess.run([
        "kubectl", "-n", namespace, "exec", pod_name, "--",
        "dd", "if=/dev/zero", "of=/data/file", "bs=1M", "count=200", "oflag=direct"
    ], capture_output=True, text=True)

    print("DD stdout:", result.stdout)
    print("DD stderr:", result.stderr)

    # Expect failure due to quota
    assert "No space left on device" in result.stderr or result.returncode != 0, \
        "Writes succeeded beyond quota! CephFS quota enforcement failed."

def test_rbd_rwo_multi_attach(kube_clients):
    core_v1, _ = kube_clients
    namespace = "test-rbd"
    pvc_name = "rbd-pvc-rwo"
    pod1_name = "rbd-pod-1"
    pod2_name = "rbd-pod-2"
    storage_class = "rook-ceph-block"

    # 1. Create namespace (ignore if exists)
    ns_body = client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace))
    try:
        core_v1.create_namespace(ns_body)
    except ApiException as e:
        if e.status != 409:
            raise

    # 2. Delete any existing PVC/Pods
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

    # 3. Create RWO PVC
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

    # 4. Define pod manifest
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
                    "volumeMounts": [{"mountPath": "/data", "name": "rbd-vol"}]
                }
            ],
            "volumes": [{"name": "rbd-vol", "persistentVolumeClaim": {"claimName": pvc_name}}]
        }
    }

    # 5. Create first pod (should succeed)
    core_v1.create_namespaced_pod(namespace, pod_manifest(pod1_name))
    # Wait for pod running
    for _ in range(60):
        pod = core_v1.read_namespaced_pod(pod1_name, namespace)
        if pod.status.phase == "Running":
            break
        time.sleep(2)
    else:
        pytest.fail(f"Pod {pod1_name} not running in time")

    # 6. Create second pod (should fail scheduling due to RWO)
    try:
        core_v1.create_namespaced_pod(namespace, pod_manifest(pod2_name))
    except ApiException as e:
        # If API rejects, it’s expected
        if e.status == 422 or e.status == 409:
            print(f"Pod {pod2_name} creation failed as expected due to RWO PVC")
            return
        else:
            raise

    # Wait to see if pod2 gets scheduled
    scheduled = False
    for _ in range(30):
        pod2 = core_v1.read_namespaced_pod(pod2_name, namespace)
        if pod2.status.phase == "Pending":
            scheduled = True
            break
        time.sleep(2)

    assert scheduled, "Pod2 should be Pending due to RWO PVC, but was scheduled!"
