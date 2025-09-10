import pytest
from pathlib import Path
import yaml

from helpers import (
    save_manifest,
    apply_manifest,
    ensure_ns,
    delete_ns,
    wait_rollout,
    get_pod_names_by_label,
    exec_in_pod,
    k,
    ceph,
    ARTIFACTS_DIR,
    TEST_NS,
    CEPHFS_SC,
)

TID = "setup_cephfs"


@pytest.fixture(scope="session", autouse=True)
def setup_teardown_namespace():
    """Ensure test-cephfs namespace is clean before/after tests."""
    delete_ns(TEST_NS, TID)
    ensure_ns(TEST_NS, TID)
    # yield
    # delete_ns(TEST_NS, TID)


def test_create_pvc_and_pod():
    # 1. Create PVC manifest
    pvc_manifest = {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": "cephfs-pvc", "namespace": TEST_NS},
        "spec": {
            "accessModes": ["ReadWriteMany"],
            "resources": {"requests": {"storage": "1Gi"}},
            "storageClassName": CEPHFS_SC,
        },
    }
    pvc_path = save_manifest(TID, "pvc", pvc_manifest)
    apply_manifest(pvc_path, TID, "pvc_apply")

    # 2. Create Pod manifest mounting the PVC
    pod_manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "cephfs-tester", "namespace": TEST_NS, "labels": {"app": "cephfs-tester"}},
        "spec": {
            "containers": [
                {
                    "name": "tester",
                    "image": "busybox",
                    "command": ["sh", "-c", "sleep 36000"],
                    "volumeMounts": [{"mountPath": "/mnt/cephfs", "name": "cephfs-vol"}],
                }
            ],
            "volumes": [{"name": "cephfs-vol", "persistentVolumeClaim": {"claimName": "cephfs-pvc"}}],
        },
    }
    pod_path = save_manifest(TID, "pod", pod_manifest)
    apply_manifest(pod_path, TID, "pod_apply")

    # 3. Wait for pod to be running
    wait_rollout(TEST_NS, "pod/cephfs-tester", TID, timeout=240)

    # 4. Verify pod exists
    pods = get_pod_names_by_label(TEST_NS, "app=cephfs-tester", TID, "pods")
    assert pods, "No tester pod found"
    pod = pods[0]

    # 5. Write + read test file inside CephFS
    exec_in_pod(TEST_NS, pod, "echo 'cephfs test OK' > /mnt/cephfs/testfile.txt", TID, "write")
    out = exec_in_pod(TEST_NS, pod, "cat /mnt/cephfs/testfile.txt", TID, "read")
    assert "cephfs test OK" in out, "CephFS read/write failed"

    # 6. Collect evidence
    k(f"-n {TEST_NS} get pvc cephfs-pvc -o yaml", ARTIFACTS_DIR / f"{TID}_pvc_status.log")
    k(f"-n {TEST_NS} describe pod cephfs-tester", ARTIFACTS_DIR / f"{TID}_pod_desc.log")
    ceph("status", ARTIFACTS_DIR / f"{TID}_ceph_status.log")
