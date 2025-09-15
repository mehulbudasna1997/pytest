import time
import pytest
from helpers import (
    ARTIFACTS_DIR,
    log_step,
    k,
    wait_for_rollout,
    ceph,
    exec_in_pod,
    get_pod_names_by_label,
    TEST_NS,
)

TID = "upg_01_rook_csi_upgrade"
ROOK_NS = "rook-ceph"
CSI_LABELS = [
    "app=csi-rbdplugin",
    "app=csi-cephfsplugin",
    "app=csi-provisioner",
]


@pytest.mark.upgrade
def test_rolling_upgrade_rook_csi():
    """
    UPG-01 — Rolling upgrade Rook/CSI
    Objective: Validate non-disruptive upgrade of Rook/CSI.
    Steps:
      - Bump images/tags
      - Observe rollout
      - Run storage smoke test (PVC + IO)
    Expected:
      - No disruption of ongoing IO
      - Provisioning pauses ≤ few minutes
    """

    # 1. Baseline: verify Ceph is healthy
    log_step(TID, "Checking initial Ceph cluster health")
    status = ceph("status", ARTIFACTS_DIR / f"{TID}_baseline_ceph_status.log")
    assert "HEALTH_OK" in status, f"[{TID}] Cluster not healthy before upgrade"

    # 2. Start a pod with PVC + IO workload (fio)
    pod_manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "fio-upgrade-tester", "namespace": TEST_NS, "labels": {"app": "fio-upgrade"}},
        "spec": {
            "containers": [
                {
                    "name": "fio",
                    "image": "ghcr.io/axboe/fio:latest",
                    "args": ["--name=upgrade", "--rw=write", "--size=50m", "--filename=/mnt/testfile"],
                    "volumeMounts": [{"mountPath": "/mnt", "name": "vol"}],
                }
            ],
            "volumes": [{"name": "vol", "persistentVolumeClaim": {"claimName": "cephfs-pvc"}}],
        },
    }
    from helpers import save_manifest, apply_manifest, wait_rollout
    pod_path = save_manifest(TID, "fio_pod", pod_manifest)
    apply_manifest(pod_path, TID, "fio_pod_apply")
    wait_rollout(TEST_NS, "pod/fio-upgrade-tester", TID, timeout=180)

    fio_pods = get_pod_names_by_label(TEST_NS, "app=fio-upgrade", TID, "fio_pods")
    assert fio_pods, f"[{TID}] fio-upgrade pod not found"
    fio_pod = fio_pods[0]

    # 3. Bump Rook and CSI images (simulate upgrade)
    log_step(TID, "Patching Rook operator with new image tag")
    # Example: update operator image (adjust tags/values for your env)
    k(f"-n {ROOK_NS} set image deploy/rook-ceph-operator rook-ceph-operator=rook/ceph:v1.14.0",
      ARTIFACTS_DIR / f"{TID}_rook_upgrade.log")

    # Upgrade CSI sidecars/plugins
    for label in CSI_LABELS:
        pods = get_pod_names_by_label(ROOK_NS, label, TID, f"csi_{label.replace('=','_')}")
        for pod in pods:
            k(f"-n {ROOK_NS} delete pod {pod}", ARTIFACTS_DIR / f"{TID}_{pod}_delete.log")

    # 4. Wait for rollouts to complete
    wait_for_rollout(ROOK_NS, "deploy/rook-ceph-operator", timeout=600, tid=TID)
    for label in CSI_LABELS:
        # ensure new pods spawn
        time.sleep(10)
        pods = get_pod_names_by_label(ROOK_NS, label, TID, f"post_upgrade_{label.replace('=','_')}")
        assert pods, f"[{TID}] No pods with {label} after upgrade"

    # 5. Validate IO continuity
    log_step(TID, "Checking fio pod is still running")
    k(f"-n {TEST_NS} get pod {fio_pod}", ARTIFACTS_DIR / f"{TID}_fio_status.log")
    exec_in_pod(TEST_NS, fio_pod, "ls -l /mnt/testfile", TID, "fio_check")

    # 6. Provision new PVC to check provisioning latency
    pvc_manifest = {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": "pvc-upgrade-test", "namespace": TEST_NS},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "1Gi"}},
            "storageClassName": "cephfs",
        },
    }
    pvc_path = save_manifest(TID, "pvc", pvc_manifest)
    apply_manifest(pvc_path, TID, "pvc_apply")

    # Check provisioning completes within 3 minutes
    start = time.time()
    while time.time() - start < 180:
        out = k(f"-n {TEST_NS} get pvc pvc-upgrade-test -o jsonpath='{{.status.phase}}'",
                ARTIFACTS_DIR / f"{TID}_pvc_status.log", check=False)
        if "Bound" in out:
            log_step(TID, "PVC bound successfully after upgrade")
            break
        time.sleep(5)
    else:
        raise AssertionError(f"[{TID}] PVC provisioning took longer than 3 minutes")

    # 7. Final cluster check
    status = ceph("status", ARTIFACTS_DIR / f"{TID}_final_ceph_status.log")
    assert "HEALTH_OK" in status, f"[{TID}] Cluster not healthy after upgrade"
