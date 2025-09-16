import pytest
import time
from helpers import (
    exec_in_pod, get_pod_names_by_label,
    TEST_NS, log_step, save_manifest, apply_manifest, MANIFESTS_DIR
)

TID = "S08"

PVC_NAME = "ceph-pvc"
SNAP_NAME = "snap-s08"
CLONE_PVC = "clone-pvc-s08"
CLONE_POD = "clone-pod-s08"


def test_snapshot_restore_under_load():
    """
    S-08: Snapshot/Restore under load
    Objective:
        Confirm data protection under traffic.
    Steps:
        - Create checkpoint file on source pod while it is writing
        - Take VolumeSnapshot of PVC
        - Create clone PVC + Pod from snapshot
        - Compare md5sum between source checkpoint and clone
    Expected:
        - Snapshot is point-in-time consistent (CSI guarantees)
    """

    # Step 1: Prepare source pod(s)
    pods = get_pod_names_by_label(TEST_NS, "app=ceph-writer")
    assert pods, f"[{TID}] No ceph-writer pods found in {TEST_NS}"

    src_pod = pods[0]
    src_file = "/data/file.txt"
    checkpoint = "/data/file_checkpoint.txt"

    # Write a checkpoint copy of the file while app continues writes
    log_step(TID, f"Creating checkpoint file on {src_pod}")
    exec_in_pod(TEST_NS, src_pod, f"cp {src_file} {checkpoint}", TID, "checkpoint")

    # Calculate md5sum of checkpoint
    src_md5 = exec_in_pod(TEST_NS, src_pod, f"md5sum {checkpoint}", TID, "src_md5").split()[0]

    # Step 2: Create snapshot
    log_step(TID, f"Creating VolumeSnapshot {SNAP_NAME} from PVC {PVC_NAME}")
    snap_manifest = {
        "apiVersion": "snapshot.storage.k8s.io/v1",
        "kind": "VolumeSnapshot",
        "metadata": {"name": SNAP_NAME, "namespace": TEST_NS},
        "spec": {
            "source": {"persistentVolumeClaimName": PVC_NAME},
            "volumeSnapshotClassName": "csi-cephfsplugin-snapclass",  # adjust if needed
        },
    }
    save_manifest(TID, "snapshot", snap_manifest)
    apply_manifest(MANIFESTS_DIR / f"{TID}_snapshot.yaml", TID, "snap_apply")

    # Give time for snapshot creation
    time.sleep(10)

    # Step 3: Create clone PVC from snapshot
    log_step(TID, f"Creating clone PVC {CLONE_PVC} from snapshot {SNAP_NAME}")
    clone_manifest = {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": CLONE_PVC, "namespace": TEST_NS},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "1Gi"}},
            "storageClassName": "cephfs",  # adjust if needed
            "dataSource": {"name": SNAP_NAME, "kind": "VolumeSnapshot", "apiGroup": "snapshot.storage.k8s.io"},
        },
    }
    save_manifest(TID, "clone_pvc", clone_manifest)
    apply_manifest(MANIFESTS_DIR / f"{TID}_clone_pvc.yaml", TID, "clone_pvc_apply")

    # Step 4: Create pod using clone PVC
    log_step(TID, f"Creating pod {CLONE_POD} mounting clone PVC")
    pod_manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": CLONE_POD, "namespace": TEST_NS},
        "spec": {
            "containers": [
                {
                    "name": "app",
                    "image": "busybox:1.36",
                    "command": ["sh", "-c", "sleep 60"],
                    "volumeMounts": [{"mountPath": "/data", "name": "vol"}],
                }
            ],
            "volumes": [{"name": "vol", "persistentVolumeClaim": {"claimName": CLONE_PVC}}],
            "restartPolicy": "Never",
        },
    }
    save_manifest(TID, "clone_pod", pod_manifest)
    apply_manifest(MANIFESTS_DIR / f"{TID}_clone_pod.yaml", TID, "clone_pod_apply")

    # Wait briefly for pod start
    time.sleep(10)

    # Step 5: Verify md5sum in clone
    clone_md5 = exec_in_pod(TEST_NS, CLONE_POD, f"md5sum {src_file}", TID, "clone_md5").split()[0]

    log_step(TID, f"Source checkpoint md5={src_md5}, Clone md5={clone_md5}")
    assert clone_md5 == src_md5, f"[{TID}] Snapshot not point-in-time consistent"

    log_step(TID, "PASS: Snapshot is point-in-time consistent under load")
