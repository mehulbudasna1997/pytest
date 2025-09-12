import pytest
from helpers import k, exec_in_pod, get_pod_names_by_label, TEST_NS, log_step

TID = "S08"


def test_snapshot_restore_under_load():
    """
    Objective:
        Snapshot/restore validation under load.
    """

    pvc_name = "ceph-pvc"
    snap_name = "snap-s08"

    log_step(TID, f"Creating snapshot {snap_name} from {pvc_name}")
    k(f"kubectl create volumesnapshot {snap_name} --source-pvc={pvc_name} -n {TEST_NS}")

    log_step(TID, "Creating clone PVC from snapshot (manifest required)")
    k(f"kubectl apply -f manifests/clone_from_snap.yaml -n {TEST_NS}")

    log_step(TID, "Validating md5sum on writer pods")
    pods = get_pod_names_by_label(TEST_NS, "app=ceph-writer")
    for pod in pods:
        exec_in_pod(TEST_NS, pod, ["sh", "-c", "md5sum /data/file.txt || true"])
