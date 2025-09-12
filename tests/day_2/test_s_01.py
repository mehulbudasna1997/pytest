import pytest
from helpers import (
    k,
    exec_in_pod,
    get_pod_names_by_label,
    ARTIFACTS_DIR,
    TEST_NS,
    log_step,
    wait_for_rollout,
)

TID = "S01"


def test_cephfs_rwx_integrity_under_pod_restarts():
    """
    Objective:
        Validate CephFS RWX multi-writer correctness under pod restarts.
    """

    log_step(TID, "Fetching CephFS shared pods")
    pod_names = get_pod_names_by_label(TEST_NS, "app=cephfs-shared")
    assert len(pod_names) >= 3, "Expected at least 3 CephFS pods"

    log_step(TID, "Writing file in first pod")
    exec_in_pod(TEST_NS, pod_names[0], ["sh", "-c", "echo 'hello-s01' > /shared/file.txt"])

    log_step(TID, "Validating file from other pods")
    for pod in pod_names[1:]:
        out = exec_in_pod(TEST_NS, pod, ["cat", "/shared/file.txt"])
        assert "hello-s01" in out

    log_step(TID, "Restarting all CephFS pods")
    k(f"delete pod -n {TEST_NS} -l app=cephfs-shared", ARTIFACTS_DIR / f"{TID}_delete.json")
    wait_for_rollout(TEST_NS, "deploy/cephfs-shared", tid=TID)

    log_step(TID, "Revalidating file after restart")
    pod_names = get_pod_names_by_label(TEST_NS, "app=cephfs-shared")
    for pod in pod_names:
        out = exec_in_pod(TEST_NS, pod, ["cat", "/shared/file.txt"])
        assert "hello-s01" in out
