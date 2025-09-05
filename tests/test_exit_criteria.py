import os
import pytest
import subprocess
from kubernetes import client, config


@pytest.fixture(scope="session")
def kube_client():
    """
    Load kubeconfig once per session and provide core, apps, autoscaling clients.
    """
    config.load_kube_config()
    core_v1 = client.CoreV1Api()
    apps_v1 = client.AppsV1Api()
    autoscaling_v1 = client.AutoscalingV1Api()
    return core_v1, apps_v1, autoscaling_v1


@pytest.mark.critical
def test_exit_all_critical_high_passed(request):
    """
    Purpose:
        Verify all Critical & High severity tests passed in this pytest session.
    Preconditions:
        - All test phases executed before exit checks
    Steps:
        1. Collect total and failed test outcomes
    Expected Result:
        - No Critical or High severity test failures
    """
    total = request.session.testscollected
    failed = request.session.testsfailed
    assert failed == 0, f"{failed} test(s) failed out of {total}, exit criteria not met"


@pytest.mark.high
def test_exit_no_unresolved_alerts(kube_client):
    """
    Purpose:
        Validate no unresolved alerts/warnings in Kubernetes or Ceph.
    Preconditions:
        - Cluster operational
        - Ceph CLI available
    Steps:
        1. Check Ceph cluster health
        2. Check Kubernetes events for warnings
    Expected Result:
        - Ceph reports HEALTH_OK
        - No Warning events in Kubernetes
    """
    ceph_status = subprocess.check_output(["ceph", "-s"], text=True)
    assert "HEALTH_OK" in ceph_status, f"Ceph not healthy: {ceph_status}"

    events = subprocess.check_output(
        ["kubectl", "get", "events", "-A", "--sort-by=.lastTimestamp"], text=True
    )
    assert "Warning" not in events, f"Cluster has unresolved warnings:\n{events}"


@pytest.mark.high
def test_exit_performance_baseline_recorded():
    """
    Purpose:
        Verify baseline performance results are captured and available.
    Preconditions:
        - Performance tests executed (Phase 6)
    Steps:
        1. Look for baseline artifacts/logs
    Expected Result:
        - Baseline throughput/latency recorded in artifacts
    """
    try:
        with open("artifacts/performance_baseline.txt") as f:
            data = f.read()
        assert "throughput" in data.lower(), "Baseline throughput not recorded"
    except FileNotFoundError:
        pytest.skip("Performance baseline logs not found")


@pytest.mark.high
def test_exit_backup_restore_verified():
    """
    Purpose:
        Ensure backup/restore rehearsal artifacts are present.
    Preconditions:
        - Backup rehearsal executed
    Steps:
        1. Check etcd snapshot artifact
        2. Check Ceph metadata backup
    Expected Result:
        - Required backup artifacts exist
    """
    assert os.path.exists("artifacts/etcd_snapshot.tar.gz"), "etcd snapshot missing"
    assert os.path.exists("artifacts/ceph_metadata.json"), "Ceph metadata backup missing"


@pytest.mark.high
def test_exit_upgrade_rehearsal_success():
    """
    Purpose:
        Verify upgrade rehearsal completed successfully.
    Preconditions:
        - Upgrade rehearsal executed (Phase 9)
    Steps:
        1. Parse upgrade status logs
    Expected Result:
        - SUCCESS flag recorded in logs
    """
    try:
        with open("artifacts/upgrade_status.log") as f:
            status = f.read()
        assert "SUCCESS" in status, "Upgrade rehearsal did not complete successfully"
    except FileNotFoundError:
        pytest.skip("Upgrade rehearsal logs not found")


@pytest.mark.high
def test_exit_stakeholder_signoff():
    """
    Purpose:
        Confirm stakeholder sign-off before final exit.
    Preconditions:
        - Approval document prepared
    Steps:
        1. Load sign-off artifact
    Expected Result:
        - Sign-off file includes 'approved: true'
    """
    try:
        with open("artifacts/signoff.yaml") as f:
            signoff = f.read()
        assert "approved: true" in signoff, "Stakeholder sign-off missing"
    except FileNotFoundError:
        pytest.skip("Sign-off artifact not found")
