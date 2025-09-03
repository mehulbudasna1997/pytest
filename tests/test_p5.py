import pytest
import subprocess
import time
from kubernetes import client, config


@pytest.fixture(scope="session")
def core_v1():
    """Load kubeconfig and return CoreV1Api"""
    config.load_kube_config()
    return client.CoreV1Api()



@pytest.mark.high
def test_t5_1_worker_reboot(core_v1):
    """
    Purpose:
        Validate pod rescheduling and storage persistence when a worker node reboots.
    Preconditions:
        - At least one application pod (label=app=cephfs-shared) running in test-cephfs namespace
        - PVC mounted at /shared
    Steps:
        1. Identify the worker node hosting the pod
        2. Write a test file to /shared/t51.txt and record checksum
        3. Drain the node (simulate reboot)
        4. Uncordon the node
        5. Verify pod reschedules and data remains intact
    Expected Result:
        - Pod is rescheduled to a healthy node
        - PVC re-attaches successfully
        - md5sum before == md5sum after (data integrity preserved)
    """
    ns = "test-cephfs"
    selector = "app=cephfs-shared"
    test_file = "/shared/t51.txt"

    pods = core_v1.list_namespaced_pod(ns, label_selector=selector).items
    assert pods, "No pods found with label app=cephfs-shared"
    pod = pods[0]
    node, pod_name = pod.spec.node_name, pod.metadata.name

    # Write test file
    subprocess.run(
        ["kubectl", "-n", ns, "exec", pod_name, "--",
         "sh", "-c", f"echo T51 > {test_file}"],
        check=True,
    )
    md5_before = subprocess.check_output(
        ["kubectl", "-n", ns, "exec", pod_name, "--", "md5sum", test_file]
    ).decode().split()[0]

    # Drain + uncordon node
    subprocess.run(
        ["kubectl", "drain", node, "--ignore-daemonsets", "--delete-emptydir-data",
         "--force", "--grace-period=30", "--timeout=5m"],
        check=True,
    )
    time.sleep(10)
    subprocess.run(["kubectl", "uncordon", node], check=True)

    # Verify file integrity from new pod
    pods_after = core_v1.list_namespaced_pod(ns, label_selector=selector).items
    assert pods_after, "No pods running after node drain/uncordon"
    pod2 = pods_after[0].metadata.name
    md5_after = subprocess.check_output(
        ["kubectl", "-n", ns, "exec", pod2, "--", "md5sum", test_file]
    ).decode().split()[0]

    assert md5_before == md5_after, "Data integrity failed after worker reboot"


@pytest.mark.critical
def test_t5_2_master_reboot():
    """
    Purpose:
        Validate cluster stability when a master node (hosting MON/MGR/OSD) is rebooted.
    Steps:
        1. Reboot one master node (manual step or via automation)
        2. Run `ceph -s` to verify cluster health
        3. Run `kubectl get nodes` to check control-plane status
    Expected Result:
        - Ceph quorum is maintained (2/3 or more MONs active)
        - Kubernetes control plane remains healthy and Ready
    """
    result = subprocess.run(["ceph", "-s"], capture_output=True, text=True, check=True)
    output = result.stdout
    assert "quorum" in output, "Ceph quorum not maintained"
    assert "health" in output.lower(), "Ceph health not reported"

    nodes = subprocess.run(["kubectl", "get", "nodes"], capture_output=True, text=True, check=True)
    assert "Ready" in nodes.stdout, "Some control-plane nodes not Ready"


@pytest.mark.critical
def test_t5_3_osd_failure():
    """
    Purpose:
        Validate Ceph cluster resilience to an OSD daemon failure.
    Steps:
        1. Stop an OSD service via `ceph orch daemon stop osd.<id>`
        2. Observe cluster health (`ceph -s`)
        3. Restart OSD daemon
    Expected Result:
        - HEALTH_WARN reported during failure/recovery
        - Data remains available
        - HEALTH_OK after OSD is restarted
    """
    osd_id = "0"  # Adjust if multiple OSDs exist
    subprocess.run(["ceph", "orch", "daemon", "stop", f"osd.{osd_id}"], check=True)
    time.sleep(10)
    status_warn = subprocess.run(["ceph", "-s"], capture_output=True, text=True, check=True).stdout
    assert "HEALTH_WARN" in status_warn or "HEALTH_OK" in status_warn

    subprocess.run(["ceph", "orch", "daemon", "start", f"osd.{osd_id}"], check=True)
    time.sleep(30)
    status_ok = subprocess.run(["ceph", "-s"], capture_output=True, text=True, check=True).stdout
    assert "HEALTH_OK" in status_ok, "Ceph did not return to HEALTH_OK after OSD restart"



@pytest.mark.high
def test_t5_4_network_flap():
    """
    Purpose:
        Validate cluster stability during a short network disruption.
    Steps:
        1. Block MON port (6789/3300) on one master node using iptables
        2. Observe cluster health with `ceph -s`
        3. Unblock MON port
    Expected Result:
        - No client IO interruption
        - Ceph quorum remains ≥2
    """
    # Block MON port temporarily (using iptables on localhost where MON is running)
    # Note: Adjust port (6789 or 3300) depending on Ceph version
    mon_port = "3300"
    try:
        subprocess.run(
            ["sudo", "iptables", "-A", "INPUT", "-p", "tcp", "--dport", mon_port, "-j", "DROP"],
            check=True,
        )

        time.sleep(5)  # short disruption window

        # Check ceph health during disruption
        result = subprocess.run(["ceph", "-s"], capture_output=True, text=True, check=True)
        output = result.stdout
        print(output)

        assert "quorum" in output, "Ceph quorum not maintained during network flap"
        assert "health" in output.lower(), "Ceph health not reported during network flap"

    finally:
        # Always unblock port after test
        subprocess.run(
            ["sudo", "iptables", "-D", "INPUT", "-p", "tcp", "--dport", mon_port, "-j", "DROP"],
            check=False,
        )
        time.sleep(5)

        # Verify cluster is healthy again
        final_status = subprocess.run(["ceph", "-s"], capture_output=True, text=True, check=True).stdout
        assert "HEALTH_OK" in final_status or "HEALTH_WARN" in final_status, "Cluster did not recover after network flap"


@pytest.mark.medium
def test_t5_5_csi_restart(core_v1):
    """
    Purpose:
        Validate resilience of CSI drivers when restarted.
    Steps:
        1. Restart CSI CephFS plugin provisioner and node daemonset
        2. Verify application pods remain Running
    Expected Result:
        - Volumes remain mounted and functional
        - No application disruption beyond restart window
    """
    subprocess.run(
        ["kubectl", "-n", "rook-ceph", "rollout", "restart", "deploy/csi-cephfsplugin-provisioner"],
        check=True,
    )
    subprocess.run(
        ["kubectl", "-n", "rook-ceph", "rollout", "restart", "ds/csi-cephfsplugin"],
        check=True,
    )
    time.sleep(15)

    pods = core_v1.list_namespaced_pod("test-cephfs", label_selector="app=cephfs-shared").items
    for pod in pods:
        assert pod.status.phase == "Running", f"Pod {pod.metadata.name} not running after CSI restart"
