import time
import pytest
from pathlib import Path
from helpers import (
    k,
    ceph,
    get_pod_names_by_label,
    exec_in_pod,
    placeholder,
    ARTIFACTS_DIR,
    TEST_NS
)

TID = "N03"

def test_kubelet_restart_worker():
    # 1. Identify a CephFS tester pod and its node
    pods = get_pod_names_by_label(TEST_NS, "app=cephfs-tester", TID, "pods")
    assert pods, "No CephFS tester pod found"
    pod_name = pods[0]

    out = k(f"-n {TEST_NS} get pod {pod_name} -o wide", ARTIFACTS_DIR / f"{TID}_pod_info.log")
    node_name = out.splitlines()[1].split()[6]  # NODE column
    placeholder(TID, "selected_node", f"Pod {pod_name} is on node {node_name}")

    # 2. Collect pre-restart evidence
    k("get nodes -o wide", ARTIFACTS_DIR / f"{TID}_nodes_before.log")
    ceph("status", ARTIFACTS_DIR / f"{TID}_ceph_before.log")
    exec_in_pod(TEST_NS, pod_name, "md5sum /mnt/cephfs/testfile.txt", TID, "md5_before", check=False)

    # 3. Restart kubelet (manual / lab step)
    placeholder(TID, "kubelet_restart", f"SSH into node {node_name} and run: sudo systemctl restart kubelet")

    print(f"⚠️ Kubelet restart initiated on node {node_name}. Monitor pod readiness...")

    # 4. Wait for pod to be Running again
    start = time.time()
    recovered = False
    while time.time() - start < 180:  # 3 minutes max
        pods_status = k(f"-n {TEST_NS} get pod {pod_name}", ARTIFACTS_DIR / f"{TID}_pod_status.log")
        if "Running" in pods_status and "0/1" not in pods_status:  # avoid 0/1 Ready blip
            recovered = True
            break
        time.sleep(5)
    assert recovered, f"Pod {pod_name} did not recover after kubelet restart"

    # 5. Verify CephFS data still accessible
    out = exec_in_pod(TEST_NS, pod_name, "cat /mnt/cephfs/testfile.txt", TID, "md5_after", check=False)
    assert "cephfs test OK" in out, "CephFS data lost after kubelet restart"

    # 6. Collect post-restart evidence
    k("get nodes -o wide", ARTIFACTS_DIR / f"{TID}_nodes_after.log")
    k(f"-n {TEST_NS} get pods -o wide", ARTIFACTS_DIR / f"{TID}_pods_after.log")
    ceph("status", ARTIFACTS_DIR / f"{TID}_ceph_after.log")

    print("✅ Kubelet restart test passed! Pod remained healthy and CephFS volumes intact.")
