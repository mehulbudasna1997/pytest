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

TID = "N01"


def is_node_ready(node_name: str) -> bool:
    """Check if node Ready condition is True."""
    out = k(f"get node {node_name} -o json", ARTIFACTS_DIR / f"{TID}_node.json")
    import json
    data = json.loads(out)
    for cond in data.get("status", {}).get("conditions", []):
        if cond["type"] == "Ready":
            return cond["status"] == "True"
    return False


def test_worker_node_reboot_graceful():
    # 1. Find CephFS pod and node
    pods = get_pod_names_by_label(TEST_NS, "app=cephfs-tester", TID, "pods")
    assert pods, "No CephFS tester pod found"
    pod_name = pods[0]

    out = k(f"-n {TEST_NS} get pod {pod_name} -o wide", ARTIFACTS_DIR / f"{TID}_pod_info.log")
    node_name = out.splitlines()[1].split()[6]  # NODE column
    placeholder(TID, "selected_node", f"Pod {pod_name} is on node {node_name}")

    # 2. Collect pre-reboot evidence
    k("get nodes -o wide", ARTIFACTS_DIR / f"{TID}_nodes_before.log")
    ceph("status", ARTIFACTS_DIR / f"{TID}_ceph_before.log")
    exec_in_pod(TEST_NS, pod_name, "md5sum /mnt/cephfs/testfile.txt", TID, "md5_before", check=False)

    # 3. Drain node gracefully
    k(f"drain {node_name} --ignore-daemonsets --delete-emptydir-data", ARTIFACTS_DIR / f"{TID}_drain.log")

    # 4. Reboot node (manual step)
    placeholder(TID, "reboot_node", f"Reboot node {node_name} manually or via automation")

    print(f"⚠️ Node {node_name} should be rebooted now. Waiting up to 5 minutes for Ready...")

    # 5. Wait for node Ready (max 5 min)
    start = time.time()
    node_ready = False
    while time.time() - start < 300:
        if is_node_ready(node_name):
            node_ready = True
            break
        time.sleep(10)
    assert node_ready, f"Node {node_name} did not become Ready in 5 min"

    # 6. Uncordon node
    k(f"uncordon {node_name}", ARTIFACTS_DIR / f"{TID}_uncordon.log")

    # 7. Verify pod rescheduled
    recovered = False
    start = time.time()
    while time.time() - start < 120:
        pods_after = get_pod_names_by_label(TEST_NS, "app=cephfs-tester", TID, "pods_after")
        if pods_after and pods_after[0] != pod_name:
            recovered = True
            new_pod = pods_after[0]
            break
        time.sleep(5)
    assert recovered, "Pod did not reschedule to another node within 120s"

    # 8. Verify data
    out = exec_in_pod(TEST_NS, new_pod, "cat /mnt/cephfs/testfile.txt", TID, "md5_after", check=False)
    assert "cephfs test OK" in out, "CephFS data lost after node reboot"

    print("✅ Worker node reboot (graceful) test passed!")
