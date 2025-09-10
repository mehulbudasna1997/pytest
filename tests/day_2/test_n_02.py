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

TID = "N02"

def is_node_ready(node_name: str) -> bool:
    """Check if node Ready condition is True."""
    out = k(f"get node {node_name} -o json", ARTIFACTS_DIR / f"{TID}_node.json")
    import json
    data = json.loads(out)
    for cond in data.get("status", {}).get("conditions", []):
        if cond["type"] == "Ready":
            return cond["status"] == "True"
    return False


def test_worker_hard_power_cycle():
    # 1. Find CephFS pod and node
    pods = get_pod_names_by_label(TEST_NS, "app=cephfs-tester", TID, "pods")
    assert pods, "No CephFS tester pod found"
    pod_name = pods[0]

    out = k(f"-n {TEST_NS} get pod {pod_name} -o wide", ARTIFACTS_DIR / f"{TID}_pod_info.log")
    node_name = out.splitlines()[1].split()[6]  # NODE column
    placeholder(TID, "selected_node", f"Pod {pod_name} is on node {node_name}")

    # 2. Collect pre-crash evidence
    k("get nodes -o wide", ARTIFACTS_DIR / f"{TID}_nodes_before.log")
    ceph("status", ARTIFACTS_DIR / f"{TID}_ceph_before.log")
    exec_in_pod(TEST_NS, pod_name, "md5sum /mnt/cephfs/testfile.txt", TID, "md5_before", check=False)

    # 3. Trigger hard power cycle (manual step / lab command)
    placeholder(TID, "poweroff_node", f"Power off node {node_name} immediately via BMC or sysrq-trigger")

    print(f"⚠️ Node {node_name} is powered off. Wait 2-3 minutes for Kubernetes controllers...")

    # 4. Wait for node to be marked NotReady (~2-3 min)
    time.sleep(180)

    # 5. Collect post-failure evidence
    k("get nodes -o wide", ARTIFACTS_DIR / f"{TID}_nodes_after.log")
    k(f"-n {TEST_NS} get pods -o wide", ARTIFACTS_DIR / f"{TID}_pods_after.log")
    ceph("status", ARTIFACTS_DIR / f"{TID}_ceph_after.log")

    # 6. Find replacement pod on healthy node
    pods_after = get_pod_names_by_label(TEST_NS, "app=cephfs-tester", TID, "pods_after2")
    new_pod = None
    for p in pods_after:
        out = k(f"-n {TEST_NS} get pod {p} -o wide", ARTIFACTS_DIR / f"{TID}_{p}.log")
        if node_name not in out:
            new_pod = p
            break
    assert new_pod, "No replacement pod running on healthy node"

    # 7. Verify data (PVC reattached successfully)
    out = exec_in_pod(TEST_NS, new_pod, "cat /mnt/cephfs/testfile.txt", TID, "md5_after", check=False)
    assert "cephfs test OK" in out, "CephFS data lost after hard node crash"

    print("✅ Worker hard power cycle test passed!")
