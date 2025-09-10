import time
import pytest
from helpers import (
    k,
    ceph,
    get_pod_names_by_label,
    exec_in_pod,
    placeholder,
    ARTIFACTS_DIR,
    TEST_NS
)

TID = "N05"

def test_node_network_flap():
    # 1️⃣ Find CephFS tester pod
    pods = get_pod_names_by_label(TEST_NS, "app=cephfs-tester", TID, "pods")
    assert pods, "No CephFS tester pod found"
    pod_name = pods[0]

    out = k(f"-n {TEST_NS} get pod {pod_name} -o wide", ARTIFACTS_DIR / f"{TID}_pod_info.log")
    node_name = out.splitlines()[1].split()[6]  # NODE column
    placeholder(TID, "selected_node", f"Pod {pod_name} is on node {node_name}")

    # 2️⃣ Collect pre-flap evidence
    k("get nodes -o wide", ARTIFACTS_DIR / f"{TID}_nodes_before.log")
    ceph("status", ARTIFACTS_DIR / f"{TID}_ceph_before.log")
    exec_in_pod(TEST_NS, pod_name, "md5sum /mnt/cephfs/testfile.txt", TID, "md5_before", check=False)

    # 3️⃣ Drop default route on the node (simulate network flap)
    placeholder(TID, "drop_route", f"Run on {node_name}: sudo ip route del default")
    print("⚠️ Default route dropped. Wait 60s...")

    time.sleep(60)

    # 4️⃣ Restore default route
    placeholder(TID, "restore_route", f"Run on {node_name}: sudo ip route add default via <GATEWAY>")
    print("✅ Default route restored. Waiting for recovery...")

    # 5️⃣ Wait for pod readiness (max 2 min)
    recovered = False
    start = time.time()
    while time.time() - start < 120:
        pods_after = get_pod_names_by_label(TEST_NS, "app=cephfs-tester", TID, "pods_after")
        if pods_after and pods_after[0] == pod_name:
            recovered = True
            break
        time.sleep(5)
    assert recovered, "Pod did not recover after network flap"

    # 6️⃣ Verify CephFS data
    out = exec_in_pod(TEST_NS, pod_name, "cat /mnt/cephfs/testfile.txt", TID, "md5_after", check=False)
    assert "cephfs test OK" in out, "CephFS data lost after network flap"

    print("✅ Node network flap test passed!")
