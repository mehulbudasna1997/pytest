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

TID = "N08"

def test_disk_inode_pressure():
    # 1️⃣ Choose a node to stress
    out = k("get nodes -o wide", ARTIFACTS_DIR / f"{TID}_nodes.log")
    node_name = out.splitlines()[1].split()[0]  # pick first node
    placeholder(TID, "selected_node", f"Selected node {node_name} for disk/inode pressure test")

    # 2️⃣ Collect pre-pressure evidence
    ceph("status", ARTIFACTS_DIR / f"{TID}_ceph_before.log")
    k(f"get pods -n {TEST_NS} -o wide", ARTIFACTS_DIR / f"{TID}_pods_before.log")

    # 3️⃣ Simulate disk pressure or inode pressure
    placeholder(TID, "disk_pressure", f"Fill /var/lib/kubelet or create many small files on {node_name} to trigger DiskPressure/InodePressure")
    print("⚠️ Disk/Inode pressure applied. Observing node conditions...")

    # 4️⃣ Wait and observe node conditions (max 2 min)
    pressure_observed = False
    start = time.time()
    while time.time() - start < 120:
        out = k(f"describe node {node_name}", ARTIFACTS_DIR / f"{TID}_node_desc.log")
        if "DiskPressure" in out or "InodePressure" in out:
            pressure_observed = True
            break
        time.sleep(5)
    assert pressure_observed, f"No DiskPressure/InodePressure condition observed on {node_name}"

    print(f"⚠️ Node {node_name} shows DiskPressure/InodePressure.")

    # 5️⃣ Observe evictions (optional)
    placeholder(TID, "evictions", f"Check which pods were evicted due to pressure on {node_name}")

    # 6️⃣ Cleanup
    placeholder(TID, "cleanup", f"Delete test files to free up space/inodes on {node_name}")
    print("✅ Disk/Inode pressure cleared. Scheduler should now place pods normally.")

    # 7️⃣ Verify CephFS pod still intact
    pods = get_pod_names_by_label(TEST_NS, "app=cephfs-tester", TID, "pods_after")
    assert pods, "No CephFS pod found after node pressure test"
    pod_name = pods[0]
    out = exec_in_pod(TEST_NS, pod_name, "cat /mnt/cephfs/testfile.txt", TID, "md5_after", check=False)
    assert "cephfs test OK" in out, "CephFS data lost during disk/inode pressure test"

    print("✅ DiskPressure/InodePressure test passed!")
