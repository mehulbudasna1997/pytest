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

TID = "N06"

def test_dns_outage_coredns():
    # 1️⃣ Find CephFS tester pod
    pods = get_pod_names_by_label(TEST_NS, "app=cephfs-tester", TID, "pods")
    assert pods, "No CephFS tester pod found"
    pod_name = pods[0]

    out = k(f"-n {TEST_NS} get pod {pod_name} -o wide", ARTIFACTS_DIR / f"{TID}_pod_info.log")
    node_name = out.splitlines()[1].split()[6]  # NODE column
    placeholder(TID, "selected_node", f"Pod {pod_name} is on node {node_name}")

    # 2️⃣ Collect pre-outage evidence
    k("get nodes -o wide", ARTIFACTS_DIR / f"{TID}_nodes_before.log")
    ceph("status", ARTIFACTS_DIR / f"{TID}_ceph_before.log")
    exec_in_pod(TEST_NS, pod_name, "md5sum /mnt/cephfs/testfile.txt", TID, "md5_before", check=False)

    # 3️⃣ Scale down CoreDNS to 0 replicas (simulate DNS outage)
    placeholder(TID, "coredns_down", "kubectl -n kube-system scale deploy/coredns --replicas=0")
    print("⚠️ CoreDNS scaled to 0. DNS outage in progress... wait 2 min")
    time.sleep(120)

    # 4️⃣ Scale CoreDNS back up (restore DNS)
    placeholder(TID, "coredns_up", "kubectl -n kube-system scale deploy/coredns --replicas=2")
    print("✅ CoreDNS scaled back up. Waiting for pods to become Ready...")

    # 5️⃣ Wait for CoreDNS pods to be ready
    start = time.time()
    ready = False
    while time.time() - start < 180:
        out = k("get pods -n kube-system -l k8s-app=kube-dns -o json", ARTIFACTS_DIR / f"{TID}_coredns_status.json")
        import json
        data = json.loads(out)
        all_ready = all(c["ready"] for p in data["items"] for c in p["status"].get("containerStatuses", []))
        if all_ready:
            ready = True
            break
        time.sleep(5)
    assert ready, "CoreDNS pods did not become Ready after scaling up"

    # 6️⃣ Verify CephFS data still intact
    out = exec_in_pod(TEST_NS, pod_name, "cat /mnt/cephfs/testfile.txt", TID, "md5_after", check=False)
    assert "cephfs test OK" in out, "CephFS data lost during DNS outage"

    print("✅ DNS outage (CoreDNS) test passed!")
