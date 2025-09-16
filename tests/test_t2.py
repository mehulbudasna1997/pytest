import pytest
import subprocess
from kubernetes import client, config
import os
from dotenv import load_dotenv
import re

load_dotenv()


# -------------------------
# Fixtures
# -------------------------
@pytest.fixture(scope="session")
def kube_clients():
    """Return CoreV1Api and AppsV1Api clients."""
    try:
        kubeconfig_path = os.environ.get("KUBECONFIG")
        if kubeconfig_path:
            config.load_kube_config(config_file=kubeconfig_path)
        else:
            config.load_kube_config()
    except Exception:
        config.load_incluster_config()

    core_v1 = client.CoreV1Api()
    apps_v1 = client.AppsV1Api()
    return core_v1, apps_v1


# -------------------------
# T2.1 — Ceph cluster health + master node connectivity
# -------------------------
def test_ceph_cluster_health_and_master(kube_clients):
    print("\n=== T2.1 — Ceph Cluster Health (Prod) ===")
    try:
        result = subprocess.run(["ceph", "-s"], capture_output=True, text=True, check=True)
        output = result.stdout
        print(output)

        # HEALTH_OK check
        assert "HEALTH_OK" in output or "HEALTH_WARN" in output, (
            "Ceph cluster health is neither OK nor acceptable WARN"
        )

        # --- MON quorum check (dynamic count) ---
        mon_match = re.search(r"mon:\s+(\d+)\s+daemons, quorum\s+([\w,]+)", output)
        assert mon_match, "No MON quorum info found in ceph -s"
        total_mons = int(mon_match.group(1))
        mon_list = mon_match.group(2).split(",")
        print(f"MONs in quorum: {mon_list}")
        assert len(mon_list) == total_mons, (
            f"Expected {total_mons} MONs in quorum, found {len(mon_list)}"
        )

        # --- MGR active check ---
        mgr_match = re.search(r"mgr:\s+(\S+)\(active", output)
        assert mgr_match, "Active MGR not found in ceph -s"
        print(f"Active MGR: {mgr_match.group(1)}")

        # --- OSD up/in check ---
        osd_match = re.search(r"osd:\s+(\d+)\s+osds:\s+(\d+)\s+up.*?,\s+(\d+)\s+in", output)
        assert osd_match, "OSD status not found in ceph -s output"
        total_osds, up_osds, in_osds = map(int, osd_match.groups())
        print(f"Total OSDs: {total_osds}, Up: {up_osds}, In: {in_osds}")
        assert up_osds == total_osds, f"OSD up mismatch: total={total_osds}, up={up_osds}"
        assert in_osds == total_osds, f"OSD in mismatch: total={total_osds}, in={in_osds}"

    except subprocess.CalledProcessError as e:
        pytest.fail(f"Failed to run ceph -s: {e}")

    # Master node connectivity
    print("\n=== Checking Master Node Connectivity ===")
    core_v1, _ = kube_clients
    masters = [
        n.metadata.name
        for n in core_v1.list_node().items
        if "control-plane" in n.metadata.labels or "master" in n.metadata.labels
    ]

    not_ready = []
    for m in masters:
        node = core_v1.read_node_status(m)
        ready_status = next((s.status for s in node.status.conditions if s.type == "Ready"), "Unknown")
        print(f"Master Node: {m}, Ready: {ready_status}")
        if ready_status != "True":
            not_ready.append(m)

    assert not not_ready, f"Some master nodes are not Ready: {not_ready}"


# -------------------------
# T2.2 — Ceph OSD map & distribution + master node ping
# -------------------------
def test_ceph_osd_distribution_and_master(kube_clients):
    print("\n=== T2.2 — Ceph OSD Map & DF (Prod) ===")
    try:
        osd_tree_res = subprocess.run(["ceph", "osd", "tree"], capture_output=True, text=True, check=True)
        ceph_df_res = subprocess.run(["ceph", "df"], capture_output=True, text=True, check=True)

        print("--- OSD Tree ---")
        print(osd_tree_res.stdout)
        print("--- Ceph DF ---")
        print(ceph_df_res.stdout)

        # Parse OSD tree to ensure all OSDs are up/in
        osd_lines = [line for line in osd_tree_res.stdout.splitlines() if line.strip().startswith("osd.")]
        osd_failures = []
        for line in osd_lines:
            cols = line.split()
            if len(cols) < 5:
                continue
            osd_name = cols[0]
            status = cols[4].lower()  # up/down
            if status != "up":
                osd_failures.append(osd_name)

        assert not osd_failures, f"Some OSDs are not up: {osd_failures}"

        # Ceph DF basic check
        assert "SIZE" in ceph_df_res.stdout, "Ceph DF output missing"

    except subprocess.CalledProcessError as e:
        pytest.fail(f"Failed to run ceph commands: {e}")

    # Ping masters via Kubernetes API
    print("\n=== Ping Master Nodes via API ===")
    core_v1, _ = kube_clients
    masters = [
        n.metadata.name
        for n in core_v1.list_node().items
        if "control-plane" in n.metadata.labels or "master" in n.metadata.labels
    ]

    unreachable = []
    for m in masters:
        try:
            node = core_v1.read_node_status(m)
            print(f"Master Node: {m}, conditions retrieved successfully")
        except Exception as e:
            print(f"Cannot reach master node {m}: {e}")
            unreachable.append(m)

    assert not unreachable, f"Some master nodes are unreachable: {unreachable}"
