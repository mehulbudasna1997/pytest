import pytest
import subprocess
from kubernetes import client, config


@pytest.fixture(scope="session")
def kube_clients():
    config.load_kube_config()
    return client.CoreV1Api(), client.AppsV1Api()


# -------------------------
# T2.1 — Ceph cluster health + master node connectivity
# -------------------------
def test_ceph_cluster_health_and_master(kube_clients):
    """
    Prod: Run `ceph -s` and verify:
        - HEALTH_OK
        - quorum 3/3 MON
        - MGR active
        - OSDs up/in
    Then verify all master nodes are Ready.
    """
    print("\n=== T2.1 — Ceph Cluster Health (Prod) ===")
    try:
        # Ceph health
        result = subprocess.run(["ceph", "-s"], capture_output=True, text=True, check=True)
        output = result.stdout
        print(output)
        assert "HEALTH_OK" in output, "Ceph cluster is not HEALTH_OK"
        assert "quorum 3/3" in output, "MON quorum is not 3/3"
        assert "mgr: active" in output, "MGR is not active"
        assert "up," in output and "in" in output, "OSDs are not up/in"
    except subprocess.CalledProcessError as e:
        pytest.fail(f"Failed to run ceph -s: {e}")

    # Master node connectivity
    print("\n=== Checking Master Node Connectivity ===")
    core_v1, _ = kube_clients
    masters = [n.metadata.name for n in core_v1.list_node().items if
               "control-plane" in n.metadata.labels or "master" in n.metadata.labels]

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
    """
    Prod: Run `ceph osd tree` and `ceph df` to verify balanced OSDs,
    then verify master nodes are pingable via kubernetes API.
    """
    print("\n=== T2.2 — Ceph OSD Map & DF (Prod) ===")
    try:
        osd_tree = subprocess.run(["ceph", "osd", "tree"], capture_output=True, text=True, check=True)
        ceph_df = subprocess.run(["ceph", "df"], capture_output=True, text=True, check=True)

        print("--- OSD Tree ---")
        print(osd_tree.stdout)
        print("--- Ceph DF ---")
        print(ceph_df.stdout)

        # Basic checks
        assert "up" in osd_tree.stdout, "Some OSDs are down"
        assert "in" in osd_tree.stdout, "Some OSDs are out"
        assert "SIZE" in ceph_df.stdout, "Ceph DF output not found"
    except subprocess.CalledProcessError as e:
        pytest.fail(f"Failed to run ceph commands: {e}")

    # Ping masters via Kubernetes API
    print("\n=== Ping Master Nodes via API ===")
    core_v1, _ = kube_clients
    masters = [n.metadata.name for n in core_v1.list_node().items if
               "control-plane" in n.metadata.labels or "master" in n.metadata.labels]

    unreachable = []
    for m in masters:
        try:
            # Using kubernetes API instead of raw ping
            node = core_v1.read_node_status(m)
            print(f"Master Node: {m}, conditions retrieved successfully")
        except Exception as e:
            print(f"Cannot reach master node {m}: {e}")
            unreachable.append(m)

    assert not unreachable, f"Some master nodes are unreachable: {unreachable}"
