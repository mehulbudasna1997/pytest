import pytest
import subprocess
import time
from kubernetes import client, config

# -------------------------
# Fixtures
# -------------------------
@pytest.fixture(scope="session")
def kube_clients():
    """Return CoreV1Api and AppsV1Api clients."""
    config.load_kube_config()
    core_v1 = client.CoreV1Api()
    apps_v1 = client.AppsV1Api()
    return core_v1, apps_v1

# -------------------------
# T0.1 — Node Health
# -------------------------
def test_node_health(kube_clients):
    core_v1, _ = kube_clients
    nodes = core_v1.list_node().items

    print("\n=== Node Health ===")
    not_ready_nodes = []
    for n in nodes:
        ready_status = next((s.status for s in n.status.conditions if s.type == "Ready"), "Unknown")
        print(f"Node: {n.metadata.name}, Ready: {ready_status}")
        if ready_status != "True":
            not_ready_nodes.append(n.metadata.name)

    assert not not_ready_nodes, f"Some nodes are not Ready: {not_ready_nodes}"

# -------------------------
# T0.1 — Time Sync
# -------------------------
def test_time_sync(kube_clients):
    core_v1, _ = kube_clients
    node_reports = []

    print("\n=== Time Sync ===")
    for n in core_v1.list_node().items:
        name = n.metadata.name
        try:
            output = subprocess.check_output(["ssh", name, "chronyc tracking"], text=True)
            drift_line = next(l for l in output.splitlines() if "System time" in l)
            drift_ms = float(drift_line.split()[3].replace("ms", ""))
            time_ok = abs(drift_ms) < 0.1
            node_reports.append({"name": name, "time_sync_ok": time_ok})
            print(f"Node: {name}, Drift: {drift_ms} ms, Status: {'OK' if time_ok else 'DRIFT >100ms'}")
        except Exception as e:
            print(f"Error checking time sync on node {name}: {e}")
            node_reports.append({"name": name, "time_sync_ok": False})

    bad_time = [n["name"] for n in node_reports if not n["time_sync_ok"]]
    assert not bad_time, f"Some nodes have clock drift >100ms: {bad_time}"

# -------------------------
# T0.2 — Network MTU & Pod Connectivity
# -------------------------
def test_network_mtu_and_connectivity(kube_clients):
    core_v1, apps_v1 = kube_clients

    # MTU Check
    print("\n=== Network MTU Check ===")
    mtu_fail_nodes = []
    for n in core_v1.list_node().items:
        node_name = n.metadata.name
        try:
            output = subprocess.check_output(["ssh", node_name, "ip link"], text=True)
            for line in output.splitlines():
                if "mtu" in line:
                    mtu = int(line.split("mtu")[1].split()[0])
                    print(f"Node: {node_name}, MTU: {mtu}")
                    if mtu != 1500:  # Adjust per your prod setup
                        mtu_fail_nodes.append(node_name)
        except Exception as e:
            print(f"Error checking MTU on node {node_name}: {e}")
            mtu_fail_nodes.append(node_name)
    assert not mtu_fail_nodes, f"MTU check failed on nodes: {mtu_fail_nodes}"

    # -------------------------
    # Pod ↔ Pod connectivity using busybox DaemonSet
    # -------------------------
    print("\n=== Pod ↔ Pod Connectivity ===")
    busybox_ds = """
    apiVersion: apps/v1
    kind: DaemonSet
    metadata:
      name: busybox
      namespace: default
    spec:
      selector:
        matchLabels:
          app: busybox
      template:
        metadata:
          labels:
            app: busybox
        spec:
          containers:
          - name: busybox
            image: busybox
            command: ["sleep","3600"]
          restartPolicy: Always
    """

    import tempfile, os
    with tempfile.NamedTemporaryFile("w", delete=False) as f:
        f.write(busybox_ds)
        fpath = f.name

    try:
        subprocess.run(["kubectl", "apply", "-f", fpath], check=True)
        # Wait for pods running
        timeout = 60
        while timeout > 0:
            pods = core_v1.list_namespaced_pod(namespace="default", label_selector="app=busybox")
            if all(p.status.phase == "Running" for p in pods.items) and len(pods.items) > 0:
                break
            time.sleep(2)
            timeout -= 2

        pod_ips = {p.metadata.name: p.status.pod_ip for p in pods.items}
        connectivity_failures = []
        for src, src_ip in pod_ips.items():
            for tgt, tgt_ip in pod_ips.items():
                if src == tgt:
                    continue
                result = subprocess.run(
                    ["kubectl", "exec", src, "--", "ping", "-c", "2", tgt_ip],
                    capture_output=True, text=True
                )
                success = result.returncode == 0
                print(f"{src} -> {tgt}: {'SUCCESS' if success else 'FAILURE'}")
                if not success:
                    connectivity_failures.append(f"{src}->{tgt}")

        assert not connectivity_failures, f"Pod ↔ Pod connectivity failed: {connectivity_failures}"

    finally:
        # Cleanup DaemonSet
        subprocess.run(["kubectl", "delete", "ds", "busybox", "-n", "default"])
        os.unlink(fpath)

