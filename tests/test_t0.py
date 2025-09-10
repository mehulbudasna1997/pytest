import pytest
import time
import os
import subprocess
from kubernetes import client, config
from kubernetes.stream import stream
from dotenv import load_dotenv
import tempfile

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
# T0.2 — Time Sync using DaemonSet
# -------------------------
def test_time_sync(kube_clients):
    core_v1, apps_v1 = kube_clients

    # Create a timecheck DaemonSet
    ds_yaml = """
    apiVersion: apps/v1
    kind: DaemonSet
    metadata:
      name: timecheck
      namespace: default
    spec:
      selector:
        matchLabels:
          app: timecheck
      template:
        metadata:
          labels:
            app: timecheck
        spec:
          hostNetwork: true
          containers:
          - name: timecheck
            image: busybox
            command: ["sleep","3600"]
            securityContext:
              privileged: true
          restartPolicy: Always
    """
    with tempfile.NamedTemporaryFile("w", delete=False) as f:
        f.write(ds_yaml)
        fpath = f.name

    try:
        subprocess.run(["kubectl", "apply", "-f", fpath], check=True)
        # Wait for pods to be running
        timeout = 60
        while timeout > 0:
            pods = core_v1.list_namespaced_pod(namespace="default", label_selector="app=timecheck")
            if all(p.status.phase == "Running" for p in pods.items) and len(pods.items) > 0:
                break
            time.sleep(2)
            timeout -= 2

        # Map pods to nodes
        node_pod_map = {p.spec.node_name: p.metadata.name for p in pods.items}

        node_reports = []
        print("\n=== Time Sync ===")
        for node_name, pod_name in node_pod_map.items():
            try:
                output = stream(core_v1.connect_get_namespaced_pod_exec,
                                pod_name,
                                "default",
                                command=["date", "+%s"],
                                stderr=True, stdin=False, stdout=True, tty=False)
                node_time = int(output.strip())
                drift = abs(node_time - int(time.time()))
                time_ok = drift < 0.05
                node_reports.append({"name": node_name, "time_sync_ok": time_ok})
                print(f"Node: {node_name}, Drift: {drift}s, Status: {'OK' if time_ok else 'DRIFT>10s'}")
            except Exception as e:
                print(f"Error checking time on node {node_name}: {e}")
                node_reports.append({"name": node_name, "time_sync_ok": False})

        bad_time = [n["name"] for n in node_reports if not n["time_sync_ok"]]
        assert not bad_time, f"Some nodes have clock drift >10s: {bad_time}"

    finally:
        subprocess.run(["kubectl", "delete", "ds", "timecheck", "-n", "default"])
        os.unlink(fpath)


# -------------------------
# T0.3 — Network MTU & Pod Connectivity
# -------------------------
def test_network_mtu_and_connectivity(kube_clients):
    core_v1, apps_v1 = kube_clients

    # Privileged DaemonSet for MTU check
    mtu_ds_yaml = """
    apiVersion: apps/v1
    kind: DaemonSet
    metadata:
      name: mtu-check
      namespace: default
    spec:
      selector:
        matchLabels:
          app: mtu-check
      template:
        metadata:
          labels:
            app: mtu-check
        spec:
          hostNetwork: true
          containers:
          - name: mtu
            image: busybox
            command: ["sleep","3600"]
            securityContext:
              privileged: true
          restartPolicy: Always
    """
    with tempfile.NamedTemporaryFile("w", delete=False) as f:
        f.write(mtu_ds_yaml)
        fpath = f.name

    try:
        subprocess.run(["kubectl", "apply", "-f", fpath], check=True)
        # Wait for pods
        timeout = 60
        while timeout > 0:
            pods = core_v1.list_namespaced_pod(namespace="default", label_selector="app=mtu-check")
            if all(p.status.phase == "Running" for p in pods.items) and len(pods.items) > 0:
                break
            time.sleep(2)
            timeout -= 2

        print("\n=== Network MTU Check ===")
        mtu_fail_nodes = []
        for pod in pods.items:
            node_name = pod.spec.node_name
            pod_name = pod.metadata.name
            output = stream(core_v1.connect_get_namespaced_pod_exec,
                            pod_name,
                            "default",
                            command=["ip", "link"],
                            stderr=True, stdin=False, stdout=True, tty=False)
            for line in output.splitlines():
                if "mtu" in line and "lo" not in line:
                    mtu = int(line.split("mtu")[1].split()[0])
                    print(f"Node: {node_name}, MTU: {mtu}")
                    if mtu < 1450:
                        mtu_fail_nodes.append(node_name)

        assert not mtu_fail_nodes, f"MTU check failed on nodes: {mtu_fail_nodes}"

    finally:
        subprocess.run(["kubectl", "delete", "ds", "mtu-check", "-n", "default"])
        os.unlink(fpath)

    # Pod ↔ Pod connectivity using busybox DaemonSet
    busybox_ds_yaml = """
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
    with tempfile.NamedTemporaryFile("w", delete=False) as f:
        f.write(busybox_ds_yaml)
        fpath = f.name

    try:
        subprocess.run(["kubectl", "apply", "-f", fpath], check=True)
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
                resp = stream(core_v1.connect_get_namespaced_pod_exec,
                              src,
                              "default",
                              command=["ping", "-c", "2", tgt_ip],
                              stderr=True, stdin=False, stdout=True, tty=False)
                success = "0% packet loss" in resp
                print(f"{src} -> {tgt}: {'SUCCESS' if success else 'FAILURE'}")
                if not success:
                    connectivity_failures.append(f"{src}->{tgt}")

        assert not connectivity_failures, f"Pod ↔ Pod connectivity failed: {connectivity_failures}"

    finally:
        subprocess.run(["kubectl", "delete", "ds", "busybox", "-n", "default"])
        os.unlink(fpath)
