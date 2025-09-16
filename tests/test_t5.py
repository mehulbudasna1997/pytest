import os
import subprocess
import time
import pytest
from kubernetes import client, config

# Configurable
TEST_NAMESPACE = os.environ.get("TEST_NAMESPACE", "test-cephfs")
SSH_USER = os.environ.get("SSH_USER", None)
WAIT_READY_TIMEOUT = 600   # seconds
WAIT_NOTREADY_TIMEOUT = 180
POLL_INTERVAL = 5


def run_cmd(cmd: str, check: bool = True) -> str:
    """Run a shell command and return stdout (raise if non-zero when check=True)."""
    print(f"[CMD] {cmd}")
    proc = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and proc.returncode != 0:
        raise RuntimeError(f"Command failed: {cmd}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}")
    return proc.stdout.strip()


def kube_client():
    """Return CoreV1Api client (load kubeconfig or in-cluster config)."""
    try:
        config.load_kube_config()
    except Exception:
        config.load_incluster_config()
    return client.CoreV1Api()


def node_ready_status(v1, node_name: str) -> str:
    """Return 'True' or 'False' string for Ready condition."""
    node = v1.read_node_status(node_name)
    for cond in node.status.conditions or []:
        if cond.type == "Ready":
            return cond.status
    return "Unknown"


def wait_for_status(v1, node_name: str, target: str, timeout: int) -> bool:
    """Wait until node Ready condition == target within timeout."""
    end_time = time.time() + timeout
    while time.time() < end_time:
        status = node_ready_status(v1, node_name)
        print(f"[INFO] Node {node_name} Ready={status}")
        if status == target:
            return True
        time.sleep(POLL_INTERVAL)
    return False


@pytest.mark.phase5
def test_t5_worker_node_reboot(session):
    """
    Phase 5 — Resilience & Failure Drills
    T5.1 — Reboot a worker hosting app pod (High)

    Steps:
    1. Identify a worker node hosting an app pod.
    2. Drain the node.
    3. Reboot the node.
    4. Wait for node to go NotReady → Ready.
    5. Uncordon the node.
    6. Verify pods rescheduled successfully and PVCs reattached.
    7. Optional: ping the node to confirm reachability.
    """

    v1 = kube_client()

    # Step 1: Identify node hosting an app pod
    pods = v1.list_namespaced_pod(TEST_NAMESPACE).items
    assert pods, f"No pods found in namespace {TEST_NAMESPACE}"
    pod = pods[0]
    node_name = pod.spec.node_name
    print(f"[INFO] Selected pod {pod.metadata.name} running on node {node_name}")

    # Step 2: Drain the node
    run_cmd(f"kubectl drain {node_name} --ignore-daemonsets --delete-emptydir-data --force")

    # Step 3: Reboot the node via SSH
    node_ip = None
    for addr in v1.read_node(node_name).status.addresses:
        if addr.type == "InternalIP":
            node_ip = addr.address
            break
    ssh_target = node_ip or node_name
    if SSH_USER:
        ssh_target = f"{SSH_USER}@{ssh_target}"
    run_cmd(f"ssh -o BatchMode=yes -o ConnectTimeout=8 {ssh_target} 'sudo reboot' &", check=False)

    # Step 4: Wait for node to go NotReady then Ready
    print("[INFO] Waiting for node to go NotReady...")
    wait_for_status(v1, node_name, "False", WAIT_NOTREADY_TIMEOUT)
    print("[INFO] Waiting for node to return Ready...")
    assert wait_for_status(v1, node_name, "True", WAIT_READY_TIMEOUT), \
        f"Node {node_name} did not return to Ready in time"

    # Step 5: Uncordon node
    run_cmd(f"kubectl uncordon {node_name}")

    # Step 6: Verify pods rescheduled + PVCs reattached
    pods_after = v1.list_namespaced_pod(TEST_NAMESPACE).items
    for p in pods_after:
        conds = p.status.conditions or []
        ready = next((c.status for c in conds if c.type == "Ready"), "False")
        assert ready == "True", f"Pod {p.metadata.name} not ready after reboot"
    print(f"[PASS] All pods in namespace {TEST_NAMESPACE} are running and Ready.")

    # Step 7: ping check
    if node_ip:
        rc = os.system(f"ping -c 3 -W 2 {node_ip} > /dev/null 2>&1")
        assert rc == 0, f"Ping to node {node_ip} failed"
        print(f"[PASS] Node {node_name} reachable via ping at {node_ip}")
