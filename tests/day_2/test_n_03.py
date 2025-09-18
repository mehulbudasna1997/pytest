import os
import json
import logging
import paramiko
import pytest
import time
from kubernetes import client, config

# --- Logging setup ---
LOG_FILE = "artifacts/N03_full.log"
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# --- Env vars ---
KUBECONFIG = os.environ.get("KUBECONFIG")  # optional
SSH_USER = os.environ.get("SSH_USER")
SSH_PASS = os.environ.get("SSH_PASS")
SSH_PORT = int(os.environ.get("SSH_PORT"))


# --- Helpers ---
def kube_clients():
    """Load kube config and return CoreV1Api client."""
    try:
        if KUBECONFIG:
            config.load_kube_config(config_file=KUBECONFIG)
        else:
            config.load_kube_config()
    except Exception:
        config.load_incluster_config()
    return client.CoreV1Api()


def run_ssh_command(ip: str, command: str) -> str:
    """Run SSH command using password-based login."""
    logger.info(f"[SSH] {ip} $ {command}")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ip, port=SSH_PORT, username=SSH_USER, password=SSH_PASS)
    stdin, stdout, stderr = ssh.exec_command(command)
    out = stdout.read().decode()
    err = stderr.read().decode()
    ssh.close()
    if out:
        logger.info(f"[SSH-OUT] {out.strip()}")
    if err:
        logger.error(f"[SSH-ERR] {err.strip()}")
    return out.strip()


# --- Test ---
def test_kubelet_restart():
    """N-03 — Kubelet restart on worker node with password auth"""
    logger.info("=== Starting N-03 (Kubelet restart test) ===")

    v1 = kube_clients()

    # 1. Pick a worker node (exclude control-plane/master labels)
    nodes = v1.list_node().items
    worker_nodes = [n for n in nodes if "master" not in n.metadata.name]
    assert worker_nodes, "No worker nodes found"
    node = worker_nodes[0]
    node_name = node.metadata.name
    node_ip = node.status.addresses[0].address
    logger.info(f"Selected worker node: {node_name} ({node_ip})")

    # 2. Pods before restart
    pods_before = v1.list_pod_for_all_namespaces(watch=False)
    logger.info(f"Pods before restart: {len(pods_before.items)}")

    # 3. Restart kubelet
    run_ssh_command(node_ip, "sudo systemctl restart kubelet")
    logger.info("Kubelet restarted successfully")

    # 4. Wait for node to become Ready again
    ready = False
    for _ in range(30):
        node_status = v1.read_node_status(node_name)
        for cond in node_status.status.conditions:
            if cond.type == "Ready" and cond.status == "True":
                ready = True
        if ready:
            break
        time.sleep(10)
    assert ready, "Node did not return to Ready state"
    logger.info("Node is Ready after kubelet restart")

    # 5. Pods after restart
    pods_after = v1.list_pod_for_all_namespaces(watch=False)
    logger.info(f"Pods after restart: {len(pods_after.items)}")

    # 6. Validate pod continuity
    assert len(pods_before.items) == len(pods_after.items), \
        "Pod count mismatch after kubelet restart"

    logger.info("N-03 passed — kubelet restart caused no disruption")
