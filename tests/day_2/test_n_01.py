import time
import json
import subprocess
from pathlib import Path
import pytest
import paramiko
import os

TID = "N01"
ARTIFACTS_DIR = Path("artifacts")
ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = ARTIFACTS_DIR / f"{TID}_test.log"
TEST_NS = "test-cephfs"
SSH_USER = os.environ.get("SSH_USER")  # SSH username
SSH_PASS = os.environ.get("SSH_PASS")  # SSH password
SSH_PORT = int(os.environ.get("SSH_PORT"))


def log(msg: str):
    """Append logs to single test log file and print."""
    with LOG_FILE.open("a") as f:
        f.write(msg + "\n")
    print(msg)

def ssh_cmd(node_ip: str, command: str) -> str:
    """SSH into node and execute a command."""
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(node_ip, port=SSH_PORT, username=SSH_USER, password=SSH_PASS)

    stdin, stdout, stderr = client.exec_command(f"sudo -S {command}\n")
    stdin.write(f"{SSH_PASS}\n")
    stdin.flush()

    out = stdout.read().decode()
    err = stderr.read().decode()
    client.close()

    entry = f"\n[SSH CMD] {command}\n--- OUT ---\n{out}\n--- ERR ---\n{err}\n"
    log(entry)

    if err and "Failed" in err:
        pytest.fail(f"SSH command failed: {command}\n{err}")
    return out + err

def run_cmd(cmd: str, check=True) -> str:
    """Run a shell command and capture output."""
    try:
        res = subprocess.run(cmd, shell=True, text=True,
                             capture_output=True, check=check)
        entry = f"\n[CMD] {cmd}\n--- OUT ---\n{res.stdout}\n--- ERR ---\n{res.stderr}\n"
        log(entry)
        return res.stdout
    except subprocess.CalledProcessError as e:
        entry = f"\n[CMD FAILED] {cmd}\n--- OUT ---\n{e.stdout}\n--- ERR ---\n{e.stderr}\n"
        log(entry)
        if check:
            pytest.fail(f"Command failed: {cmd}\n{e.stderr}")
        return (e.stdout or "") + (e.stderr or "")

def k(cmd: str) -> str:
    return run_cmd(f"kubectl {cmd}")

def ceph(cmd: str) -> str:
    return run_cmd(f"ceph {cmd}")

def get_pod_names_by_label(namespace: str, label: str) -> list[str]:
    out = run_cmd(f"kubectl -n {namespace} get pods -l {label} -o json")
    items = json.loads(out).get("items", [])
    return [i["metadata"]["name"] for i in items]

def exec_in_pod(namespace: str, pod: str, command: str, check=True) -> str:
    return run_cmd(f"kubectl -n {namespace} exec {pod} -- {command}", check=check)

def is_node_ready(node_name: str) -> bool:
    out = k(f"get node {node_name} -o json")
    data = json.loads(out)
    for cond in data.get("status", {}).get("conditions", []):
        if cond["type"] == "Ready":
            return cond["status"] == "True"
    return False

def get_node_ip(node_name: str) -> str:
    """Fetch InternalIP of node."""
    out = k(f"get node {node_name} -o json")
    data = json.loads(out)
    for addr in data["status"]["addresses"]:
        if addr["type"] == "InternalIP":
            return addr["address"]
    pytest.fail(f"Could not fetch IP for node {node_name}")


def test_worker_node_reboot_graceful():
    log(f"\n===== {TID} — Worker node reboot (graceful) =====")

    # 1. Find CephFS pod and node
    pods = get_pod_names_by_label(TEST_NS, "app=cephfs-tester")
    assert pods, "No CephFS tester pod found"
    pod_name = pods[0]

    out = k(f"-n {TEST_NS} get pod {pod_name} -o wide")
    node_name = out.splitlines()[1].split()[6]  # NODE column
    node_ip = get_node_ip(node_name)
    log(f"Selected pod {pod_name} running on node {node_name} ({node_ip})")

    # 2. Collect pre-reboot evidence
    k("get nodes -o wide")
    ceph("status")
    exec_in_pod(TEST_NS, pod_name, "md5sum /mnt/cephfs/testfile.txt", check=False)

    # 3. Drain node
    k(f"drain {node_name} --ignore-daemonsets --delete-emptydir-data")

    # 4. Reboot node via SSH
    ssh_cmd(node_ip, "reboot")

    # 5. Wait for node Ready (max 5 min)
    log(f"Waiting up to 3 minutes for {node_name} to return Ready...")
    start = time.time()
    node_ready = False
    while time.time() - start < 180:
        if is_node_ready(node_name):
            node_ready = True
            break
        time.sleep(10)
    assert node_ready, f"Node {node_name} did not become Ready in 3 min"
    log(f"Node {node_name} is Ready")

    # 6. Uncordon node
    k(f"uncordon {node_name}")

    # 7. Verify pod rescheduled
    recovered = False
    start = time.time()
    new_pod = pod_name
    while time.time() - start < 120:
        pods_after = get_pod_names_by_label(TEST_NS, "app=cephfs-tester")
        if pods_after and pods_after[0] != pod_name:
            recovered = True
            new_pod = pods_after[0]
            break
        time.sleep(5)
    assert recovered, "Pod did not reschedule to another node within 120s"
    log(f"Pod rescheduled: {pod_name} -> {new_pod}")

    # 8. Verify data intact
    out = exec_in_pod(TEST_NS, new_pod, "cat /mnt/cephfs/testfile.txt", check=False)
    assert "cephfs test OK" in out, "CephFS data lost after node reboot"
    log("Data intact after reboot")

    # 9. Collect post-reboot evidence
    k("get nodes -o wide")
    k(f"-n {TEST_NS} get pods -o wide")
    ceph("status")

    log(f"{TID} Worker node reboot (graceful) test passed!\n")
