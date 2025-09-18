import os
import time
import json
import subprocess
from pathlib import Path
import pytest
import paramiko
from datetime import datetime

TID = "N04"
ARTIFACTS_DIR = Path("artifacts")
ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = ARTIFACTS_DIR / f"{TID}_test.log"
TEST_NS = "test-cephfs"

# SSH credentials from environment variables
SSH_USER = os.environ.get("SSH_USER")
SSH_PASS = os.environ.get("SSH_PASS")
SSH_PORT = int(os.environ.get("SSH_PORT", "22"))

# ---------- Logger ----------
def log(msg: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = f"[{timestamp}] {msg}"
    with LOG_FILE.open("a") as f:
        f.write(entry + "\n")
    print(entry)

# ---------- Run local command ----------
def run_cmd(cmd: str, check=True) -> str:
    log(f"$ {cmd}")
    try:
        res = subprocess.run(cmd, shell=True, text=True,
                             capture_output=True, check=check)
        if res.stdout:
            log("--- STDOUT ---")
            log(res.stdout.strip())
        if res.stderr:
            log("--- STDERR ---")
            log(res.stderr.strip())
        return res.stdout
    except subprocess.CalledProcessError as e:
        if e.stdout:
            log("--- STDOUT ---")
            log(e.stdout.strip())
        if e.stderr:
            log("--- STDERR ---")
            log(e.stderr.strip())
        if check:
            pytest.fail(f"Command failed: {cmd}\n{e.stderr}")
        return (e.stdout or "") + (e.stderr or "")

# ---------- Pod helpers ----------
def get_pod_names_by_label(namespace: str, label: str) -> list[str]:
    out = run_cmd(f"kubectl -n {namespace} get pods -l {label} -o json")
    items = json.loads(out).get("items", [])
    return [i["metadata"]["name"] for i in items]

def exec_in_pod(namespace: str, pod: str, command: str, check=True) -> str:
    return run_cmd(f"kubectl -n {namespace} exec {pod} -- {command}", check=check)

# ---------- Ceph ----------
def ceph(cmd: str, check=True) -> str:
    return run_cmd(f"ceph {cmd}", check=check)

# ---------- SSH ----------
def ssh_run(node_ip: str, cmd: str):
    log(f"[SSH {node_ip}] $ {cmd}")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(node_ip, port=SSH_PORT, username=SSH_USER, password=SSH_PASS)
    stdin, stdout, stderr = client.exec_command(cmd)
    out = stdout.read().decode()
    err = stderr.read().decode()
    if out:
        log("--- STDOUT ---")
        log(out.strip())
    if err:
        log("--- STDERR ---")
        log(err.strip())
    client.close()
    return out, err

# ---------- Test ----------
@pytest.mark.timeout(600)
def test_container_runtime_restart():
    log(f"===== {TID} — Container runtime restart =====")

    # 1. Identify tester pod + node
    pods = get_pod_names_by_label(TEST_NS, "app=cephfs-tester")
    assert pods, "No CephFS tester pod found"
    pod_name = pods[0]

    pod_info = run_cmd(f"kubectl -n {TEST_NS} get pod {pod_name} -o wide")
    node_name = pod_info.splitlines()[1].split()[6]
    log(f"Pod {pod_name} is on node {node_name}")

    # 2. Pre-restart evidence
    run_cmd("kubectl get nodes -o wide")
    ceph("status")
    exec_in_pod(TEST_NS, pod_name, "md5sum /mnt/cephfs/testfile.txt", check=False)

    # 3. Restart container runtime
    log(f"Restarting containerd on node {node_name} via SSH...")
    ssh_run(node_name, "sudo systemctl restart containerd")

    # 4. Wait for pod recovery
    start = time.time()
    recovered = False
    while time.time() - start < 180:
        pods_status = run_cmd(f"kubectl -n {TEST_NS} get pod {pod_name}", check=False)
        if "Running" in pods_status and "0/1" not in pods_status:
            recovered = True
            break
        time.sleep(5)
    assert recovered, f"Pod {pod_name} did not recover after runtime restart"

    # 5. Verify CephFS data
    out = exec_in_pod(TEST_NS, pod_name, "cat /mnt/cephfs/testfile.txt", check=False)
    assert "cephfs test OK" in out, "CephFS data lost after container runtime restart"

    # 6. Post-restart evidence
    run_cmd("kubectl get nodes -o wide")
    run_cmd(f"kubectl -n {TEST_NS} get pods -o wide")
    ceph("status")

    log(f"✅ {TID} Test completed successfully — Pod healthy & CephFS intact")
