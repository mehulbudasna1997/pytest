import os
import subprocess
import json
import paramiko
from pathlib import Path
from datetime import datetime
import pytest

# ---------- Config ----------
TID = "N03"
ARTIFACTS_DIR = Path("artifacts")
ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = ARTIFACTS_DIR / f"{TID}_test.log"

WORKER_NODE = os.environ.get("WORKER_NODE", "worker-1")
WORKER_IP = os.environ.get("WORKER_IP", "192.168.0.101")
SSH_USER = os.environ.get("SSH_USER", "ubuntu")
SSH_KEY = os.environ.get("SSH_KEY", str(Path.home() / ".ssh/id_rsa"))

# ---------- Logger ----------
def log(msg: str):
    """Append logs to single file + print to console."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = f"[{timestamp}] {msg}"
    with LOG_FILE.open("a") as f:
        f.write(entry + "\n")
    print(entry)

def run_cmd(cmd: str) -> str:
    """Run shell command and capture stdout/stderr."""
    log(f"$ {cmd}")
    proc = subprocess.run(cmd, shell=True, text=True, capture_output=True)
    if proc.stdout:
        log("--- STDOUT ---")
        log(proc.stdout.strip())
    if proc.stderr:
        log("--- STDERR ---")
        log(proc.stderr.strip())
    return proc.stdout.strip()

def ssh_cmd(ip: str, user: str, key: str, cmd: str) -> str:
    """Run remote SSH command using paramiko and log output."""
    log(f"[SSH {ip}] $ {cmd}")
    key_obj = paramiko.RSAKey.from_private_key_file(key)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(ip, username=user, pkey=key_obj)
    stdin, stdout, stderr = client.exec_command(cmd)
    out, err = stdout.read().decode(), stderr.read().decode()
    if out:
        log("--- STDOUT ---")
        log(out.strip())
    if err:
        log("--- STDERR ---")
        log(err.strip())
    client.close()
    return out.strip()

# ---------- Test Case ----------
def test_kubelet_restart_on_worker():
    log(f"===== {TID} — Kubelet restart on worker =====")

    # 1. Record pods before restart
    run_cmd("kubectl get pods -o wide -n test-cephfs")

    # 2. Record node status before restart
    run_cmd(f"kubectl describe node {WORKER_NODE}")

    # 3. Check mounts inside tester pod
    pod = run_cmd("kubectl get pod -n test-cephfs -l app=cephfs-tester "
                  "-o jsonpath='{.items[0].metadata.name}'")
    run_cmd(f"kubectl exec -n test-cephfs {pod} -- mount | grep ceph")

    # 4. Restart kubelet on worker
    ssh_cmd(WORKER_IP, SSH_USER, SSH_KEY, "sudo systemctl restart kubelet")

    # 5. Wait & recheck pod readiness
    run_cmd("kubectl get pods -o wide -n test-cephfs --watch --timeout=30s")

    # 6. Verify mounts are still present
    run_cmd(f"kubectl exec -n test-cephfs {pod} -- mount | grep ceph")

    # 7. Sanity check: I/O after restart
    run_cmd(f"kubectl exec -n test-cephfs {pod} -- bash -c 'echo N03test > /mnt/cephfs/testfile && cat /mnt/cephfs/testfile'")

    # 8. Nodes after restart
    run_cmd(f"kubectl describe node {WORKER_NODE}")

    log(f"===== {TID} Test completed successfully =====")
