import os
import time
import json
import subprocess
from pathlib import Path
import pytest
import paramiko

TID = "N04"
ARTIFACTS_DIR = Path("artifacts")
ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
TEST_NS = "test-cephfs"

# SSH credentials from environment variables
SSH_USER = os.environ.get("SSH_USER")
SSH_PASS = os.environ.get("SSH_PASS")
SSH_PORT = int(os.environ.get("SSH_PORT"))

# Utility to run local shell commands
def run_cmd(cmd: str, out_file: Path, check=True) -> str:
    try:
        res = subprocess.run(cmd, shell=True, text=True,
                             capture_output=True, check=check)
        out_file.write_text(res.stdout + (res.stderr or ""))
        return res.stdout
    except subprocess.CalledProcessError as e:
        out_file.write_text((e.stdout or "") + (e.stderr or ""))
        if check:
            pytest.fail(f"Command failed: {cmd}\n{e.stderr}")
        return (e.stdout or "") + (e.stderr or "")

# Get pod names by label
def get_pod_names_by_label(namespace: str, label: str, tid: str, fname: str) -> list[str]:
    out = run_cmd(f"kubectl -n {namespace} get pods -l {label} -o json",
                  ARTIFACTS_DIR / f"{tid}_{fname}.json")
    items = json.loads(out).get("items", [])
    return [i["metadata"]["name"] for i in items]

# Run command inside pod
def exec_in_pod(namespace: str, pod: str, command: str, tid: str, tag: str, check=True) -> str:
    return run_cmd(f"kubectl -n {namespace} exec {pod} -- {command}",
                   ARTIFACTS_DIR / f"{tid}_{tag}.log", check=check)

# Run ceph command
def ceph(cmd: str, out_file: Path, check=True) -> str:
    return run_cmd(f"ceph {cmd}", out_file, check=check)

# SSH into node and run command
def ssh_run(node_ip: str, cmd: str):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(node_ip, port=SSH_PORT, username=SSH_USER, password=SSH_PASS)
    stdin, stdout, stderr = client.exec_command(cmd)
    out = stdout.read().decode()
    err = stderr.read().decode()
    client.close()
    return out, err

@pytest.mark.timeout(600)
def test_container_runtime_restart():
    # 1. Identify a CephFS tester pod and its node
    pods = get_pod_names_by_label(TEST_NS, "app=cephfs-tester", TID, "pods")
    assert pods, "No CephFS tester pod found"
    pod_name = pods[0]

    pod_info = run_cmd(f"kubectl -n {TEST_NS} get pod {pod_name} -o wide",
                       ARTIFACTS_DIR / f"{TID}_pod_info.log")
    node_name = pod_info.splitlines()[1].split()[6]  # NODE column

    print(f"Pod {pod_name} is on node {node_name}")

    # 2. Collect pre-restart evidence
    run_cmd("kubectl get nodes -o wide", ARTIFACTS_DIR / f"{TID}_nodes_before.log")
    ceph("status", ARTIFACTS_DIR / f"{TID}_ceph_before.log")
    exec_in_pod(TEST_NS, pod_name, "md5sum /mnt/cephfs/testfile.txt", TID, "md5_before", check=False)

    # 3. Restart container runtime via SSH
    print(f"Restarting containerd on node {node_name} via SSH...")
    out, err = ssh_run(node_name, "sudo systemctl restart containerd")
    ARTIFACTS_DIR.joinpath(f"{TID}_ssh_output.log").write_text(out + "\n" + err)

    # 4. Wait for pod to be Running again
    start = time.time()
    recovered = False
    while time.time() - start < 180:  # 3 minutes max
        pods_status = run_cmd(f"kubectl -n {TEST_NS} get pod {pod_name}",
                              ARTIFACTS_DIR / f"{TID}_pod_status.log")
        if "Running" in pods_status and "0/1" not in pods_status:
            recovered = True
            break
        time.sleep(5)
    assert recovered, f"Pod {pod_name} did not recover after container runtime restart"

    # 5. Verify CephFS data still accessible
    out = exec_in_pod(TEST_NS, pod_name, "cat /mnt/cephfs/testfile.txt", TID, "md5_after", check=False)
    assert "cephfs test OK" in out, "CephFS data lost after container runtime restart"

    # 6. Collect post-restart evidence
    run_cmd("kubectl get nodes -o wide", ARTIFACTS_DIR / f"{TID}_nodes_after.log")
    run_cmd(f"kubectl -n {TEST_NS} get pods -o wide", ARTIFACTS_DIR / f"{TID}_pods_after.log")
    ceph("status", ARTIFACTS_DIR / f"{TID}_ceph_after.log")

    print("✅ Container runtime restart test passed! Pod remained healthy and CephFS volumes intact.")
