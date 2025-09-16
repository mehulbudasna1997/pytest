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
TEST_NS = "test-cephfs"

SSH_USER = os.environ.get("SSH_USER")  # SSH username
SSH_PASS = os.environ.get("SSH_PASS")  # SSH password
SSH_PORT = int(os.environ.get("SSH_PORT", 22))


def run_cmd(cmd: str, out_file: Path, check=True) -> str:
    """Run a shell command and capture output."""
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


def get_pod_names_by_label(namespace: str, label: str, tid: str, fname: str) -> list[str]:
    """Get pods in a namespace by label selector."""
    out = run_cmd(
        f"kubectl -n {namespace} get pods -l {label} -o json",
        ARTIFACTS_DIR / f"{tid}_{fname}.json"
    )
    items = json.loads(out).get("items", [])
    return [i["metadata"]["name"] for i in items]


def exec_in_pod(namespace: str, pod: str, command: str, tid: str, tag: str, check=True) -> str:
    """Run a command inside a pod."""
    return run_cmd(
        f"kubectl -n {namespace} exec {pod} -- {command}",
        ARTIFACTS_DIR / f"{tid}_{tag}.log",
        check=check
    )


def ceph(cmd: str, out_file: Path, check=True) -> str:
    """Run a Ceph command."""
    return run_cmd(f"ceph {cmd}", out_file, check=check)


def ssh_reboot_node(node_ip: str):
    """SSH into node and reboot gracefully."""
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(node_ip, port=SSH_PORT, username=SSH_USER, password=SSH_PASS)

    # Run reboot
    stdin, stdout, stderr = client.exec_command("sudo -S reboot")
    stdin.write(f"{SSH_PASS}\n")
    stdin.flush()

    out = stdout.read().decode()
    err = stderr.read().decode()
    print(f"⚡ SSH reboot sent to {node_ip}. STDOUT:\n{out}\nSTDERR:\n{err}")

    client.close()


def is_node_ready(node_name: str) -> bool:
    """Check if node Ready condition is True."""
    out = run_cmd(f"kubectl get node {node_name} -o json",
                  ARTIFACTS_DIR / f"{TID}_node.json")
    data = json.loads(out)
    for cond in data.get("status", {}).get("conditions", []):
        if cond["type"] == "Ready":
            return cond["status"] == "True"
    return False


@pytest.mark.high
def test_worker_node_reboot_graceful():
    """N-01: Worker node reboot (graceful)"""

    # 1. Find CephFS tester pod + node
    pods = get_pod_names_by_label(TEST_NS, "app=cephfs-tester", TID, "pods")
    assert pods, "No CephFS tester pod found"
    pod_name = pods[0]

    out = run_cmd(f"kubectl -n {TEST_NS} get pod {pod_name} -o wide",
                  ARTIFACTS_DIR / f"{TID}_pod_info.log")
    node_name = out.splitlines()[1].split()[6]  # NODE column
    node_ip = run_cmd(
        f"kubectl get node {node_name} -o jsonpath='{{.status.addresses[?(@.type==\"InternalIP\")].address}}'",
        ARTIFACTS_DIR / f"{TID}_node_ip.log"
    ).strip()

    print(f"⚡ Selected pod {pod_name} on node {node_name} ({node_ip})")

    # 2. Baseline evidence
    run_cmd("kubectl get nodes -o wide", ARTIFACTS_DIR / f"{TID}_nodes_before.log")
    ceph("status", ARTIFACTS_DIR / f"{TID}_ceph_before.log")
    exec_in_pod(TEST_NS, pod_name, "md5sum /mnt/cephfs/testfile.txt", TID, "md5_before", check=False)

    # 3. Drain node gracefully
    run_cmd(f"kubectl drain {node_name} --ignore-daemonsets --delete-emptydir-data --force --grace-period=30",
            ARTIFACTS_DIR / f"{TID}_drain.log")
    print(f"Node {node_name} drained")

    # 4. Reboot node via SSH
    print(f"Rebooting node {node_name} ({node_ip}) via SSH...")
    ssh_reboot_node(node_ip)

    # 5. Wait for node Ready (max 5 min)
    start = time.time()
    node_ready = False
    while time.time() - start < 300:
        if is_node_ready(node_name):
            node_ready = True
            break
        time.sleep(10)
    assert node_ready, f"Node {node_name} did not become Ready within 5 min"

    # 6. Uncordon node
    run_cmd(f"kubectl uncordon {node_name}", ARTIFACTS_DIR / f"{TID}_uncordon.log")

    # 7. Verify pod rescheduled
    recovered = False
    start = time.time()
    new_pod = pod_name
    while time.time() - start < 120:
        pods_after = get_pod_names_by_label(TEST_NS, "app=cephfs-tester", TID, "pods_after")
        if pods_after and pods_after[0] != pod_name:
            recovered = True
            new_pod = pods_after[0]
            break
        time.sleep(5)
    assert recovered, "Pod did not reschedule to another node within 120s"

    # 8. Verify data intact
    out = exec_in_pod(TEST_NS, new_pod, "cat /mnt/cephfs/testfile.txt", TID, "md5_after", check=False)
    assert "cephfs test OK" in out, "CephFS data lost after node reboot"

    # 9. Post-reboot evidence
    run_cmd("kubectl get nodes -o wide", ARTIFACTS_DIR / f"{TID}_nodes_after.log")
    run_cmd(f"kubectl -n {TEST_NS} get pods -o wide", ARTIFACTS_DIR / f"{TID}_pods_after.log")
    ceph("status", ARTIFACTS_DIR / f"{TID}_ceph_after.log")

    print("✅ Worker node reboot (graceful) test passed! Pod healthy and CephFS intact.")
