import time
import json
import subprocess
from pathlib import Path
import pytest
import paramiko

TID = "N03"
ARTIFACTS_DIR = Path("artifacts")
ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
TEST_NS = "test-cephfs"

# SSH config (fill in your values)
SSH_USER = "ubuntu"            # change as per your cluster node
SSH_PASS = "yourpassword"      # or fetch from ENV
SSH_PORT = 22


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


def ssh_restart_kubelet(node_ip: str):
    """SSH into node and restart kubelet service."""
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(node_ip, port=SSH_PORT, username=SSH_USER, password=SSH_PASS)

    stdin, stdout, stderr = client.exec_command("sudo -S systemctl restart kubelet\n")
    stdin.write(f"{SSH_PASS}\n")
    stdin.flush()

    out = stdout.read().decode()
    err = stderr.read().decode()
    client.close()

    log_path = ARTIFACTS_DIR / f"{TID}_kubelet_restart.log"
    log_path.write_text(out + err)
    if err and "Failed" in err:
        pytest.fail(f"Kubelet restart failed: {err}")
    return out


def test_kubelet_restart_worker():
    """
    Objective:
        Verify CephFS workloads tolerate kubelet restart.

    Steps:
        1. Identify CephFS tester pod + node
        2. Collect baseline evidence
        3. Restart kubelet via SSH
        4. Wait for pod recovery
        5. Validate data intact
        6. Collect post-restart evidence

    Expected:
        - Pod recovers within 3 minutes
        - No data loss in CephFS
        - Ceph cluster stays healthy
    """
    # 1. Find CephFS tester pod + node
    pods = get_pod_names_by_label(TEST_NS, "app=cephfs-tester", TID, "pods")
    assert pods, "No CephFS tester pod found"
    pod_name = pods[0]

    out = run_cmd(
        f"kubectl -n {TEST_NS} get pod {pod_name} -o wide",
        ARTIFACTS_DIR / f"{TID}_pod_info.log"
    )
    node_name = out.splitlines()[1].split()[6]  # NODE column
    node_ip = run_cmd(
        f"kubectl get node {node_name} -o jsonpath='{{.status.addresses[?(@.type==\"InternalIP\")].address}}'",
        ARTIFACTS_DIR / f"{TID}_node_ip.log"
    ).strip()

    # 2. Baseline evidence
    run_cmd("kubectl get nodes -o wide", ARTIFACTS_DIR / f"{TID}_nodes_before.log")
    ceph("status", ARTIFACTS_DIR / f"{TID}_ceph_before.log")
    exec_in_pod(TEST_NS, pod_name, "md5sum /mnt/cephfs/testfile.txt", TID, "md5_before", check=False)

    # 3. Restart kubelet on worker node via SSH
    print(f"⚡ Restarting kubelet on {node_name} ({node_ip})...")
    ssh_restart_kubelet(node_ip)

    # 4. Wait for pod recovery
    start = time.time()
    recovered = False
    while time.time() - start < 180:  # 3 min
        pods_status = run_cmd(
            f"kubectl -n {TEST_NS} get pod {pod_name}",
            ARTIFACTS_DIR / f"{TID}_pod_status.log"
        )
        if "Running" in pods_status and "0/1" not in pods_status:
            recovered = True
            break
        time.sleep(5)
    assert recovered, f"Pod {pod_name} did not recover after kubelet restart"

    # 5. Data check
    out = exec_in_pod(TEST_NS, pod_name, "cat /mnt/cephfs/testfile.txt", TID, "md5_after", check=False)
    assert "cephfs test OK" in out, "CephFS data lost after kubelet restart"

    # 6. Post-restart evidence
    run_cmd("kubectl get nodes -o wide", ARTIFACTS_DIR / f"{TID}_nodes_after.log")
    run_cmd(f"kubectl -n {TEST_NS} get pods -o wide", ARTIFACTS_DIR / f"{TID}_pods_after.log")
    ceph("status", ARTIFACTS_DIR / f"{TID}_ceph_after.log")

    print("✅ Automated kubelet restart test passed! Pod healthy, CephFS intact.")
