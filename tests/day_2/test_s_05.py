# test_s_05.py
import time
import subprocess
import json
from pathlib import Path
import pytest

# Import SSH helper from test_ssh.py
from test_ssh import run_ssh_cmd

TID = "S05"
ARTIFACTS_DIR = Path("artifacts")
ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

ROOK_NS = "rook-ceph"
TEST_NS = "test-cephfs"


# --- Local utility functions (same as N-01/N-10 style) ---
def run_cmd(cmd: str, out_file: Path, check=True) -> str:
    """Run shell command locally and capture output."""
    try:
        res = subprocess.run(cmd, shell=True, text=True,
                             capture_output=True, check=check)
        out_file.write_text((res.stdout or "") + (res.stderr or ""))
        return res.stdout
    except subprocess.CalledProcessError as e:
        out_file.write_text((e.stdout or "") + (e.stderr or ""))
        if check:
            pytest.fail(f"Command failed: {cmd}\n{e.stderr}")
        return (e.stdout or "") + (e.stderr or "")


def get_pod_names_by_label(namespace: str, label: str, tid: str, fname: str) -> list[str]:
    out = run_cmd(
        f"kubectl -n {namespace} get pods -l {label} -o json",
        ARTIFACTS_DIR / f"{tid}_{fname}.json",
        check=False,
    )
    try:
        items = json.loads(out).get("items", [])
    except json.JSONDecodeError:
        return []
    return [i["metadata"]["name"] for i in items]


def exec_in_pod(namespace: str, pod: str, command: str, tid: str, tag: str, check=True) -> str:
    return run_cmd(
        f"kubectl -n {namespace} exec {pod} -- {command}",
        ARTIFACTS_DIR / f"{tid}_{tag}.log",
        check=check
    )


def wait_rollout(namespace: str, resource: str, tid: str, timeout=180):
    """Wait until a deployment is fully rolled out."""
    start = time.time()
    while time.time() - start < timeout:
        out = run_cmd(f"kubectl -n {namespace} rollout status {resource}",
                      ARTIFACTS_DIR / f"{tid}_rollout.log", check=False)
        if "successfully rolled out" in out:
            return
        time.sleep(5)
    pytest.fail(f"{resource} did not rollout in {timeout}s")


# --- Test ---
def test_rook_operator_restart_continuity():
    """S-05: Rook operator restart continuity"""
    # Step 1: Collect pre-restart evidence
    run_ssh_cmd("ceph -s", ARTIFACTS_DIR / f"{TID}_ceph_before.log")

    pods = get_pod_names_by_label(TEST_NS, "app=cephfs-tester", TID, "pods_before")
    assert pods, "No cephfs tester pod found"
    pod_name = pods[0]

    # Verify IO before restart
    exec_in_pod(TEST_NS, pod_name, "echo pre-restart >> /mnt/cephfs/testfile.txt", TID, "io_before")
    out = exec_in_pod(TEST_NS, pod_name, "tail -n 1 /mnt/cephfs/testfile.txt", TID, "read_before")
    assert "pre-restart" in out, "Failed to verify IO before operator restart"

    # Step 2: Restart rook-ceph-operator (via SSH)
    run_ssh_cmd(f"kubectl -n {ROOK_NS} rollout restart deploy/rook-ceph-operator",
                ARTIFACTS_DIR / f"{TID}_restart_cmd.log")

    # Step 3: Wait for operator to come back
    wait_rollout(ROOK_NS, "deploy/rook-ceph-operator", TID, timeout=180)

    # Step 4: Verify IO after restart
    exec_in_pod(TEST_NS, pod_name, "echo post-restart >> /mnt/cephfs/testfile.txt", TID, "io_after")
    out = exec_in_pod(TEST_NS, pod_name, "tail -n 1 /mnt/cephfs/testfile.txt", TID, "read_after")
    assert "post-restart" in out, "IO failed after rook operator restart"

    # Step 5: Collect post-restart evidence
    run_ssh_cmd("ceph -s", ARTIFACTS_DIR / f"{TID}_ceph_after.log")
    run_ssh_cmd(f"kubectl -n {ROOK_NS} get pods -o wide", ARTIFACTS_DIR / f"{TID}_rook_pods_after.log")

    print("✅ Rook operator restart test passed (S-05)")
