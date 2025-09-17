# test_s_07.py
import time
import subprocess
import json
from pathlib import Path
import pytest

from test_ssh import run_ssh_cmd   # SSH helper

TID = "S07"
ARTIFACTS_DIR = Path("artifacts")
ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

ROOK_NS = "rook-ceph"
TEST_NS = "test-cephfs"


# --- Utility functions ---
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


def save_manifest(tid: str, name: str, manifest: dict) -> Path:
    import yaml
    path = ARTIFACTS_DIR / f"{tid}_{name}.yaml"
    with path.open("w") as f:
        yaml.dump(manifest, f)
    return path


def apply_manifest(manifest_path: Path, tid: str, tag: str):
    run_cmd(f"kubectl apply -f {manifest_path}", ARTIFACTS_DIR / f"{tid}_{tag}.log")


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


def wait_pod_ready(namespace: str, pod: str, tid: str, timeout=120):
    start = time.time()
    while time.time() - start < timeout:
        out = run_cmd(f"kubectl -n {namespace} get pod {pod} -o json",
                      ARTIFACTS_DIR / f"{tid}_wait_pod.json", check=False)
        try:
            pod_spec = json.loads(out)
            phase = pod_spec["status"]["phase"]
            if phase == "Running":
                return
        except Exception:
            pass
        time.sleep(5)
    pytest.fail(f"Pod {pod} not Running within {timeout}s")


def wait_pvc_resize(namespace: str, pvc: str, size: str, tid: str, timeout=180):
    start = time.time()
    while time.time() - start < timeout:
        out = run_cmd(f"kubectl -n {namespace} get pvc {pvc} -o json",
                      ARTIFACTS_DIR / f"{tid}_pvc_status.json", check=False)
        try:
            data = json.loads(out)
            cap = data["status"]["capacity"]["storage"]
            if cap == size:
                return
        except Exception:
            pass
        time.sleep(5)
    pytest.fail(f"PVC {pvc} did not resize to {size} within {timeout}s")


# --- Test ---
def test_pvc_online_expansion_during_load():
    """S-07: PVC online expansion during load"""

    # Step 1: Create PVC (1Gi)
    pvc_manifest = {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": "s07-pvc", "namespace": TEST_NS},
        "spec": {
            "accessModes": ["ReadWriteMany"],
            "resources": {"requests": {"storage": "1Gi"}},
            "storageClassName": "cephfs",
        },
    }
    pvc_path = save_manifest(TID, "pvc", pvc_manifest)
    apply_manifest(pvc_path, TID, "pvc_apply")

    # Step 2: Create pod using the PVC
    pod_manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "s07-writer", "namespace": TEST_NS, "labels": {"app": "s07-writer"}},
        "spec": {
            "containers": [{
                "name": "busybox",
                "image": "busybox:1.36",
                "command": ["sh", "-c", "while true; do echo hello >> /mnt/data/out.txt; sleep 2; done"],
                "volumeMounts": [{"mountPath": "/mnt/data", "name": "data"}],
            }],
            "volumes": [{"name": "data", "persistentVolumeClaim": {"claimName": "s07-pvc"}}],
        },
    }
    pod_path = save_manifest(TID, "pod", pod_manifest)
    apply_manifest(pod_path, TID, "pod_apply")
    wait_pod_ready(TEST_NS, "s07-writer", TID)

    # Step 3: Verify pod writing data
    pods = get_pod_names_by_label(TEST_NS, "app=s07-writer", TID, "pods")
    assert pods, "No writer pod found"
    pod = pods[0]
    out = exec_in_pod(TEST_NS, pod, "tail -n 5 /mnt/data/out.txt", TID, "pre_resize")
    assert "hello" in out, "Writer pod not writing data before resize"

    # Step 4: Expand PVC from 1Gi -> 5Gi
    run_ssh_cmd("kubectl -n test-cephfs patch pvc s07-pvc -p '{\"spec\":{\"resources\":{\"requests\":{\"storage\":\"5Gi\"}}}}'",
                ARTIFACTS_DIR / f"{TID}_pvc_patch.log")
    wait_pvc_resize(TEST_NS, "s07-pvc", "5Gi", TID, timeout=300)

    # Step 5: Verify pod still writing data after expansion
    out = exec_in_pod(TEST_NS, pod, "tail -n 5 /mnt/data/out.txt", TID, "post_resize")
    assert "hello" in out, "Writer pod stopped writing after PVC expansion"

    # Step 6: Collect evidence
    run_ssh_cmd("kubectl -n test-cephfs get pvc s07-pvc -o wide", ARTIFACTS_DIR / f"{TID}_pvc_after.log")
    run_ssh_cmd("kubectl -n test-cephfs get pods -o wide", ARTIFACTS_DIR / f"{TID}_pods_after.log")

    # Cleanup
    run_cmd("kubectl -n test-cephfs delete pod s07-writer --ignore-not-found", ARTIFACTS_DIR / f"{TID}_cleanup_pod.log")
    run_cmd("kubectl -n test-cephfs delete pvc s07-pvc --ignore-not-found", ARTIFACTS_DIR / f"{TID}_cleanup_pvc.log")

    print("✅ PVC online expansion test passed (S-07)")
