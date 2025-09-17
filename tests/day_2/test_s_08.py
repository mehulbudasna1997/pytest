# test_s_08.py
import time
import subprocess
import json
from pathlib import Path
import pytest

from test_ssh import run_ssh_cmd   # SSH helper for kubectl from admin node

TID = "S08"
ARTIFACTS_DIR = Path("artifacts")
ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

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
            if pod_spec["status"]["phase"] == "Running":
                return
        except Exception:
            pass
        time.sleep(5)
    pytest.fail(f"Pod {pod} not Running within {timeout}s")


def wait_snapshot_ready(namespace: str, snap: str, tid: str, timeout=120):
    start = time.time()
    while time.time() - start < timeout:
        out = run_cmd(f"kubectl -n {namespace} get volumesnapshot {snap} -o json",
                      ARTIFACTS_DIR / f"{tid}_snap.json", check=False)
        try:
            snap_spec = json.loads(out)
            if snap_spec["status"]["readyToUse"]:
                return
        except Exception:
            pass
        time.sleep(5)
    pytest.fail(f"Snapshot {snap} not ready in {timeout}s")


# --- Test ---
def test_snapshot_restore_under_load():
    """S-08: Snapshot/Restore under load"""

    # Step 1: Create PVC + writer pod
    pvc_manifest = {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": "s08-src-pvc", "namespace": TEST_NS},
        "spec": {
            "accessModes": ["ReadWriteMany"],
            "resources": {"requests": {"storage": "1Gi"}},
            "storageClassName": "cephfs",
        },
    }
    pvc_path = save_manifest(TID, "pvc", pvc_manifest)
    apply_manifest(pvc_path, TID, "pvc_apply")

    pod_manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "s08-writer", "namespace": TEST_NS, "labels": {"app": "s08-writer"}},
        "spec": {
            "containers": [{
                "name": "busybox",
                "image": "busybox:1.36",
                "command": ["sh", "-c", "i=0; while true; do echo data_$i >> /mnt/data/testfile.txt; i=$((i+1)); sleep 1; done"],
                "volumeMounts": [{"mountPath": "/mnt/data", "name": "data"}],
            }],
            "volumes": [{"name": "data", "persistentVolumeClaim": {"claimName": "s08-src-pvc"}}],
        },
    }
    pod_path = save_manifest(TID, "pod", pod_manifest)
    apply_manifest(pod_path, TID, "pod_apply")
    wait_pod_ready(TEST_NS, "s08-writer", TID)

    # Step 2: Wait for some writes
    time.sleep(10)
    pods = get_pod_names_by_label(TEST_NS, "app=s08-writer", TID, "pods")
    pod = pods[0]
    exec_in_pod(TEST_NS, pod, "cp /mnt/data/testfile.txt /mnt/data/checkpoint.txt", TID, "checkpoint")

    # Step 3: Take snapshot of PVC
    snap_manifest = {
        "apiVersion": "snapshot.storage.k8s.io/v1",
        "kind": "VolumeSnapshot",
        "metadata": {"name": "s08-snap", "namespace": TEST_NS},
        "spec": {
            "source": {"persistentVolumeClaimName": "s08-src-pvc"},
            "volumeSnapshotClassName": "csi-cephfsplugin-snapclass"
        }
    }
    snap_path = save_manifest(TID, "snap", snap_manifest)
    apply_manifest(snap_path, TID, "snap_apply")
    wait_snapshot_ready(TEST_NS, "s08-snap", TID)

    # Step 4: Create PVC from snapshot
    clone_pvc_manifest = {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": "s08-clone-pvc", "namespace": TEST_NS},
        "spec": {
            "accessModes": ["ReadWriteMany"],
            "resources": {"requests": {"storage": "1Gi"}},
            "storageClassName": "cephfs",
            "dataSource": {
                "name": "s08-snap",
                "kind": "VolumeSnapshot",
                "apiGroup": "snapshot.storage.k8s.io"
            }
        }
    }
    clone_path = save_manifest(TID, "clone_pvc", clone_pvc_manifest)
    apply_manifest(clone_path, TID, "clone_pvc_apply")

    # Step 5: Pod to read from clone
    reader_manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "s08-reader", "namespace": TEST_NS, "labels": {"app": "s08-reader"}},
        "spec": {
            "containers": [{
                "name": "busybox",
                "image": "busybox:1.36",
                "command": ["sh", "-c", "sleep 3600"],
                "volumeMounts": [{"mountPath": "/mnt/data", "name": "data"}],
            }],
            "volumes": [{"name": "data", "persistentVolumeClaim": {"claimName": "s08-clone-pvc"}}],
        },
    }
    reader_path = save_manifest(TID, "reader", reader_manifest)
    apply_manifest(reader_path, TID, "reader_apply")
    wait_pod_ready(TEST_NS, "s08-reader", TID)

    # Step 6: Verify checkpoint consistency
    reader_out = exec_in_pod(TEST_NS, "s08-reader", "cat /mnt/data/checkpoint.txt", TID, "read_clone", check=False)
    writer_out = exec_in_pod(TEST_NS, "s08-writer", "cat /mnt/data/checkpoint.txt", TID, "read_src", check=False)

    assert reader_out.strip() == writer_out.strip(), "Snapshot not consistent with checkpoint"

    # Step 7: Collect evidence
    run_ssh_cmd("kubectl -n test-cephfs get pvc -o wide", ARTIFACTS_DIR / f"{TID}_pvcs.log")
    run_ssh_cmd("kubectl -n test-cephfs get volumesnapshot -o wide", ARTIFACTS_DIR / f"{TID}_snaps.log")
    run_ssh_cmd("kubectl -n test-cephfs get pods -o wide", ARTIFACTS_DIR / f"{TID}_pods.log")

    # Cleanup
    run_cmd("kubectl -n test-cephfs delete pod s08-writer s08-reader --ignore-not-found", ARTIFACTS_DIR / f"{TID}_cleanup_pods.log")
    run_cmd("kubectl -n test-cephfs delete pvc s08-src-pvc s08-clone-pvc --ignore-not-found", ARTIFACTS_DIR / f"{TID}_cleanup_pvcs.log")
    run_cmd("kubectl -n test-cephfs delete volumesnapshot s08-snap --ignore-not-found", ARTIFACTS_DIR / f"{TID}_cleanup_snap.log")

    print("✅ Snapshot/Restore under load test passed (S-08)")
