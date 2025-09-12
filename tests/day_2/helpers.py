import os
import time
import json
import yaml
import pytest
import subprocess
from pathlib import Path


ARTIFACTS_DIR = Path(os.environ.get("ARTIFACTS_DIR", "artifacts"))
MANIFESTS_DIR = ARTIFACTS_DIR / "manifests"
SCREENSHOTS_DIR = ARTIFACTS_DIR / "screenshots"

TEST_NS = os.environ.get("TEST_NS", "test-cephfs")
CEPHFS_SC = os.environ.get("CEPHFS_SC", "cephfs")
SNAPSHOT_CLASS = os.environ.get("SNAPSHOT_CLASS")
CEPH_ALLOW_ORCH = os.environ.get("CEPH_ALLOW_ORCH", "false").lower() in ("1", "true", "yes")

for d in (ARTIFACTS_DIR, MANIFESTS_DIR, SCREENSHOTS_DIR):
    d.mkdir(parents=True, exist_ok=True)

# Utility helpers
def _write(path: Path, data: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(data)

def run_cmd(cmd: str, out: Path, check: bool = True) -> str:
    """Run a shell command and capture stdout+stderr to file."""
    try:
        res = subprocess.run(cmd, shell=True, text=True, capture_output=True, check=check)
        _write(out, res.stdout + (("\n--- STDERR ---\n" + res.stderr) if res.stderr else ""))
        return res.stdout
    except subprocess.CalledProcessError as e:
        _write(out, (e.stdout or "") + "\n--- STDERR ---\n" + (e.stderr or ""))
        if check:
            pytest.fail(f"Command failed ({cmd}): {e.stderr}")
        return (e.stdout or "") + (e.stderr or "")

def k(cmd: str, out: Path, check: bool = True) -> str:
    return run_cmd(f"kubectl {cmd}", out, check=check)

def ceph(cmd: str, out: Path, check: bool = True) -> str:
    return run_cmd(f"ceph {cmd}", out, check=check)

def save_manifest(tid: str, name: str, manifest: dict) -> Path:
    path = MANIFESTS_DIR / f"{tid}_{name}.yaml"
    _write(path, yaml.safe_dump(manifest, sort_keys=False))
    return path

def placeholder(tid: str, name: str, text: str) -> Path:
    path = SCREENSHOTS_DIR / f"{tid}_{name}.txt"
    _write(path, f"{tid} :: {name}\n{text}\n")
    return path

def wait_rollout(namespace: str, kind_name: str, tid: str, timeout: int = 240):
    k(f"-n {namespace} rollout status {kind_name} --timeout={timeout}s",
      ARTIFACTS_DIR / f"{tid}_rollout_status.log")

def jsonpath(cmd: str, tid: str, fname: str) -> str:
    out = k(f"{cmd} -o json", ARTIFACTS_DIR / f"{tid}_{fname}.json")
    return out

def get_pod_names_by_label(namespace: str, label: str, tid: str, fname: str) -> list[str]:
    out = json.loads(jsonpath(f"-n {namespace} get pods -l {label}", tid, fname) or "{}")
    items = out.get("items", [])
    return [i["metadata"]["name"] for i in items]

def exec_in_pod(namespace: str, pod: str, command: str, tid: str, tag: str, check: bool = True) -> str:
    return k(f"-n {namespace} exec {pod} -- {command}", ARTIFACTS_DIR / f"{tid}_{tag}.log", check=check)

def apply_manifest(path: Path, tid: str, tag: str):
    k(f"apply -f {path}", ARTIFACTS_DIR / f"{tid}_{tag}.log")

def delete_ns(namespace: str, tid: str):
    k(f"delete ns {namespace} --ignore-not-found=true", ARTIFACTS_DIR / f"{tid}_ns_delete.log", check=False)

def ensure_ns(namespace: str, tid: str):
    k(f"create ns {namespace} || true", ARTIFACTS_DIR / f"{tid}_ns_create.log", check=False)

def log_step(tid: str, msg: str):
    """Log step progress to console + artifact file."""
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{tid}] {ts} {msg}"
    print(line)
    with open(ARTIFACTS_DIR / f"{tid}_log.txt", "a") as f:
        f.write(line + "\n")


def wait_for_health_ok(timeout=300, interval=10, tid="GENERIC"):
    """Wait until Ceph cluster reports HEALTH_OK."""
    start = time.time()
    while time.time() - start < timeout:
        status = ceph("status", ARTIFACTS_DIR / f"{tid}_wait_ok.json")
        if "HEALTH_OK" in status:
            log_step(tid, "Cluster is HEALTH_OK")
            return True
        time.sleep(interval)
    raise AssertionError(f"[{tid}] Cluster did not reach HEALTH_OK within {timeout}s")


def wait_for_health_warn(timeout=120, interval=5, tid="GENERIC"):
    """Wait until Ceph cluster reports HEALTH_WARN."""
    start = time.time()
    while time.time() - start < timeout:
        status = ceph("status", ARTIFACTS_DIR / f"{tid}_wait_warn.json")
        if "HEALTH_WARN" in status:
            log_step(tid, "Cluster entered HEALTH_WARN")
            return True
        time.sleep(interval)
    raise AssertionError(f"[{tid}] Cluster did not enter HEALTH_WARN within {timeout}s")


def assert_quorum(min_size=2, tid="GENERIC"):
    """Assert Ceph quorum has at least `min_size` members."""
    status = ceph("quorum_status", ARTIFACTS_DIR / f"{tid}_quorum.json")
    import json
    try:
        data = json.loads(status)
        quorum = data.get("quorum", [])
        assert len(quorum) >= min_size, f"[{tid}] Quorum size {len(quorum)} < {min_size}"
        log_step(tid, f"Quorum has {len(quorum)} members (>= {min_size})")
    except Exception:
        raise AssertionError(f"[{tid}] Failed to parse quorum_status")


def wait_for_rollout(ns: str, deploy: str, timeout=300, tid="GENERIC"):
    """Wait for a deployment rollout to complete with retries."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            k(f"rollout status {deploy} -n {ns}", ARTIFACTS_DIR / f"{tid}_{deploy}_rollout.json")
            log_step(tid, f"Rollout {deploy} completed in ns={ns}")
            return True
        except Exception:
            time.sleep(5)
    raise AssertionError(f"[{tid}] Rollout {deploy} did not complete in {timeout}s")
