import pytest
import time
import json
import subprocess
from pathlib import Path

TID = "n10_cordon_drain"
ARTIFACTS_DIR = Path("artifacts")
ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
TEST_NS = "test-cephfs"


# --- Utility functions ---
def run_cmd(cmd: str, out_file: Path, check=True) -> str:
    """Run shell command and capture output in file."""
    try:
        res = subprocess.run(cmd, shell=True, text=True, capture_output=True, check=check)
        out_file.write_text((res.stdout or "") + (res.stderr or ""))
        return res.stdout
    except subprocess.CalledProcessError as e:
        out_file.write_text((e.stdout or "") + (e.stderr or ""))
        if check:
            pytest.fail(f"Command failed: {cmd}\n{e.stderr}")
        return (e.stdout or "") + (e.stderr or "")


def ensure_ns(namespace: str):
    """Create namespace if it does not exist."""
    run_cmd(f"kubectl get ns {namespace}", ARTIFACTS_DIR / f"{TID}_ns_check.log", check=False)
    run_cmd(f"kubectl create ns {namespace}", ARTIFACTS_DIR / f"{TID}_ns_create.log", check=False)


def save_manifest(tid: str, name: str, manifest: dict) -> Path:
    """Save dict as YAML manifest."""
    import yaml
    path = ARTIFACTS_DIR / f"{tid}_{name}.yaml"
    with path.open("w") as f:
        yaml.dump(manifest, f)
    return path


def apply_manifest(manifest_path: Path, tid: str, tag: str):
    run_cmd(f"kubectl apply -f {manifest_path}", ARTIFACTS_DIR / f"{tid}_{tag}.log")


def wait_rollout(namespace: str, resource: str, tid: str, timeout=120):
    """Wait until a pod or deployment is Ready."""
    start = time.time()
    while time.time() - start < timeout:
        out = run_cmd(f"kubectl -n {namespace} get {resource}", ARTIFACTS_DIR / f"{tid}_wait_rollout.log", check=False)
        if "Running" in out or "1/1" in out:
            return
        time.sleep(5)
    pytest.fail(f"{resource} did not rollout in time")


def get_pod_names_by_label(namespace: str, label: str, tid: str, fname: str) -> list[str]:
    out = run_cmd(f"kubectl -n {namespace} get pods -l {label} -o json",
                  ARTIFACTS_DIR / f"{tid}_{fname}.json")
    items = json.loads(out).get("items", [])
    return [i["metadata"]["name"] for i in items]


# --- Test ---
def test_cordon_drain_policy():
    """N-10: cordon/drain/uncordon test"""
    # Ensure namespace exists
    ensure_ns(TEST_NS)

    # Step 1: Create a test pod
    pod_manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "n10-tester", "namespace": TEST_NS, "labels": {"app": "n10-tester"}},
        "spec": {"containers": [{"name": "pause", "image": "k8s.gcr.io/pause:3.9"}]},
    }
    pod_path = save_manifest(TID, "pod", pod_manifest)
    apply_manifest(pod_path, TID, "pod_apply")
    wait_rollout(TEST_NS, "pod/n10-tester", TID, timeout=120)

    pods = get_pod_names_by_label(TEST_NS, "app=n10-tester", TID, "pods")
    assert pods, "No test pod found"
    pod = pods[0]

    # Find node where pod is scheduled
    pod_json = run_cmd(f"kubectl -n {TEST_NS} get pod {pod} -o json",
                       ARTIFACTS_DIR / f"{TID}_pod.json")
    pod_spec = json.loads(pod_json)
    node_name = pod_spec["spec"]["nodeName"]

    # Step 2: cordon node
    run_cmd(f"kubectl cordon {node_name}", ARTIFACTS_DIR / f"{TID}_cordon.log")

    # Step 3: Create new pod pinned to same node (should stay Pending)
    pod_manifest2 = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "n10-tester-blocked", "namespace": TEST_NS},
        "spec": {
            "containers": [{"name": "pause", "image": "k8s.gcr.io/pause:3.9"}],
            "nodeSelector": {"kubernetes.io/hostname": node_name},
        },
    }
    pod_path2 = save_manifest(TID, "pod_blocked", pod_manifest2)
    apply_manifest(pod_path2, TID, "pod_blocked_apply")
    time.sleep(10)
    pod_json2 = run_cmd(f"kubectl -n {TEST_NS} get pod n10-tester-blocked -o json",
                        ARTIFACTS_DIR / f"{TID}_blocked_pod.json")
    pod_spec2 = json.loads(pod_json2)
    phase = pod_spec2["status"].get("phase", "")
    assert phase in ("Pending", ""), f"Pod scheduled on cordoned node! Phase={phase}"

    # Step 4: drain node (existing pod should reschedule)
    run_cmd(f"kubectl drain {node_name} --ignore-daemonsets --delete-emptydir-data --force --grace-period=30 --timeout=5m",
            ARTIFACTS_DIR / f"{TID}_drain.log")
    time.sleep(15)

    # Verify reschedule
    pods_after = get_pod_names_by_label(TEST_NS, "app=n10-tester", TID, "pods_after")
    assert pods_after, "Pod did not reschedule after drain"
    assert pods_after[0] != pod, "Pod did not move, still same pod"

    # Step 5: uncordon node
    run_cmd(f"kubectl uncordon {node_name}", ARTIFACTS_DIR / f"{TID}_uncordon.log")

    # Cleanup
    run_cmd(f"kubectl -n {TEST_NS} delete pod n10-tester n10-tester-blocked --ignore-not-found",
            ARTIFACTS_DIR / f"{TID}_cleanup.log")
