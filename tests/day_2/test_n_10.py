import pytest
import time
import json
from helpers import (
    k,
    get_pod_names_by_label,
    exec_in_pod,
    ensure_ns,
    delete_ns,
    save_manifest,
    apply_manifest,
    wait_rollout,
    ARTIFACTS_DIR,
    TEST_NS,
)

TID = "n10_cordon_drain"


@pytest.mark.high
def test_n10_cordon_drain_policy():
    """
    N-10 — Cordon/Drain policy

    Objective:
        Confirm operational procedures for node cordon, drain, and uncordon.

    Steps:
        1. Deploy a simple pod to the test namespace.
        2. Cordon the node where it is scheduled.
        3. Verify new pod cannot be scheduled to the cordoned node (remains Pending).
        4. Drain the node and verify the pod reschedules elsewhere.
        5. Uncordon node.

    Expected Result:
        - Cordoned node rejects new pods.
        - Existing pods drain and reschedule cleanly.
        - Node is schedulable again after uncordon.
    """
    # Ensure namespace exists
    ensure_ns(TEST_NS, TID)

    # Step 1: Create a test pod
    pod_manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "n10-tester", "namespace": TEST_NS, "labels": {"app": "n10-tester"}},
        "spec": {
            "containers": [{"name": "pause", "image": "k8s.gcr.io/pause:3.9"}],
        },
    }
    pod_path = save_manifest(TID, "pod", pod_manifest)
    apply_manifest(pod_path, TID, "pod_apply")
    wait_rollout(TEST_NS, "pod/n10-tester", TID, timeout=120)

    pods = get_pod_names_by_label(TEST_NS, "app=n10-tester", TID, "pods")
    assert pods, "No test pod found"
    pod = pods[0]

    # Find node where pod is scheduled
    pod_json = k(f"-n {TEST_NS} get pod {pod} -o json", ARTIFACTS_DIR / f"{TID}_pod.json")
    pod_spec = json.loads(pod_json)
    node_name = pod_spec["spec"]["nodeName"]

    # Step 2: cordon node
    k(f"cordon {node_name}", ARTIFACTS_DIR / f"{TID}_cordon.log")

    # Step 3: Try scheduling a new pod pinned to the same node → should Pending
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
    pod_json2 = k(f"-n {TEST_NS} get pod n10-tester-blocked -o json", ARTIFACTS_DIR / f"{TID}_blocked_pod.json")
    pod_spec2 = json.loads(pod_json2)
    phase = pod_spec2["status"].get("phase", "")
    assert phase in ("Pending", ""), f"Pod scheduled on cordoned node! Phase={phase}"

    # Step 4: drain node (existing pod should reschedule)
    k(f"drain {node_name} --ignore-daemonsets --delete-emptydir-data --force --grace-period=30 --timeout=5m",
      ARTIFACTS_DIR / f"{TID}_drain.log")
    time.sleep(15)

    # Verify reschedule
    pods_after = get_pod_names_by_label(TEST_NS, "app=n10-tester", TID, "pods_after")
    assert pods_after, "Pod did not reschedule after drain"
    assert pods_after[0] != pod, "Pod did not move, still same pod"

    # Step 5: uncordon node
    k(f"uncordon {node_name}", ARTIFACTS_DIR / f"{TID}_uncordon.log")

    # Cleanup
    k(f"-n {TEST_NS} delete pod n10-tester n10-tester-blocked --ignore-not-found", ARTIFACTS_DIR / f"{TID}_cleanup.log")
