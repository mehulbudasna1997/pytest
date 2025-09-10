import time
import pytest
from helpers import (
    k,
    ceph,
    get_pod_names_by_label,
    exec_in_pod,
    placeholder,
    ARTIFACTS_DIR,
    TEST_NS
)

TID = "N07"


def test_image_pull_failure():
    # 1️⃣ Prepare: define a test pod manifest (example)
    pod_manifest = ARTIFACTS_DIR / f"{TID}_test_pod.yaml"
    placeholder(TID, "pod_manifest",
                f"Create a pod YAML with an image that will fail to pull initially: {pod_manifest}")

    # 2️⃣ Block registry egress (simulate network restriction)
    placeholder(TID, "block_registry", "Apply NetworkPolicy or iptables rule to block registry access for namespace")
    print("⚠️ Registry access blocked. Attempting to deploy pod...")

    # 3️⃣ Deploy the pod
    placeholder(TID, "deploy_pod_blocked", f"kubectl apply -f {pod_manifest}")

    # 4️⃣ Wait for ImagePullBackOff status (max 2 min)
    backoff_observed = False
    start = time.time()
    while time.time() - start < 120:
        out = k(f"get pods -n {TEST_NS} -o json", ARTIFACTS_DIR / f"{TID}_pods.json")
        import json
        data = json.loads(out)
        for p in data.get("items", []):
            if "ImagePullBackOff" in [cs.get("state", {}).get("waiting", {}).get("reason", "") for cs in
                                      p["status"].get("containerStatuses", [])]:
                backoff_observed = True
                failed_pod_name = p["metadata"]["name"]
                break
        if backoff_observed:
            break
        time.sleep(5)
    assert backoff_observed, "Pod did not enter ImagePullBackOff"

    print(f"⚠️ Pod {failed_pod_name} is in ImagePullBackOff as expected.")

    # 5️⃣ Restore registry access
    placeholder(TID, "restore_registry", "Remove NetworkPolicy or iptables rule to allow registry access")
    print("✅ Registry access restored. Waiting for pod to pull image...")

    # 6️⃣ Wait for pod to become Ready (max 2 min)
    ready = False
    start = time.time()
    while time.time() - start < 120:
        out = k(f"get pod {failed_pod_name} -n {TEST_NS} -o json", ARTIFACTS_DIR / f"{TID}_pod_status.json")
        data = json.loads(out)
        conditions = data["status"].get("conditions", [])
        for cond in conditions:
            if cond["type"] == "Ready" and cond["status"] == "True":
                ready = True
                break
        if ready:
            break
        time.sleep(5)
    assert ready, f"Pod {failed_pod_name} did not become Ready after restoring registry access"

    print(f"✅ Pod {failed_pod_name} successfully pulled image and is Ready. Image pull failure test passed!")
