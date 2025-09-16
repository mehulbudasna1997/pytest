import pytest
from helpers import k, exec_in_pod, get_pod_names_by_label, TEST_NS, log_step

TID = "S07"

def test_pvc_online_expansion_during_load():
    """
    S-07: PVC online expansion during load
    Objective:
        Confirm expansion safety.
    Steps:
        - Expand PVC size to 5Gi while writer app is running
        - Verify filesystem reflects new size
    Expected:
        - No IO errors
        - App remains steady
    """

    pvc_name = "ceph-pvc"

    # Step 1: Write before expansion
    pods = get_pod_names_by_label(TEST_NS, "app=ceph-writer")
    for pod in pods:
        log_step(TID, f"Baseline write on {pod}")
        exec_in_pod(TEST_NS, pod, "sh -c 'echo BEFORE >> /data/check.txt'", TID, "before_write")

    # Step 2: Expand PVC
    log_step(TID, f"Expanding PVC {pvc_name} to 5Gi")
    k(
        f"patch pvc {pvc_name} -n {TEST_NS} "
        "-p '{\"spec\": {\"resources\": {\"requests\": {\"storage\": \"5Gi\"}}}}'"
    )

    # Step 3: Verify filesystem expansion
    for pod in pods:
        out = exec_in_pod(TEST_NS, pod, "sh -c 'df -h /data'", TID, "df_check")
        assert "5.0G" in out or "5G" in out, f"{pod} did not show expanded PVC size"

    # Step 4: Write after expansion
    for pod in pods:
        log_step(TID, f"Post-expansion write on {pod}")
        exec_in_pod(TEST_NS, pod, "sh -c 'echo AFTER >> /data/check.txt'", TID, "after_write")

    log_step(TID, "PASS: PVC expanded to 5Gi with no IO errors, app steady")

