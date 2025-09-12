import pytest
from helpers import k, exec_in_pod, get_pod_names_by_label, TEST_NS, log_step

TID = "S07"


def test_pvc_online_expansion_during_load():
    """
    Objective:
        Validate PVC online expansion during active IO.
    """

    pvc_name = "ceph-pvc"
    log_step(TID, f"Expanding PVC {pvc_name} to 5Gi")
    k(
        f"patch pvc {pvc_name} -n {TEST_NS} "
        "-p '{\"spec\": {\"resources\": {\"requests\": {\"storage\": \"5Gi\"}}}}'"
    )

    log_step(TID, "Validating filesystem expansion")
    pods = get_pod_names_by_label(TEST_NS, "app=ceph-writer")
    for pod in pods:
        out = exec_in_pod(TEST_NS, pod, ["sh", "-c", "df -h /data"])
        assert "5" in out, f"{pod} did not show expanded PVC size"
