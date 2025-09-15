import time
import pytest
from helpers import (
    ARTIFACTS_DIR,
    log_step,
    ceph,
    k,
    wait_for_rollout,
)

TID = "obs_01_scrape_failure"
MGR_POD_LABEL = "app=rook-ceph-mgr"
NAMESPACE = "rook-ceph"   # adjust if your Ceph mgr runs elsewhere


def test_prometheus_scrape_failure():
    """
    OBS-01 — Prometheus scrape failure
    Objective: Ensure missing scrape triggers alert and recovers on restore.
    """

    # 1. Get Ceph MGR pod
    mgr_pods = k(f"-n {NAMESPACE} get pods -l {MGR_POD_LABEL} -o name",
                 ARTIFACTS_DIR / f"{TID}_mgr_pods.log")
    assert mgr_pods.strip(), f"[{TID}] No Ceph MGR pods found"
    mgr_pod = mgr_pods.strip().splitlines()[0]
    log_step(TID, f"Using Ceph MGR pod {mgr_pod}")

    # 2. Disable Prometheus mgr module (simulate scrape failure)
    log_step(TID, "Disabling prometheus mgr module")
    ceph("mgr module disable prometheus", ARTIFACTS_DIR / f"{TID}_disable_prom.log")

    # 3. Wait & check that alert appears
    # NOTE: Replace 'CephMgrPrometheusScrapeError' with your actual alert name
    found_alert = False
    for i in range(24):  # ~2 minutes with 5s interval
        alerts = k("-n monitoring get PrometheusRule -o yaml",
                   ARTIFACTS_DIR / f"{TID}_alerts_iter{i}.yaml",
                   check=False)
        if "CephMgrPrometheusScrapeError" in alerts or "PrometheusScrapeFailed" in alerts:
            log_step(TID, f"Alert fired at iteration {i}")
            found_alert = True
            break
        time.sleep(5)

    assert found_alert, f"[{TID}] Expected scrape failure alert did not fire"

    # 4. Re-enable Prometheus mgr module
    log_step(TID, "Re-enabling prometheus mgr module")
    ceph("mgr module enable prometheus", ARTIFACTS_DIR / f"{TID}_enable_prom.log")

    # 5. Wait & check that alert resolves
    resolved = False
    for i in range(24):  # ~2 minutes with 5s interval
        alerts = k("-n monitoring get PrometheusRule -o yaml",
                   ARTIFACTS_DIR / f"{TID}_alerts_recover{i}.yaml",
                   check=False)
        if "CephMgrPrometheusScrapeError" not in alerts and "PrometheusScrapeFailed" not in alerts:
            log_step(TID, f"Alert resolved at iteration {i}")
            resolved = True
            break
        time.sleep(5)

    assert resolved, f"[{TID}] Expected scrape failure alert did not resolve after recovery"

    # 6. Evidence
    ceph("status", ARTIFACTS_DIR / f"{TID}_final_ceph_status.log")
    k(f"-n {NAMESPACE} describe {mgr_pod}", ARTIFACTS_DIR / f"{TID}_mgr_pod_desc.log")
