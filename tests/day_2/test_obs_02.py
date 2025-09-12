import time
import pytest
from helpers import (
    ARTIFACTS_DIR,
    log_step,
    ceph,
    k,
)

TID = "obs_02_alert_routing"
ALERT_NS = "monitoring"   # namespace where Alertmanager + Prometheus run


@pytest.mark.observability
def test_ceph_alert_routing():
    """
    OBS-02 — Ceph alert routing
    Objective: Verify alert flow correctness and routing labels/receivers.
    """

    # 1. Trigger a Ceph alert (simulate OSD down)
    log_step(TID, "Marking OSD.0 down to trigger alert")
    ceph("osd down 0", ARTIFACTS_DIR / f"{TID}_osd_down.log")

    # 2. Wait for alert to appear in Prometheus/Alertmanager
    found_alert = None
    for i in range(30):  # up to ~3 minutes
        alerts = k(f"-n {ALERT_NS} get PrometheusRule -o yaml",
                   ARTIFACTS_DIR / f"{TID}_alerts_iter{i}.yaml",
                   check=False)
        if "CephOSDDown" in alerts or "CephNearFull" in alerts:
            log_step(TID, f"Ceph alert fired (iteration={i})")
            found_alert = alerts
            break
        time.sleep(6)

    assert found_alert, f"[{TID}] Expected Ceph OSD down / nearfull alert did not fire"

    # 3. Validate Alertmanager routing (labels, severity, receivers)
    # NOTE: This assumes Alertmanager API is exposed; adjust query/URL as needed.
    am_alerts = k(f"-n {ALERT_NS} exec svc/alertmanager-main -- curl -s http://localhost:9093/api/v2/alerts",
                  ARTIFACTS_DIR / f"{TID}_am_alerts.json",
                  check=False)

    assert "CephOSDDown" in am_alerts or "CephNearFull" in am_alerts, f"[{TID}] Alert not routed to Alertmanager"

    # Look for severity, receiver, runbook
    assert any(lbl in am_alerts for lbl in ["severity", "receiver"]), f"[{TID}] Missing severity/receiver labels"
    assert "runbook_url" in am_alerts or "runbook" in am_alerts, f"[{TID}] Runbook link missing from alert"

    log_step(TID, "Alert routing validated with severity, receiver, and runbook link")

    # 4. Restore OSD health
    log_step(TID, "Restoring OSD.0 back up")
    ceph("osd up 0", ARTIFACTS_DIR / f"{TID}_osd_up.log")

    # 5. Wait for alert to resolve
    resolved = False
    for i in range(30):
        alerts = k(f"-n {ALERT_NS} exec svc/alertmanager-main -- curl -s http://localhost:9093/api/v2/alerts",
                   ARTIFACTS_DIR / f"{TID}_alerts_resolve_iter{i}.json",
                   check=False)
        if "CephOSDDown" not in alerts and "CephNearFull" not in alerts:
            log_step(TID, f"Ceph alert resolved at iteration {i}")
            resolved = True
            break
        time.sleep(6)

    assert resolved, f"[{TID}] Ceph alert did not resolve after OSD restored"

    # 6. Collect final evidence
    ceph("status", ARTIFACTS_DIR / f"{TID}_final_ceph_status.log")
