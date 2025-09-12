import pytest
from helpers import ceph, wait_for_health_ok, log_step

TID = "S04"


def test_ceph_mgr_failover():
    """
    Objective:
        Verify Ceph MGR failover works.
    """

    mgr_id = "a"  # TODO: discover dynamically
    log_step(TID, f"Stopping active MGR {mgr_id}")
    ceph(f"orch daemon stop mgr.{mgr_id}")

    log_step(TID, "Waiting for standby to become active")
    wait_for_health_ok(tid=TID)

    log_step(TID, f"Restarting MGR {mgr_id}")
    ceph(f"orch daemon start mgr.{mgr_id}")

    wait_for_health_ok(tid=TID)
