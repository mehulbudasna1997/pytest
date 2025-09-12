import pytest
from helpers import ceph, assert_quorum, wait_for_health_ok, log_step

TID = "S03"


def test_ceph_mon_down():
    """
    Objective:
        Verify quorum resilience when a MON goes down.
    """

    mon_id = "a"  # TODO: discover dynamically
    log_step(TID, f"Stopping MON {mon_id}")
    ceph(f"orch daemon stop mon.{mon_id}")

    assert_quorum(min_size=2, tid=TID)

    log_step(TID, f"Restarting MON {mon_id}")
    ceph(f"orch daemon start mon.{mon_id}")

    wait_for_health_ok(tid=TID)
