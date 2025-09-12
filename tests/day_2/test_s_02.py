import pytest
from helpers import ceph, wait_for_health_ok, wait_for_health_warn, log_step

TID = "S02"


def test_ceph_osd_down():
    """
    Objective:
        Data availability during single OSD loss.
    """

    osd_id = "0"  # TODO: discover dynamically
    log_step(TID, f"Stopping OSD {osd_id}")
    ceph(f"orch daemon stop osd.{osd_id}")

    wait_for_health_warn(tid=TID)

    log_step(TID, f"Restarting OSD {osd_id}")
    ceph(f"orch daemon start osd.{osd_id}")

    wait_for_health_ok(tid=TID)
