import pytest
from helpers import k, wait_for_rollout, log_step

TID = "S06"


def test_ceph_csi_restart():
    """
    Objective:
        Validate CSI resilience under restart.
    """

    components = [
        "deploy/csi-cephfsplugin-provisioner",
        "deploy/csi-rbdplugin-provisioner",
        "ds/csi-cephfsplugin",
        "ds/csi-rbdplugin",
    ]
    for comp in components:
        log_step(TID, f"Restarting {comp}")
        k(f"rollout restart {comp} -n rook-ceph")
        wait_for_rollout("rook-ceph", comp, tid=TID)
