import pytest
from helpers import k, wait_for_rollout, log_step

TID = "S05"


def test_rook_operator_restart():
    """
    Objective:
        Rook operator restart resilience.
    """

    log_step(TID, "Restarting rook-ceph-operator")
    k("rollout restart deploy/rook-ceph-operator -n rook-ceph")

    wait_for_rollout("rook-ceph", "deploy/rook-ceph-operator", tid=TID)
