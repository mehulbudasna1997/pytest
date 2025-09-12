import pytest
from helpers import k, wait_for_rollout, ARTIFACTS_DIR, log_step

TID = "SEC03"


def test_secret_rotation_csi_users():
    """
    Objective:
        Key rotation without IO impact.
    """

    secret_manifest = """
        apiVersion: v1
        kind: Secret
        metadata:
          name: csi-cephfs-secret
          namespace: rook-ceph
        stringData:
          adminID: client.csi-cephfs
          adminKey: ROTATED_KEY_CEPHFS
        ---
        apiVersion: v1
        kind: Secret
        metadata:
          name: csi-rbd-secret
          namespace: rook-ceph
        stringData:
          adminID: client.csi-rbd
          adminKey: ROTATED_KEY_RBD
        """

    log_step(TID, "Rotating CSI secrets inline")
    k(f"apply -f - <<EOF\n{secret_manifest}\nEOF", ARTIFACTS_DIR / f"{TID}_rotate.json")

    log_step(TID, "Restarting CSI provisioners")
    for comp in [
        "deploy/csi-cephfsplugin-provisioner",
        "deploy/csi-rbdplugin-provisioner",
    ]:
        k(f"rollout restart {comp} -n rook-ceph")
        wait_for_rollout("rook-ceph", comp, tid=TID)

    pvc_manifest = """
        apiVersion: v1
        kind: PersistentVolumeClaim
        metadata:
          name: sec03-test-pvc
          namespace: test-cephfs
        spec:
          accessModes: ["ReadWriteMany"]
          resources:
            requests:
              storage: 1Gi
          storageClassName: cephfs
        """

    log_step(TID, "Creating PVC with new secrets")
    k(f"apply -f - <<EOF\n{pvc_manifest}\nEOF", ARTIFACTS_DIR / f"{TID}_pvc.json")

    log_step(TID, "Deleting test PVC")
    k(f"delete -f - <<EOF\n{pvc_manifest}\nEOF", ARTIFACTS_DIR / f"{TID}_delete_pvc.json")
