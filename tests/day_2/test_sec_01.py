import pytest
from helpers import k, ARTIFACTS_DIR, log_step

TID = "SEC01"


def test_rbac_least_privilege():
    """
    Objective:
        Verify that reduced roles cannot modify cluster-scoped storage.
    """

    readonly_manifest = """
        apiVersion: v1
        kind: ServiceAccount
        metadata:
          name: readonly-sa
          namespace: default
        ---
        apiVersion: rbac.authorization.k8s.io/v1
        kind: Role
        metadata:
          name: readonly-role
          namespace: default
        rules:
          - apiGroups: [""]
            resources: ["pods", "pods/log", "services", "endpoints", "persistentvolumeclaims"]
            verbs: ["get", "list", "watch"]
        ---
        apiVersion: rbac.authorization.k8s.io/v1
        kind: RoleBinding
        metadata:
          name: readonly-binding
          namespace: default
        subjects:
          - kind: ServiceAccount
            name: readonly-sa
            namespace: default
        roleRef:
          kind: Role
          name: readonly-role
          apiGroup: rbac.authorization.k8s.io
        """

    log_step(TID, "Creating read-only ServiceAccount via inline manifest")
    k(f"apply -f - <<EOF\n{readonly_manifest}\nEOF", ARTIFACTS_DIR / f"{TID}_sa.json")

    log_step(TID, "Attempting forbidden PVC create with readonly-sa")
    result = k(
        "auth can-i create pvc --as=system:serviceaccount:default:readonly-sa -n test-cephfs",
        ARTIFACTS_DIR / f"{TID}_pvc_check.json",
    )
    assert "no" in result.lower()
