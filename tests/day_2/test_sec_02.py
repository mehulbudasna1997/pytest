import pytest
from helpers import k, ARTIFACTS_DIR, log_step

TID = "SEC02"


def test_pod_security_restricted():
    """
    Objective:
        Enforce restricted PodSecurity baseline.
    """

    privileged_pod = """
        apiVersion: v1
        kind: Pod
        metadata:
          name: privileged-pod
          namespace: test-cephfs
        spec:
          containers:
            - name: privileged
              image: busybox:1.36
              command: ["sh", "-c", "sleep 3600"]
              securityContext:
                privileged: true
              volumeMounts:
                - name: host-dev
                  mountPath: /host-dev
          volumes:
            - name: host-dev
              hostPath:
                path: /dev
        """

    log_step(TID, "Trying to deploy privileged pod (should be denied)")
    try:
        k(f"apply -f - <<EOF\n{privileged_pod}\nEOF", ARTIFACTS_DIR / f"{TID}_pod.json")
        admitted = True
    except Exception:
        admitted = False

    assert not admitted, "Privileged pod should be denied by PodSecurity admission"
