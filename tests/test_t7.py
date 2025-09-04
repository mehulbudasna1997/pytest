import subprocess
import pytest
from kubernetes import client, config



@pytest.fixture(scope="session")
def kube_client():
    """
    Load kubeconfig once per session and provide core, apps, autoscaling clients.
    """
    config.load_kube_config()
    core_v1 = client.CoreV1Api()
    apps_v1 = client.AppsV1Api()
    autoscaling_v1 = client.AutoscalingV1Api()
    return core_v1, apps_v1, autoscaling_v1



@pytest.mark.high
def test_t7_1_rbac_least_privilege(kube_client):
    """
    Purpose:
        Validate that Rook/CSI ServiceAccounts follow least-privilege RBAC.
    Preconditions:
        - Rook/Ceph CSI installed in rook-ceph namespace
    Steps:
        1. List ClusterRoleBindings bound to rook-ceph
        2. Attempt a prohibited action (delete pod) with a read-only ServiceAccount
    Expected Result:
        - Excess privileges are not present
        - Prohibited action is denied
    """
    roles = subprocess.check_output(
        ["kubectl", "-n", "rook-ceph", "get", "clusterrolebindings", "-o", "wide"],
        text=True
    )
    print("ClusterRoleBindings for rook-ceph:\n", roles)

    try:
        subprocess.run(
            ["kubectl", "auth", "can-i", "delete", "pods",
             "--as=system:serviceaccount:rook-ceph:rook-ceph-csi"],
            check=True, text=True, capture_output=True
        )
        result = True
    except subprocess.CalledProcessError as e:
        print("Access denied as expected:", e.stderr)
        result = False

    assert result is False, "Read-only SA unexpectedly has delete pod privilege"



@pytest.mark.high
def test_t7_2_secrets_at_rest(kube_client):
    """
    Purpose:
        Validate Kubernetes secrets are encrypted at rest.
    Steps:
        1. Fetch kube-apiserver manifest
        2. Verify --encryption-provider-config flag is set
    Expected Result:
        - Encryption at rest enabled (AES-CBC/KMS)
        - Secrets not readable as plain text on disk
    """
    manifest = subprocess.check_output(
        ["kubectl", "-n", "kube-system", "get", "pod",
         "-l", "component=kube-apiserver", "-o", "yaml"],
        text=True
    )
    print("kube-apiserver manifest:\n", manifest[:500])
    assert "--encryption-provider-config" in manifest, "Encryption provider config not set"


@pytest.mark.medium
def test_t7_3_network_policies(kube_client):
    """
    Validate Network Policies enforce least-privilege communication.
    """
    ns = "test-netpol"

    # Ensure namespace exists
    subprocess.run(["kubectl", "create", "ns", ns], check=False)

    # Ensure pod exists
    try:
        pod_name = subprocess.check_output(
            ["kubectl", "-n", ns, "get", "pod", "-l", "app=test-app",
             "-o", "jsonpath={.items[0].metadata.name}"],
            text=True
        )
    except subprocess.CalledProcessError:
        subprocess.run(
            ["kubectl", "run", "test-app", "--image=busybox", "--restart=Never",
             "-n", ns, "--labels=app=test-app", "--", "sleep", "3600"],
            check=True
        )
        pod_name = "test-app"

    # Step 1: Apply deny-all
    deny_all = """
    apiVersion: networking.k8s.io/v1
    kind: NetworkPolicy
    metadata:
      name: deny-all
      namespace: test-netpol
    spec:
      podSelector: {}
      policyTypes:
      - Ingress
      - Egress
    """
    subprocess.run(["kubectl", "apply", "-f", "-"],
                   input=deny_all, text=True, check=True)

    # Step 2: Attempt external ping
    reachable = True
    try:
        subprocess.run(
            ["kubectl", "-n", ns, "exec", pod_name, "--",
             "ping", "-c", "1", "8.8.8.8"],
            check=True, capture_output=True, text=True, timeout=10
        )
    except subprocess.CalledProcessError:
        reachable = False

    assert reachable is False, "Pod unexpectedly reached external address despite deny-all policy"

    # Step 3: Allow CoreDNS egress
    allow_coredns = """
    apiVersion: networking.k8s.io/v1
    kind: NetworkPolicy
    metadata:
      name: allow-dns
      namespace: test-netpol
    spec:
      podSelector:
        matchLabels:
          app: test-app
      policyTypes:
      - Egress
      egress:
      - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: kube-system
        ports:
        - protocol: UDP
          port: 53
    """
    subprocess.run(["kubectl", "apply", "-f", "-"],
                   input=allow_coredns, text=True, check=True)

    # Step 4: Allow storage access (auto-detect Ceph service)
    ceph_services = subprocess.check_output(
        ["kubectl", "-n", "rook-ceph", "get", "svc", "-o", "json"],
        text=True
    )
    svc_json = json.loads(ceph_services)
    ceph_service = None
    for svc in svc_json["items"]:
        if "mon" in svc["metadata"]["name"]:  # pick monitor service
            ceph_service = f"{svc['metadata']['name']}.{svc['metadata']['namespace']}:{svc['spec']['ports'][0]['port']}"
            break

    if not ceph_service:
        pytest.skip("No Ceph monitor service found in rook-ceph namespace")

    allow_storage = f"""
    apiVersion: networking.k8s.io/v1
    kind: NetworkPolicy
    metadata:
      name: allow-storage
      namespace: test-netpol
    spec:
      podSelector:
        matchLabels:
          app: test-app
      policyTypes:
      - Egress
      egress:
      - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: rook-ceph
    """
    subprocess.run(["kubectl", "apply", "-f", "-"],
                   input=allow_storage, text=True, check=True)

    # Step 5: Verify DNS works
    subprocess.run(
        ["kubectl", "-n", ns, "exec", pod_name, "--",
         "nslookup", "kubernetes.default.svc.cluster.local"],
        check=True, capture_output=True, text=True, timeout=10
    )

    # Step 6: Verify Ceph monitor is reachable
    subprocess.run(
        ["kubectl", "-n", ns, "exec", pod_name, "--",
         "nc", "-zv"] + ceph_service.split(":"),
        check=True, capture_output=True, text=True, timeout=10
    )
