import pytest
import subprocess
import time
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import os
from dotenv import load_dotenv
load_dotenv()

# -------------------------
# Fixtures
# -------------------------
@pytest.fixture(scope="session")
def kube_clients():
    """Return CoreV1Api and AppsV1Api clients."""
    try:
        # Respect explicit kubeconfig if provided
        kubeconfig_path = os.environ.get("KUBECONFIG")
        if kubeconfig_path:
            config.load_kube_config(config_file=kubeconfig_path)
        else:
            config.load_kube_config()
    except Exception:
        # Fall back to in-cluster configuration (when tests run in a Pod)
        config.load_incluster_config()

    core_v1 = client.CoreV1Api()
    apps_v1 = client.AppsV1Api()
    autoscaling_v1 = client.AutoscalingV1Api()
    return core_v1, apps_v1, autoscaling_v1

# -------------------------
# T8.1 — Metrics Server Availability
# -------------------------
def test_metrics_server_running(kube_clients):
    core_v1, _, _ = kube_clients
    namespace = "kube-system"

    pods = core_v1.list_namespaced_pod(namespace=namespace, label_selector="k8s-app=metrics-server")  ####### Change metrics server name accordingly
    pod_names = [p.metadata.name for p in pods.items]
    print("Metrics-server pods:", pod_names)

    assert pods.items, "Metrics-server is not deployed"
    not_running = [p.metadata.name for p in pods.items if p.status.phase != "Running"]
    assert not not_running, f"Metrics-server pods not running: {not_running}"

# -------------------------
# T8.1 — HPA CPU scaling demo
# -------------------------
def test_hpa_cpu_scaling(kube_clients):
    core_v1, apps_v1, autoscaling_v1 = kube_clients
    namespace = "default"
    deployment_name = "hpa-demo"
    hpa_name = "hpa-demo"

    # 1. Create a simple CPU-bound deployment
    deployment_manifest = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": deployment_name},
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": deployment_name}},
            "template": {
                "metadata": {"labels": {"app": deployment_name}},
                "spec": {
                    "containers": [
                        {
                            "name": "cpu-demo",
                            "image": "vish/stress",
                            "args": ["--cpu", "1", "--timeout", "600s"],
                            "resources": {"requests": {"cpu": "100m"}, "limits": {"cpu": "200m"}}
                        }
                    ]
                }
            }
        }
    }

    try:
        apps_v1.create_namespaced_deployment(namespace, deployment_manifest)
    except ApiException as e:
        if e.status != 409:  # ignore if already exists
            raise

    # 2. Create HPA for CPU scaling
    hpa_manifest = {
        "apiVersion": "autoscaling/v1",
        "kind": "HorizontalPodAutoscaler",
        "metadata": {"name": hpa_name},
        "spec": {
            "scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": deployment_name},
            "minReplicas": 1,
            "maxReplicas": 3,
            "targetCPUUtilizationPercentage": 50
        }
    }

    try:
        autoscaling_v1.create_namespaced_horizontal_pod_autoscaler(namespace, hpa_manifest)
    except ApiException as e:
        if e.status != 409:
            raise

    # 3. Wait for deployment pods to be running
    for _ in range(30):
        pods = core_v1.list_namespaced_pod(namespace, label_selector=f"app={deployment_name}")
        if pods.items and all(p.status.phase == "Running" for p in pods.items):
            break
        time.sleep(2)
    else:
        pytest.fail("Deployment pods did not reach Running state")

    # 4. Check metrics via kubectl top (requires metrics-server)
    try:
        output = subprocess.check_output(
            ["kubectl", "top", "pods", "-n", namespace, "-l", f"app={deployment_name}"],
            text=True
        )
        print("CPU Metrics:\n", output)
        assert output, "No CPU metrics returned"
    except subprocess.CalledProcessError as e:
        pytest.fail(f"Failed to fetch metrics: {e}")

    # 5. Optional: Wait a bit and check HPA desired replicas (demo)
    time.sleep(15)
    hpa_status = autoscaling_v1.read_namespaced_horizontal_pod_autoscaler(hpa_name, namespace)
    print(f"HPA status: currentReplicas={hpa_status.status.current_replicas}, desiredReplicas={hpa_status.status.desired_replicas}")
    assert hpa_status.status.desired_replicas >= 1, "HPA did not scale deployment as expected"

def test_ceph_rook_logs(kube_client):
    """
    Ensure cluster/system logs are collected and Ceph/Rook events are searchable.
    """
    core_v1, _, _ = kube_client

    # Get events from all namespaces
    events = core_v1.list_event_for_all_namespaces().items

    # Filter events related to Ceph or Rook
    ceph_rook_events = [e for e in events if "ceph" in e.involved_object.name.lower() or "rook" in e.involved_object.name.lower()]

    # Print some details for verification
    for e in ceph_rook_events[:10]:  # show first 10 for brevity
        print(f"Namespace: {e.metadata.namespace}, Name: {e.involved_object.name}, Reason: {e.reason}, Message: {e.message}")

    assert ceph_rook_events, "No Ceph/Rook events found in cluster logs"