import pytest
import time
import requests
from kubernetes import client, config

config.load_kube_config()
core_v1 = client.CoreV1Api()
apps_v1 = client.AppsV1Api()

NAMESPACE = "test-net01"

@pytest.fixture(scope="module", autouse=True)
def setup_namespace():
    # Create namespace
    ns = client.V1Namespace(metadata=client.V1ObjectMeta(name=NAMESPACE))
    try:
        core_v1.create_namespace(ns)
    except client.exceptions.ApiException as e:
        if e.status != 409:  # Already exists
            raise
    yield
    core_v1.delete_namespace(NAMESPACE)

def test_service_loadbalancer_disruption():
    # 1. Deploy nginx Deployment
    deployment = client.V1Deployment(
        metadata=client.V1ObjectMeta(name="nginx", namespace=NAMESPACE),
        spec=client.V1DeploymentSpec(
            replicas=1,
            selector=client.V1LabelSelector(match_labels={"app": "nginx"}),
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(labels={"app": "nginx"}),
                spec=client.V1PodSpec(
                    containers=[
                        client.V1Container(
                            name="nginx",
                            image="nginx:1.25",
                            ports=[client.V1ContainerPort(container_port=80)]
                        )
                    ]
                )
            )
        )
    )
    apps_v1.create_namespaced_deployment(namespace=NAMESPACE, body=deployment)

    # 2. Create LoadBalancer Service
    service = client.V1Service(
        metadata=client.V1ObjectMeta(name="nginx-lb", namespace=NAMESPACE),
        spec=client.V1ServiceSpec(
            selector={"app": "nginx"},
            type="LoadBalancer",
            ports=[client.V1ServicePort(port=80, target_port=80)]
        )
    )
    core_v1.create_namespaced_service(namespace=NAMESPACE, body=service)

    # 3. Wait for external IP from MetalLB
    external_ip = None
    for _ in range(30):
        svc = core_v1.read_namespaced_service("nginx-lb", NAMESPACE)
        ingress = svc.status.load_balancer.ingress
        if ingress and ingress[0].ip:
            external_ip = ingress[0].ip
            break
        time.sleep(5)

    assert external_ip, "No external IP assigned by MetalLB"

    # 4. Verify service reachable before disruption
    url = f"http://{external_ip}"
    resp = None
    for _ in range(10):
        try:
            resp = requests.get(url, timeout=3)
            if resp.status_code == 200:
                break
        except requests.exceptions.RequestException:
            pass
        time.sleep(3)
    assert resp and resp.status_code == 200, "Service not reachable before disruption"

    # 5. Disrupt MetalLB speaker (delete one pod)
    pods = core_v1.list_namespaced_pod("metallb-system", label_selector="component=speaker")
    assert pods.items, "No MetalLB speaker pods found"
    disrupted_pod = pods.items[0]
    core_v1.delete_namespaced_pod(disrupted_pod.metadata.name, "metallb-system")

    time.sleep(10)  # allow failover

    # 6. Verify service still reachable
    resp = None
    for _ in range(15):
        try:
            resp = requests.get(url, timeout=3)
            if resp.status_code == 200:
                break
        except requests.exceptions.RequestException:
            pass
        time.sleep(5)
    assert resp and resp.status_code == 200, "Service not reachable after disruption"

    # ✅ Expected: traffic fails over
