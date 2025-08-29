from kubernetes import client


def test_deployments_have_desired_available_replicas(
    kube_clients: tuple[client.CoreV1Api, client.AppsV1Api]
):
    _, apps_v1 = kube_clients
    deployments = apps_v1.list_deployment_for_all_namespaces().items

    not_fully_available = []
    for dep in deployments:
        desired = dep.spec.replicas or 0
        available = dep.status.available_replicas or 0
        if available != desired:
            not_fully_available.append(
                f"{dep.metadata.namespace}/{dep.metadata.name} "
                f"(desired={desired}, available={available})"
            )

    assert not not_fully_available, (
        "Deployments without desired available replicas: "
        + "; ".join(not_fully_available)
    )


