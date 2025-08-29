from kubernetes import client


REQUIRED_NAMESPACES = {"default", "kube-system"}


def test_required_namespaces_exist(
    kube_clients: tuple[client.CoreV1Api, client.AppsV1Api]
):
    core_v1, _ = kube_clients
    namespaces = {ns.metadata.name for ns in core_v1.list_namespace().items}

    missing = REQUIRED_NAMESPACES - namespaces
    assert not missing, (
        f"Missing required namespaces: {', '.join(sorted(missing))}. "
        f"Existing: {', '.join(sorted(namespaces))}"
    )


