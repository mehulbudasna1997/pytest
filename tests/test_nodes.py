from kubernetes import client


def test_all_nodes_ready(kube_clients: tuple[client.CoreV1Api, client.AppsV1Api]):
    core_v1, _ = kube_clients
    nodes = core_v1.list_node().items

    not_ready = []
    for node in nodes:
        conditions = {c.type: c.status for c in (node.status.conditions or [])}
        is_ready = conditions.get("Ready") == "True"
        if not is_ready:
            not_ready.append(node.metadata.name)

    assert not not_ready, (
        "Expected all nodes to be Ready, but these are not: " + ", ".join(not_ready)
    )


