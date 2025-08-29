from kubernetes import client


ALLOWED_PHASES = {"Running", "Succeeded"}


def test_all_pods_in_allowed_phase(
    kube_clients: tuple[client.CoreV1Api, client.AppsV1Api]
):
    core_v1, _ = kube_clients
    pods = core_v1.list_pod_for_all_namespaces().items

    bad = []
    for pod in pods:
        phase = (pod.status.phase or "").strip()
        if phase not in ALLOWED_PHASES:
            bad.append(
                f"{pod.metadata.namespace}/{pod.metadata.name} (phase={phase})"
            )

    assert not bad, (
        "Found pods not in allowed phases (Running, Succeeded): "
        + "; ".join(bad)
    )


