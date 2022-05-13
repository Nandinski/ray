import kubernetes.client
import kubernetes.config
import kubernetes.watch
from kubernetes.client.rest import ApiException
import time
import os
import logging
log = logging.getLogger(__name__)

kubernetes.config.load_kube_config()
RAY_CLUSTER_CONFIG = {
    "group": 'cluster.ray.io',
    "version": 'v1',
    "namespace": 'ray',
    "plural": 'rayclusters',
    "name": 'example-cluster'
}
RAY_WORKER_SELECTOR = "ray-node-type=worker"

#  Kubernetes clients
with kubernetes.client.ApiClient() as api_client:
    # Create an instance of the API class
    api_instance = kubernetes.client.CustomObjectsApi(api_client)

w = kubernetes.watch.Watch()
core_v1 = kubernetes.client.CoreV1Api()

def validate_cluster_spec(cluster_spec):
    for pod in cluster_spec["spec"]["podTypes"]:
        if pod["name"] == "rayHeadType":
            assert pod["maxWorkers"] == pod["minWorkers"] == 0
        elif pod["name"] == "rayWorkerType":
            assert cluster_spec["spec"]["maxWorkers"] == pod["maxWorkers"] == pod["minWorkers"]
            assert pod["podConfig"]["spec"]["containers"][0]["resources"]["requests"].items() == pod["podConfig"]["spec"]["containers"][0]["resources"]["limits"].items()
        else:
            log.error(f"Unexpected podtype: {pod['name']}")


def get_full_cluster_spec():
    log.info(f"Obtaining current cluster specification")
    try:
        cluster_spec = api_instance.get_namespaced_custom_object(**RAY_CLUSTER_CONFIG)
        return cluster_spec
    
    except ApiException as e:
        log.exception("Exception when calling CustomObjectsApi->get_namespaced_custom_object: %s\n" % e)
    

def get_cluster_spec():
    log.info(f"Obtaining current cluster specification (simplified)")
    cluster_spec = get_full_cluster_spec()
    
    worker_resources = cluster_spec["spec"]["podTypes"][1]["podConfig"]["spec"]["containers"][0]["resources"]["requests"]
    simplified_spec = { "num_workers": cluster_spec["spec"]["maxWorkers"], 
                        "cpu": worker_resources["cpu"],
                        "memory": worker_resources["memory"]}
    
    return simplified_spec

def wait_for_new_spec(new_cluster_specification):
    log.info(f"Waiting for cluster config to be {new_cluster_specification}")
    start_time = time.time()

    num_workers = new_cluster_specification["num_workers"]
    
    timeout = 60 # Max time waiting for pod
    nodes_running = 0
    for event in w.stream(func=core_v1.list_namespaced_pod,
                          namespace=RAY_CLUSTER_CONFIG["namespace"],
                          label_selector=RAY_WORKER_SELECTOR,
                          timeout_seconds=timeout):
        if event["object"].status.phase == "Running":
            resources = event["object"].spec.containers[0].resources.requests
            # Check if node running has the configuration we want
            if resources.items() <= new_cluster_specification.items():
                # log.info(f"Nodes running = {nodes_running}")
                nodes_running += 1

        if nodes_running == num_workers:
            end_time = time.time()
            log.info(f"Waited {end_time-start_time:0.2f} sec")
            return

    log.error(f"Time waiting for node count exceeded timeout of {timeout}s")
    exit(1)
    
def get_node_resources_from_kubernetes_resp(rep):
    return rep

def change_cluster_spec(new_cluster_config, sync=True):
    log.debug(f"Changing cluster specification. New configuration: {new_cluster_config} nodes.")

    cluster_spec = get_full_cluster_spec()
    
    assert int(new_cluster_config["num_workers"]) > 0
    assert int(new_cluster_config["cpu"]) > 0
    assert int(new_cluster_config["memory"][:-2]) > 0

    for pod in cluster_spec["spec"]["podTypes"]:
        if pod["name"] == "rayWorkerType":
            log.info(f"Current worker node count = {pod['maxWorkers']}")
            cluster_spec["spec"]["maxWorkers"] = pod["maxWorkers"] = pod["minWorkers"] = new_cluster_config["num_workers"]
            
            resources = pod["podConfig"]["spec"]["containers"][0]["resources"]
            log.info(f"Current worker node configuration = {resources['requests']}")
            for header in ["limits", "requests"]:
                for resource in ["cpu", "memory"]:
                    resources[header][resource] = new_cluster_config[resource]            
            break

    validate_cluster_spec(cluster_spec)
    
    try:
        cluster_spec = api_instance.patch_namespaced_custom_object(*RAY_CLUSTER_CONFIG.values(), cluster_spec)
        # log.info(f'Updated cluster worker count = {cluster_spec["spec"]["maxWorkers"]}')
        # log.info(f'Updated worker node configuration = {cluster_spec["spec"]["podTypes"][1]["podConfig"]["spec"]["containers"][0]["resources"]["requests"]}')

    except ApiException as e:
        logging.exception("Exception when calling CustomObjectsApi->get_namespaced_custom_object: %s\n" % e)

    if sync: 
        wait_for_new_spec(new_cluster_config)

change_cluster_spec({'num_workers': 2, 'cpu': '2', 'memory': '1Gi'})