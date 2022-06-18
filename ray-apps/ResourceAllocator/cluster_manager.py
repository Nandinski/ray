import kubernetes.client
import kubernetes.config
import kubernetes.watch
from kubernetes.client.rest import ApiException
import time
import os
import logging
log = logging.getLogger(__name__)

# FORMAT = "[%(levelname)s] [%(filename)-20s:%(lineno)-s] %(message)s"
# logging.basicConfig(format=FORMAT, level=os.environ.get("LOGLEVEL", "INFO"))


kubernetes.config.load_kube_config()
RAY_CLUSTER_CONFIG = {
    "group": 'cluster.ray.io',
    "version": 'v1',
    "namespace": 'ray',
    "plural": 'rayclusters',
    "name": 'example-cluster'
}
RAY_WORKER_SELECTOR = "ray-node-type=worker"
RUNNING_POD_SELECTOR = "status.phase=Running"

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
    # log.info(f"Obtaining current cluster specification")
    try:
        cluster_spec = api_instance.get_namespaced_custom_object(**RAY_CLUSTER_CONFIG)
        return cluster_spec
    
    except ApiException as e:
        log.exception("Exception when calling CustomObjectsApi->get_namespaced_custom_object: %s\n" % e)
    

def get_cluster_spec():
    # log.info(f"Obtaining current cluster specification (simplified)")
    cluster_spec = get_full_cluster_spec()
    
    worker_resources = cluster_spec["spec"]["podTypes"][1]["podConfig"]["spec"]["containers"][0]["resources"]["requests"]
    simplified_spec = make_cluster_config(cluster_spec["spec"]["maxWorkers"], worker_resources["cpu"], worker_resources["memory"])    
    return simplified_spec

def wait_for_new_spec(new_cluster_specification):
    log.info(f"Waiting for cluster config to be {new_cluster_specification}")
    start_time = time.time()

    num_desired_workers = new_cluster_specification["num_workers"]
    
    prev_number_workers = -1
    while(True):
        list_res = core_v1.list_namespaced_pod(namespace=RAY_CLUSTER_CONFIG["namespace"], label_selector=RAY_WORKER_SELECTOR, field_selector=RUNNING_POD_SELECTOR)
        pods = list_res.items

        nodes_running_w_expected_config = 0
        for pod in pods:
            resources = pod.spec.containers[0].resources.requests
            # Check if node running has the configuration we want
            if resources["cpu"] == new_cluster_specification["cpu_per_worker"] and \
                resources["memory"] == new_cluster_specification["memory_per_worker"]:
                nodes_running_w_expected_config += 1
                # log.info(f"Found pod running: {pod.metadata.name}")
                # log.info(f"Nodes running w/ expected config = {nodes_running_w_expected_config}")
        
        if nodes_running_w_expected_config == num_desired_workers:
            end_time = time.time()
            log.info(f"Waited {end_time-start_time:0.2f} sec")
            return
        else:
            if nodes_running_w_expected_config != prev_number_workers: 
                log.info("Number of current workers is different from expected.")
                log.info(f"Current workers = {nodes_running_w_expected_config}. Desired = {num_desired_workers}")
                prev_number_workers = nodes_running_w_expected_config
            
            time.sleep(1)
    
def get_node_resources_from_kubernetes_resp(rep):
    return rep

def change_cluster_spec(new_cluster_config, sync=True):
    log.debug(f"Changing cluster specification. New configuration: {new_cluster_config} nodes.")

    cluster_spec = get_full_cluster_spec()
    
    assert int(new_cluster_config["num_workers"]) > 0
    assert int(new_cluster_config["cpu_per_worker"]) > 0
    assert int(new_cluster_config["memory_per_worker"][:-2]) > 0

    for pod in cluster_spec["spec"]["podTypes"]:
        if pod["name"] == "rayWorkerType":
            # log.info(f"Current worker node count = {pod['maxWorkers']}")
            cluster_spec["spec"]["maxWorkers"] = pod["maxWorkers"] = pod["minWorkers"] = new_cluster_config["num_workers"]
            
            resources = pod["podConfig"]["spec"]["containers"][0]["resources"]
            # log.info(f"Current worker node configuration = {resources['requests']}")
            for header in ["limits", "requests"]:
                for resourceY, resourceL in [("cpu", "cpu_per_worker"), ("memory", "memory_per_worker")]:
                    resources[header][resourceY] = new_cluster_config[resourceL]            
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

def make_cluster_config(worker_count, cpu_per_worker, memory_per_worker="1Gi"):
    return {"num_workers": int(worker_count), "cpu_per_worker": str(cpu_per_worker), "memory_per_worker": str(memory_per_worker)}

# change_cluster_spec({'num_workers': 2, 'cpu_per_worker': '1', 'memory_per_worker': '1Gi'})