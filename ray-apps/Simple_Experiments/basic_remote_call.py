from pickletools import optimize
from re import X
import sys
import time
import ray

sys.path.append('/home/nando/PhD/Ray/ray/ray-apps')
from ResourceAllocator.resource_allocator import RManager, resourceWrapper, resourceWrapperStress

""" Run this script locally to execute a Ray program on your Ray cluster on
Kubernetes.

Before running this script, you must port-forward from the local host to
the relevant Kubernetes head service e.g.
kubectl -n ray port-forward service/example-cluster-ray-head 10001:10001.

Set the constant LOCAL_PORT below to the local port being forwarded.
"""
LOCAL_PORT = 10001

def wait_for_nodes(expected):
    # Wait for all nodes to join the cluster.
    while True:
        resources = ray.cluster_resources()
        node_keys = [key for key in resources if "node" in key]
        num_nodes = sum(resources[node_key] for node_key in node_keys)
        if num_nodes < expected:
            print("{} nodes have joined so far, waiting for {} more.".format(
                num_nodes, expected - num_nodes))
            sys.stdout.flush()
            time.sleep(1)
        else:
            break

@ray.remote
def testFunc():
    return 0

@resourceWrapperStress()
def testFunc2():
    return 0

def main():
    refs = testFunc.remote()
    refs = testFunc2.remote()
    v = ray.get(refs)
    print("Success!")

if __name__ == "__main__":
    runtime_env = {"py_modules": ["../ResourceAllocator"]}
    ray.init(f"ray://127.0.0.1:{LOCAL_PORT}", runtime_env=runtime_env)

    # RManager.optimize(lambda: main(), exploration_strategy="GridSearch", per_func_pareto=True, configs_to_test=1)
    RManager.optimize(lambda: main(), exploration_strategy="OPTUNASearch", per_func_pareto=True, configs_to_test=1, timeout=None)

    ray.shutdown()