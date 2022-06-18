from re import X
import sys
import time
import ray

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
def testFunc(i):
    return i

def main():
    ray.init(f"ray://127.0.0.1:{LOCAL_PORT}")
    call_count = 10

    time_taken_list = []
    for i in range(call_count):
        start = time.time()
        ray.get(testFunc.remote(i))
        time_taken = round(time.time() - start, 4)
        time_taken_list.append(time_taken)
    print(f"Time taken list = {time_taken_list}")
    print(f"Average time per remote call = {sum(time_taken_list)/call_count:.2f}s")
    ray.shutdown()

if __name__ == "__main__":
    main()