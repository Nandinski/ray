from re import X
import sys
import time
import ray
import requests
import random
import pandas as pd
import argparse

from dataclasses import dataclass, fields

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

@ray.remote(num_cpus=1)
def computeSim():
    x = 0
    for i in range(0, int(1e7)):
        x += i

    refs = []
    for i in range(2):
        ref = computeSim.remote()
        refs.append(ref)
    ray.get(refs)

    return x


def main():
    wait_for_nodes(3)
    refs = [computeSim.remote() for _ in range(100)]
    v = ray.get(refs)
    print(v)
    print("Success!")


if __name__ == "__main__":
    ray.init(f"ray://127.0.0.1:{LOCAL_PORT}")
    main()
    # print(ray.available_resources())
    ray.shutdown()