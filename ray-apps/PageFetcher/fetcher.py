from collections import defaultdict
from re import X
import sys
import time
import ray
import requests
import random
import pandas as pd
import argparse
import os

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

@dataclass
class URLMetrics:
    url: str
    timeGettingFile: float
    timeComputing: float

@dataclass
class HeaderMetrics:
    nCPUs: str
    expN: int

@ray.remote
class MetricStore:
    def __init__(self, headerAttrs, metricDCAttrs):
        self.headerAttrs = headerAttrs
        self.metricDCAttrs = metricDCAttrs
        self.metricList = []

    def storeMetric(self, header, metric):
        metricAttr = [getattr(metric,attr) for attr in self.metricDCAttrs]
        headerAttr = [getattr(header,attr) for attr in self.headerAttrs]
        self.metricList.append(headerAttr + metricAttr)
    
    def getMetricsByID(self, id):
        return self.store[id]

    def matchMetricCount(self, count):
        return len(self.metricList) == count

    def getDF(self):
        df = pd.DataFrame(self.metricList, columns=[*self.headerAttrs, *self.metricDCAttrs])
        return df

def getRandomFileURL():
    nFiles = 5
    randomFileID = random. randint(1, nFiles)
    return getFileURL(randomFileID)

def getFileURL(i):
    CONTENT_SERVER_ADDR = "content-server-service"
    return f"http://{CONTENT_SERVER_ADDR}:9090/files/file{i}.dat"

def fetchURL(url):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException as err:
        print(f"Error getting URL {url}: err: {err}\n")
        exit(1)
    
    if response.status_code == 404 :
        print(f"File {url} not found.\n", )
        exit(1)
		# When this happens, enqueue the goroutine again?

def computeSim():
    x = 0
    for i in range(0, int(5e5)):
        x += i
    return x

@ray.remote(num_cpus=1)
def processURL(url, headerMetrics, mStore):
    # print("Fetching ", url)
    
    start = time.time()
    # fetchURL(url)
    # simulate fetching with sleep so that we don't bottleneck on network
    # Sleep for 40ms as this is the average time fetching files
    time.sleep(40/1000)
    timeGettingFile = time.time() - start 
    # print(f"Time getting file: {timeGettingFile}")

    # Simulate computation
    start = time.time()
    computeSim()
    timeComputing = time.time() - start 
    # print(f"Time computing: {timeComputing}")

    mStore.storeMetric.remote(headerMetrics, URLMetrics(url, timeGettingFile, timeComputing))
    return "Success"

@ray.remote
def remote_experiment(nr_urls, nCPU, curr_exp, mStore):
    start = time.time()
    refs = [processURL.options(num_cpus=nCPU).remote(getFileURL((i % 5)+1), HeaderMetrics(nCPU, curr_exp), mStore) for i in range(nr_urls)]
    pageResults = ray.get(refs)
    execTime = time.time() - start
    return execTime

def run_experiment(expName, exp_count, nr_urls, CPUs_to_exp):
    metricAttrs = [field.name for field in fields(URLMetrics)]
    headerAttrs = [field.name for field in fields(HeaderMetrics)]
    
    mStore = MetricStore.remote(headerAttrs, metricAttrs)
    execTimeList = []
    
    for nCPU in CPUs_to_exp:
        for curr_exp in range(exp_count):
            print(f"Running experiment with {nCPU=} and {curr_exp=}")
            execTime = ray.get(remote_experiment.remote(nr_urls, nCPU, curr_exp, mStore))
            print(f"{execTime=}")
            execTimeList.append([nCPU, curr_exp, execTime])

    # Wait until all metrics have been collected
    while True:
        if ray.get(mStore.matchMetricCount.remote(exp_count*nr_urls*len(CPUs_to_exp))):
            break
        else:
            print("Not all metrics present. Waiting.")
            time.sleep(0.1)

    parentDir = os.path.join("Metrics", expName)
    os.makedirs(parentDir, exist_ok=True)
    metricDF = ray.get(mStore.getDF.remote())
    metricDF.to_csv(os.path.join(parentDir,'collectedMetrics.csv'), index=False)
    execDF = pd.DataFrame(execTimeList, columns=["nCPUs", "expN", "Execution Time"])
    execDF.to_csv(os.path.join(parentDir, 'execTime.csv'), index=False)


def main():
    parser = argparse.ArgumentParser(description='Webcrawler on Ray')
    parser.add_argument('--exp_name', type=str, help='Name of experiment to prepend to output files', required=True)
    parser.add_argument('--exp_count', type=int, help='Name of times to repeat experiment', choices=range(1, int(1e2)), default=int(100))
    parser.add_argument('--nr_urls', type=int, help='Nr of urls to fetch', choices=range(1, int(1e6)), default=int(1e2))
    parser.add_argument('--CPUs_to_exp', nargs="+", help='Cpus to experiment with',  default=[1, 0.5])

    args = parser.parse_args()
    exp_name = args.exp_name
    exp_count = args.exp_count
    nr_urls = args.nr_urls
    CPUs_to_exp = args.CPUs_to_exp

    wait_for_nodes(3)
    run_experiment(exp_name, exp_count, nr_urls, CPUs_to_exp)
    sys.stdout.flush()
    print("Page fetcher experiment was a success!")


if __name__ == "__main__":
    ray.init(f"ray://127.0.0.1:{LOCAL_PORT}")
    main()
    # print(ray.available_resources())
    ray.shutdown()