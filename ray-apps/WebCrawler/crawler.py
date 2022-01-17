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

@dataclass
class CrawlURLParams:
    URL: str
    Depth: int

@dataclass
class CrawlURLMetrics:
    params: CrawlURLParams
    timeGettingFile: float
    timeComputing: float
    timeEnqueing: float

@ray.remote
class MetricStore:
    def __init__(self, interestedVarName, metricDCAttrs):
        self.interestedVar = interestedVarName
        self.metricDCAttrs = metricDCAttrs
        self.metricList = []

    def storeMetric(self, id, metric):
        newMetric = [getattr(metric,attr) for attr in self.metricDCAttrs]
        newMetric.insert(0, id)
        self.metricList.append(newMetric)
    
    def getMetricsByID(self, id):
        return self.store[id]

    def getDF(self):
        df = pd.DataFrame(self.metricList, columns=[self.interestedVar, *self.metricDCAttrs])
        return df

def getRandomFileURL():
    # Get nsqlookupdAddr
    CONTENT_SERVER_ADDR = "content-server-service"
    nFiles = 1000
    randomFile = random. randint(1, nFiles)
    return f"http://{CONTENT_SERVER_ADDR}:9090/files/file{randomFile}.dat"


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
    for i in range(0, int(1e5)):
        x += i
    return x

@ray.remote(num_cpus=1)
def crawlURL(params, maxDepth, n_found_URLs, nCPU, mStore):
    print("Fetching ", params.URL, " depth: ", params.Depth)
    
    start = time.time()
    fetchURL(params.URL)
    timeGettingFile = time.time() - start 
    print(f"Time getting file: {timeGettingFile}")

    # Simulate computation
    start = time.time()
    computeSim()
    timeComputing = time.time() - start 
    print(f"Time computing: {timeComputing}")

    depth = params.Depth
    depth += 1
    if depth > maxDepth:
        mStore.storeMetric.remote(nCPU, CrawlURLMetrics(params, timeGettingFile, timeComputing, 0))
        print("Max depth reached. Exiting.")
        return 
    
    foundURLs = [getRandomFileURL() for _ in range(n_found_URLs)]

    start = time.time()
    refs = []
    for url in foundURLs:
        print("Found url: ", url)
        ref = crawlURL.remote(CrawlURLParams(url, depth), maxDepth, n_found_URLs, nCPU, mStore)
        # ref = crawlURL.options(num_cpus=nCPU).remote(CrawlURLParams(url, depth), maxDepth, n_found_URLs, nCPU, mStore)
        refs.append(ref)
    timeEnqueing = time.time() - start 
    print("Time enqueing: ", timeEnqueing)

    mStore.storeMetric.remote(nCPU, CrawlURLMetrics(params, timeGettingFile, timeComputing, timeEnqueing))
    ray.get(refs)
    return

def run_experiment(max_depth, n_found_URLs):
    attrs = [field.name for field in fields(CrawlURLMetrics)]
    mStore = MetricStore.remote("nCPUs", attrs)
    execTimeList = []
    
    for nCPU in [1]:
        start = time.time()
    
        # ref = crawlURL.options(num_cpus=nCPU).remote(CrawlURLParams(getRandomFileURL(), 0), max_depth, n_found_URLs, nCPU, mStore)
        ref = crawlURL.remote(CrawlURLParams(getRandomFileURL(), 0), max_depth, n_found_URLs, nCPU, mStore)
        ref = ray.get(ref)
    
        execTime = time.time() - start
        print(f"{execTime=}")
        execTimeList.append([nCPU, execTime])

    metricDF = ray.get(mStore.getDF.remote())
    metricDF.to_csv('collectedMetrics.csv', index=False)
    execDF = pd.DataFrame(execTimeList, columns=["nCPUs", "Execution Time"])
    execDF.to_csv('execTime.csv', index=False)


def main():
    parser = argparse.ArgumentParser(description='Webcrawler on Ray')
    parser.add_argument('--max_depth', type=int, help='Set Max depth', choices=range(1, 10), default=2)
    parser.add_argument('--n_found_URLs', type=int, help='Set Found urls per crawl', choices=range(1, 10), default=2)

    args = parser.parse_args()
    max_depth = args.max_depth
    n_found_URLs = args.n_found_URLs

    wait_for_nodes(3)
    run_experiment(max_depth, n_found_URLs)
    sys.stdout.flush()
    print("Web crawl experiment was a success!")


if __name__ == "__main__":
    ray.init(f"ray://127.0.0.1:{LOCAL_PORT}")
    main()
    # print(ray.available_resources())
    ray.shutdown()