import sys
import time
import ray
import requests
import random
import pandas as pd
import argparse
import os
import functools
from ray.autoscaler.sdk import request_resources

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

    def waitForMetricCount(self, count):
        while True:
            if len(self.metricList) >= count:
                return
            else:
                print("Not all metrics present. Waiting.")
                time.sleep(0.1)

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
    for i in range(0, int(1e7)):
        x += i
    return x

# from ray.util.metrics import Counter, Gauge, Histogram
# def monitorF(func):
#     print(f"Monitoring function {func.__name__}.")
#     histogram = Histogram(
#         "request_latency",
#         description="Latencies of requests in ms.",
#         boundaries=[0.1, 100],
#         tag_keys=("func_name", ))
#     print(f"{histogram._default_tags=}")
#     @functools.wraps(func)
#     def wrapper_decorator(*args, **kwargs):
#         # Do something before
#         start = time.time()
#         value = func(*args, **kwargs)
#         # Do something after
#         #  Record the latency for this request in ms.
#         latency = 1000 * (time.time() - start)
#         print(f"{histogram._default_tags=}")
#         histogram.observe(latency, {"func_name": func.__name__})
#         print(f"Function {func.__name__}, measured latency = {latency}")
#         return value
#     return wrapper_decorator

from resource_allocator import resourceWrapper
# @resourceWrapper
# @ray.remote(num_cpus=1)
# def processURL(url, headerMetrics, mStore):
#     # print("Fetching ", url)
    
#     start = time.time()
#     # fetchURL(url)
#     # simulate fetching with sleep so that we don't bottleneck on network
#     # Sleep for 40ms as this is the average time fetching files
#     time.sleep(40/1000)
#     timeGettingFile = time.time() - start 
#     # print(f"Time getting file: {timeGettingFile}")

#     # Simulate computation
#     start = time.time()
#     computeSim()
#     timeComputing = time.time() - start 
#     # print(f"Time computing: {timeComputing}")

#     mStore.storeMetric.remote(headerMetrics, URLMetrics(url, timeGettingFile, timeComputing))
#     return "Success"

@resourceWrapper
@ray.remote(num_cpus=1)
def processURL(url):
    # print("Fetching ", url)
    
    # start = time.time()
    # fetchURL(url)
    # simulate fetching with sleep so that we don't bottleneck on network
    # Sleep for 40ms as this is the average time fetching files
    time.sleep(1000/1000)
    # timeGettingFile = time.time() - start 
    # print(f"Time getting file: {timeGettingFile}")

    # Simulate computation
    # start = time.time()
    computeSim()
    # timeComputing = time.time() - start 
    # print(f"Time computing: {timeComputing}")
    return "Success"

# @ray.remote
def run_remote_experiment(nr_urls):
    start = time.time()
    refs = [processURL.remote(getFileURL((i % 5)+1)) for i in range(nr_urls)]
    pageResults = ray.get(refs)
    execTime = time.time() - start
    print(f"ExecTime = {round(execTime, 2)}s")

@ray.remote
def remote_experiment(nr_urls, nCPU, curr_exp, mStore):
    start = time.time()
    refs = [processURL.options(num_cpus=nCPU).remote(getFileURL((i % 5)+1), HeaderMetrics(nCPU, curr_exp), mStore) for i in range(nr_urls)]
    pageResults = ray.get(refs)
    execTime = time.time() - start
    return execTime

def run_test(nr_urls, curr_exp, mStore, nCPU=1):
    # print(f"Running experiment with {nCPU=} and {curr_exp=}")
    execTime = ray.get(remote_experiment.remote(nr_urls, nCPU, curr_exp, mStore))
    print(f"{execTime=}")
    return execTime
    
def profile_run(nr_urls, CPUs_to_exp, exp_count, mStore):
    execTimeList = []

    for nCPU in CPUs_to_exp:
        for curr_exp in range(exp_count):
            execTime = run_test(nr_urls, curr_exp, mStore, nCPU=nCPU)
            execTimeList.append([nCPU, curr_exp, execTime])
    
    return execTimeList

def run_experiment(expName, exp_count, nr_urls, CPUs_to_exp, exp_type="profile"):
    metricAttrs = [field.name for field in fields(URLMetrics)]
    headerAttrs = [field.name for field in fields(HeaderMetrics)]
    
    mStore = MetricStore.remote(headerAttrs, metricAttrs)
    
    execTimeList = []
    if exp_type == "profile":
        execTimeList = profile_run(nr_urls, CPUs_to_exp, exp_count, mStore)
        metricsToWait = exp_count*nr_urls*len(CPUs_to_exp)

    # Wait until all metrics have been collected
    ray.get(mStore.waitForMetricCount.remote(metricsToWait))

    parentDir = os.path.join("Metrics", expName)
    os.makedirs(parentDir, exist_ok=True)
    metricDF = ray.get(mStore.getDF.remote())
    metricDF.to_csv(os.path.join(parentDir,'collectedMetrics.csv'), index=False)
    execDF = pd.DataFrame(execTimeList, columns=["nCPUs", "expN", "Execution Time"])
    execDF.to_csv(os.path.join(parentDir, 'execTime.csv'), index=False)


def main(nr_urls):
    ray.init(f"ray://127.0.0.1:{LOCAL_PORT}")
    wait_for_nodes(2)
    run_remote_experiment(nr_urls)
    ray.shutdown()
    # print("Page fetcher experiment was a success!")


@ray.remote
def no_work(x):
    return x

def measure_overhead():
    start = time.time()
    num_calls = 1000
    [ray.get(no_work.remote(x)) for x in range(num_calls)]
    print("per task overhead (ms) =", (time.time() - start)*1000/num_calls)

from resource_allocator import rManager
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='PageFetcher on Ray')
    parser.add_argument('--exp_name', type=str, help='Name of experiment to prepend to output files', required=True)
    parser.add_argument('--exp_count', type=int, help='Nr of times to repeat experiment', choices=range(1, int(1e3)), default=int(15))
    parser.add_argument('--nr_urls', type=int, help='Nr of urls to fetch', choices=range(1, int(1e4)), default=int(100))
    parser.add_argument('--desired_SLO', type=int, help='SLO in ms',  default=0)
    parser.add_argument('--max_config_attempts', type=int, help='Max configurations to experiment',  default=50)

    args = parser.parse_args()
    exp_name = args.exp_name
    exp_count = args.exp_count
    nr_urls = args.nr_urls
    desired_SLO = args.desired_SLO
    max_config_attempts = args.max_config_attempts

    # ray.init(f"ray://127.0.0.1:{LOCAL_PORT}")
    # rManager.setOptimizer("MonotonicDecrease")
    # main(nr_urls)
    # measure_overhead()
    rManager.optimize(lambda: main(nr_urls), SLO=desired_SLO, max_config_attempts=max_config_attempts)
    # ray.shutdown()
