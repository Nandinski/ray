import os 
import ray 
import time
from dataclasses import dataclass
import logging
from collections import defaultdict
import statistics
log = logging.getLogger(__name__)

@dataclass
class FuncTimeMetric():
    exec_time: float

# from ..utils import Singleton
@ray.remote(num_cpus=0)
class FunctionRecorder():
    def __init__(self):
        log.info("Function Time recorder intiated")
        self.invocation_count = defaultdict(int)
        self.metrics = defaultdict(float)
        self.total_invocation_count = 0
        self.received_metrics_count = 0

    def report_metrics(self, func_name, metric):
        # print("Reporting metric")
        self.metrics[func_name] += metric
        self.received_metrics_count += 1

    def count_invocations(self, func_name):
        # print(f"Received invocation from {func_name}")
        self.invocation_count[func_name] += 1
        self.total_invocation_count += 1

    def get_invocation_count(self):
        return dict(self.invocation_count)

    def get_median_metrics(self):
        median_metrics = {}
        for func_name, metrics in self.metrics.items():
            median_metrics[func_name] = statistics.median(metrics)
        return median_metrics

    def wait_get_median_and_reset(self):
        print("Getting median values from FRecorder")
        while(self.received_metrics_count != self.total_invocation_count):
            print(f"Awaiting metrics to arrive. Current received = {self.received_metrics_count}. Expected = {self.total_invocation_count}")
            time.sleep(0.05)
        
        median_metrics = self.get_median_metrics()
        
        self.reset_metrics()
        return median_metrics
    
    def reset_metrics(self):
        self.metrics = defaultdict(float)
        self.received_metrics_count = 0

    def wait_get_metrics_and_reset(self):
        while(self.received_metrics_count != self.total_invocation_count):
            print(f"Awaiting metrics to arrive. Current received = {self.received_metrics_count}. Expected = {self.total_invocation_count}")
            time.sleep(0.1)
            
        metrics = dict(self.metrics)
        self.reset_metrics()
        return metrics

def count_func_invocations_wrapper(function, FTRecorder):
    def wrapper(*args, **kwargs):
        result = function(*args, **kwargs)
        ray.get(FTRecorder.count_invocations.remote(function.__name__))
        return result
    return wrapper
    
def record_func_time_wrapper(function, FTRecorder):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = function(*args, **kwargs)
        exec_time = time.time() - start_time
        
        # start_time = time.time()
        # m = FuncTimeMetric(exec_time)
        ref = FTRecorder.report_metrics.remote(function.__name__, exec_time)
        # print("Metric Reported")
        # ray.get(ref) # wait for metric to be pushed
        # overhead_time = time.time() - start_time
        # print(f"Overhead of function time recorder: {overhead_time}")
        
        return result
    return wrapper