import os 
import ray 
import time
from dataclasses import dataclass
import logging

log = logging.getLogger(__name__)
# configure logging
FORMAT = "[%(levelname)s] [%(filename)-20s:%(lineno)-s] %(message)s"
logging.basicConfig(format=FORMAT, level=os.environ.get("LOGLEVEL", "INFO"))

@dataclass
class FuncTimeMetric():
    exec_time: float

# from ..utils import Singleton
@ray.remote(num_cpus=0)
class FunctionTimeRecorder():
    def __init__(self):
        log.info("Function Time recorder intiated")
        self.metrics = []

    def report_metrics(self, metric):
        self.metrics.append(metric)

    def get_metrics(self):
        return self.metrics

# FTRecorder = FunctionTimeRecorder()

# def record_func_time_wrapper(function):
#     def wrapper(*args, **kwargs):
#         start_time = time.time()
#         function(*args, **kwargs)
#         exec_time = time.time() - start_time
        
#         start_time = time.time()
#         m = FuncTimeMetric(exec_time)
#         ref = FTRecorder.report_metrics.remote(m)
#         # ref = FTRecorder.report_metrics(m)
#         # ray.get(ref)
#         overhead_time = time.time() - start_time
#         log.info(f"Overhead of function time recorder: {overhead_time}")
        
#     return wrapper

def get_function_time_metrics_from_recorder():
    FTRecorder = ray.get_actor("FTRecorder")
    return ray.get(FTRecorder.get_metrics.remote())

def test_function():
    return 0

LOCAL_PORT = 10001
def main():
    ray.init(f"ray://127.0.0.1:{LOCAL_PORT}")

    FTRecorderHandle = FunctionTimeRecorder.options(name="FTRecorder").remote()

    # w_f = record_func_time_wrapper(test_function)
    m = get_function_time_metrics_from_recorder()
    print(f"Metrics = {m}")
    print("Done")

    ray.shutdown()

main()