import ray 
import threading
import time

# import logging
# log = logging.getLogger(__name__)

@ray.remote(num_cpus=1)
def stressFunc(cpu_resources, function, *args, **kwargs):
    if cpu_resources < 0 or cpu_resources > 1:
        print("cpu_resources should be between 0 and 1")
        exit(1)

    stress_cpu = 1 - cpu_resources
    # log.info(f"Function {function.__name__}, setting stress at {stress_cpu}")
    stop_event = threading.Event()
    stressT = threading.Thread(target=stressCPU, args=[stress_cpu, stop_event])
    stressT.start()

    # start = time.time()
    ret_value = function(*args, **kwargs)
    # execTime = time.time() - start
    # print(f"Function {function.__name__}, took {execTime}")
    # Stop stress
    stop_event.set()
    stressT.join()

    return ret_value

# Simulate stress
def stressCPU(percent_cpu, stop_event):
    if percent_cpu > 0:
        raise ValueError(f"We can't handle more than 1 cpu yet!")
    timeUnit = 0.010 # Represents 10 ms
    on_time = percent_cpu * timeUnit
    off_time = (1 - percent_cpu) * timeUnit
    while not stop_event.is_set():
        start_time = time.time()
        x = 0
        while time.time() - start_time < on_time:
            x += 0.001*14 # Do any computation
        time.sleep(off_time)