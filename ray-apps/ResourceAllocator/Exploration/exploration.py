from collections import defaultdict
from dataclasses import dataclass
import os
import time
import logging
import numpy as np
import pandas as pd
import ray 
import statistics


log = logging.getLogger(__name__)
# configure logging
FORMAT = "[%(levelname)s] [%(filename)-20s:%(lineno)-s] %(message)s"
logging.basicConfig(format=FORMAT, level=os.environ.get("LOGLEVEL", "DEBUG"))

from .FunctionRecorder import FunctionRecorder, record_func_time_wrapper, count_func_invocations_wrapper

@dataclass(frozen=True)
class ConfigPoint():
    cluster_config: tuple
    function_resource_map: tuple

@dataclass
class ConfigPointResult():
    loss_used: str
    loss_value: float
    execution_time: float
    cost_to_run: float
    func_times: dict
    attempt_n: int
    
@dataclass
class ConfigPointReturn():
    cp: ConfigPoint
    cpR : ConfigPointResult 

@dataclass
class ConfigRangeToExplore():
    worker_count_range: tuple
    cpu_per_worker_range: tuple
    task_cpu_range: tuple

class BaseExplorationStrategy():
    def __init__(self, initial_f_resource_map, initial_cluster_config, update_resources_func, update_fuction_wrappers_func, func_to_explore, record_time=True, c_range_to_explore=None):
        self.initial_f_resource_map = initial_f_resource_map
        self.initial_cluster_config = initial_cluster_config
        self.update_resources_func = update_resources_func

        self.func_to_explore = func_to_explore
        self.loss = self.get_loss_func()
        self.record_time = record_time

        if c_range_to_explore == None:
            c_range_to_explore = ConfigRangeToExplore(worker_count_range=[1, 1], cpu_per_worker_range=[1, 1], task_cpu_range=[0.1, 1])
        self.c_range_to_explore = c_range_to_explore

        self.FRecorderHandle = FunctionRecorder.remote()
        self.update_fuction_wrappers_func = lambda func: update_fuction_wrappers_func(func, self.FRecorderHandle)

        self.configurations = defaultdict(list)
        self.configurations_summary = defaultdict(list)

    def get_ftr_handle(self):
        return self.FRecorderHandle
    
    def compute_cost_to_run(self, cluster_config, exec_time):
        # Currently, the cost to run a configuration is computed by the time taken to run the experiment times total nr of cpus
        total_cpus = cluster_config["num_workers"] * int(cluster_config["cpu_per_worker"])
        return total_cpus * exec_time

    def compute_cost_to_run_per_func(self, config_point, func_execution_times):
        # This cost is computed as a multiplication of time running times cost per run with cpu config
        # normalizing that 1 sec with 1 cpu equals 1 
        # so for example 1 sec with 0.5 cpu equals 0.5
        # and 0.5 seconds with 0.5 cpu equals 0.25
        total_cost = 0
        for f_name, cpu_resource in config_point.function_resource_map:
            total_cost += cpu_resource * func_execution_times[f_name]
        return total_cost

    def store_config_point(self, cluster_config, f_resource_map, attempt_n, loss, loss_value, exec_time):
        # Using cp as hash index, because of this we need to change the cluster_config and f_resource_map to immutable types like tupples.
        cp = ConfigPoint(tuple(cluster_config.items()), tuple(f_resource_map.items()))
        cpR = ConfigPointResult(loss.__name__, loss_value, exec_time, self.compute_cost_to_run(cluster_config, exec_time), {}, attempt_n)

        if self.record_time:
            time_metrics = ray.get(self.FRecorderHandle.wait_get_metrics_and_reset.remote())
            cpR.func_times = time_metrics
            cpR.cost_to_run = self.compute_cost_to_run_per_func(cp, time_metrics)
            # print(f"FR time {time_metrics}")
        
        log.debug(f"Storing config:\n {cp}")
        self.configurations[cp].append(cpR)
    
    def make_config_summary(self, cp):
        interested_columns = ["loss_value", "execution_time", "cost_to_run"]
        result = {}
        for column in interested_columns:
            metrics = []
            for config in self.configurations[cp]:
                metrics.append(getattr(config, column))
            result[column] = statistics.median(metrics)

        cpR = ConfigPointResult(self.loss.__name__, result["loss_value"], result["execution_time"], result["cost_to_run"], func_times={}, attempt_n=0)

        self.configurations_summary[cp] = cpR

        return cpR

    def get_function_time(self):
        self.FRecorderHandle

    def explore_function_wrapper(self, cluster_config, f_resource_map):
        # log.info(f"Executing application with the following cluster config = \n{cluster_config}")
        # log.info(f"Executing application with the following function resource configuration = \n{f_resource_map}")
        self.update_resources_func(cluster_config, f_resource_map)

        start = time.time()
        log.debug("Calling application")
        self.func_to_explore()
        # time.sleep(0.1)
        exec_time = round((time.time() - start), 3)
        log.debug(f"Application returned. Exec_time = {exec_time}s")

        loss_v = self.loss(cluster_config, f_resource_map, exec_time)

        # return function to minimize
        return loss_v, exec_time

    def explore_config_space(self, configs_to_test, attempts_per_config=3, per_func_pareto=False, **kwargs):
        # Clear initialization costs by calling the function 1 time before exploring
        log.info("Clearing initialization overhead by calling the function with current setup.")
        
        log.info("Recording the number of function invocations.")
        self.update_fuction_wrappers_func(count_func_invocations_wrapper)
        self.func_to_explore()
        n_invocations = ray.get(self.FRecorderHandle.get_invocation_count.remote())
        print(f"Found {n_invocations} invocations during execution.")
        
        if self.record_time:
            log.info("Switch to record time of functions")
            self.update_fuction_wrappers_func(record_func_time_wrapper)

        log.info("Exploring config space")

        start = time.time()
        self.explore(configs_to_test, attempts_per_config, per_func_pareto, **kwargs)
        self.time_to_search = time.time() - start

    def explore(self, configs_to_test, attempts_per_config, per_func_pareto, **kwargs):
        raise NotImplementedError

    def explore_per_func(self, configs_to_test, attempts_per_config, **kwargs):
        raise NotImplementedError

    def explore_config(self, cluster_config, f_resource_map, attempts_per_config):
        # Before testing we check if this config was already tested before
        cp, cpR = self.get_config_value(cluster_config, f_resource_map) 
        if cpR:
            log.info(f"Requested configuration was already explored, returning previously obtained value.")
            return cpR.loss_value, cpR.cost_to_run

        log.info(f"Experimenting with cluster_config:\n{cluster_config}")
        log.info(f"Function resource map:\n{f_resource_map}")

        for attempt_n in range(attempts_per_config):
            log.info(f"Attempt no {attempt_n}")
            loss_v, exec_time = self.explore_function_wrapper(cluster_config, f_resource_map)
            self.store_config_point(cluster_config, f_resource_map, attempt_n, self.loss, loss_v, exec_time)

        config_summary = self.make_config_summary(cp)

        log.info(f"Reported loss = {config_summary.loss_value}")
        log.info(f"Reported cost = {config_summary.cost_to_run}")
        return config_summary.loss_value, config_summary.cost_to_run

    def experiment_w_initial_config(self, attempts_per_config):
        # Experimenting with the initial configuration
        cluster_config = self.initial_cluster_config
        f_resource_map = self.initial_f_resource_map
        # start experimentation using user provided initial configuration?
        log.info(f"Experimenting with the initial configuration")
        loss, cost = self.explore_config(cluster_config, f_resource_map, attempts_per_config)

    def save_configs_to_file(self, filename):
        with open(filename + ".txt", 'w') as f:
            for config_point, config_point_result in self.configurations.items():
                f.write(f"{config_point}|->{config_point_result}\n")
            f.write(f"Time taken = {self.time_to_search}s\n")

    def save_configs_to_csv_file(self, filename):
        if len(self.configurations) == 0:
            print("No configurations explored.")
            return

        list_df = []
        config_point, config_point_result_list = list(self.configurations.items())[0]
        c_config_columns = [("cluster_config", c) for c in dict(config_point.cluster_config).keys()]
        f_res_map_columns = [("function_resource_map", c) for c in dict(config_point.function_resource_map).keys()]
        c_p_result_columns = [("config_point_result", c) for c in config_point_result_list[0].__dict__.keys()]
        func_time_columns = []

        if self.record_time:
            c_p_result_columns = list(filter(lambda column_el: column_el[1] != "func_times", c_p_result_columns))
            func_time_columns = [("function_times", c) for c in config_point_result_list[0].func_times.keys()]

        columns = [*c_config_columns, *f_res_map_columns, *func_time_columns, *c_p_result_columns]

        for config_point, config_point_result_list in self.configurations.items():
            for config_point_result in config_point_result_list:
                c_config = dict(config_point.cluster_config).values()
                f_res_map = dict(config_point.function_resource_map).values()
                c_p_result = list(config_point_result.__dict__.values())
                func_times = []

                if self.record_time:
                    func_times = config_point_result.func_times.values()
                    del c_p_result[-2] # remove this from showing up in the c_p_column
                
                list_df.append([*c_config, *f_res_map, *func_times, *c_p_result])

        df = pd.DataFrame(list_df, columns=pd.MultiIndex.from_tuples(columns))
        df.to_csv(filename + ".csv", index=False) 

    # def get_best_config(self):
    #     log.info(f"Getting best configuration.")

    #     if len(self.configurations) == 0:
    #         log.error("Requesting best config but no configuration explored.")
    #         return []

    #     initial_configuration, initial_configuration_res = self.get_config_value(self.initial_cluster_config, self.initial_f_resource_map)
    #     best_config, best_config_res = min(self.configurations.items(), key=lambda c: c[1].loss_value)

    #     log.info(f"Initial configuration = \n{initial_configuration}")
    #     log.info(f"Best configuration found = \n{best_config}")
    #     log.info(f"Init configuration res = \n{initial_configuration_res}")
    #     log.info(f"Best configuration res = \n{best_config_res}")
    #     log.info(f"Improvement over initial configuration = {(round(initial_configuration_res.loss_value/best_config_res.loss_value,2)-1)*100}%")
    #     return best_config

    def pareto_front_configs(self):
        config_points = np.empty(len(self.configurations), dtype=object)
        n_configs = len(self.configurations)
        n_cost = 2 # cost represents execution time and monetary cost
        points_w_cost = np.empty((n_configs, n_cost))
        for i, (config_point, config_point_result) in enumerate(self.configurations_summary.items()):
            points_w_cost[i][0] = config_point_result.cost_to_run
            points_w_cost[i][1] = config_point_result.execution_time

            config_points[i] = ConfigPointReturn(config_point, config_point_result)

        pareto_indices = get_pareto_front_indices(points_w_cost)
        pareto_front = config_points[pareto_indices]

        log.info("Pareto front: ")
        log.info(pareto_front)

        return config_points.tolist(), pareto_front

    def get_config_value(self, cluster_config, f_resource_map):
        cp = ConfigPoint(tuple(cluster_config.items()), tuple(f_resource_map.items()))
        if cp in self.configurations_summary:
            return cp, self.configurations_summary[cp]
        
        return cp, None

    # TODO use a more interesting loss function
    def get_loss_func(self):
        l_func = None
        def latency_loss(cluster_config, f_resource_map, execution_time):
            return execution_time
        l_func = latency_loss
        return l_func
                
        # if SLO:
        #     def loss_SLO(latency, resources):
                
        #         loss = penaltyForSLOViolation*max(0, latency-SLO)
        #         if minResources:
        #             # Add up all the resources being used
        #             resourceSum = penaltyResourceUsage*sum(resources.values())
        #             loss += resourceSum
        #         return -loss
        #     return loss_SLO
        # else:
        #     def loss_fastest(latency, resources):
        #         loss = latency
        #         if minResources:
        #             # Add up all the resources being used
        #             resourceSum = penaltyResourceUsage*sum(resources.values())
        #             loss += resourceSum
        #         return -loss
        #     return loss_fastest

# Compute the pareto front to return back to the user
# Taken from: https://stackoverflow.com/questions/32791911/fast-calculation-of-pareto-front-in-python
def get_pareto_front_indices(points):
    """
    Find the pareto front
    :param points: An (n_points, n_costs) array
    :return: A (N, ) section of points array containing only elements in the Pareto front 
    """
    n_points = points.shape[0]
    in_p_front = np.ones(n_points, dtype = bool) # by default all points are in the pareto front
    for i, c in enumerate(points):
        if in_p_front[i]:
            in_p_front[in_p_front] = np.any(points[in_p_front] < c, axis=1)  # Keep any point with a lower cost
            in_p_front[i] = True  # And keep self
    return in_p_front