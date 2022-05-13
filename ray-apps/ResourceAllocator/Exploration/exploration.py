from dataclasses import dataclass
import os
import time
import logging

from sklearn.exceptions import NonBLASDotWarning
log = logging.getLogger(__name__)
# configure logging
FORMAT = "[%(levelname)s] [%(filename)-20s:%(lineno)-s] %(message)s"
logging.basicConfig(format=FORMAT, level=os.environ.get("LOGLEVEL", "DEBUG"))

@dataclass(frozen=True)
class ConfigPoint():
    cluster_config: tuple
    function_resource_map: tuple

@dataclass
class ConfigPointResult():
    loss_used: str
    loss_value: float
    execution_time: float
    
@dataclass
class ConfigRangeToExplore():
    node_count_range: tuple
    cpu_per_node_range: tuple
    task_cpu_range: tuple

class BaseExplorationStrategy():
    def __init__(self, initial_f_resource_map, initial_cluster_config, update_resources_func, func_to_explore, c_range_to_explore=None):
        self.initial_f_resource_map = initial_f_resource_map
        self.initial_cluster_config = initial_cluster_config
        self.update_resources_func = update_resources_func
        self.func_to_explore = func_to_explore
        self.loss = self.get_loss_func()

        if c_range_to_explore == None:
            c_range_to_explore = ConfigRangeToExplore(node_count_range=[1, 2], cpu_per_node_range=[1, 1], task_cpu_range=[0.1, 1])
        self.c_range_to_explore = c_range_to_explore

        self.configurations = {}
    
    def store_config_point(self, cluster_config, f_resource_map, loss, loss_value, exec_time):
        # Using cp as hash index, because of this we need to change the cluster_config and f_resource_map to immutable types like tupples.
        cp = ConfigPoint(tuple(cluster_config.items()), tuple(f_resource_map.items()))
        cpR = ConfigPointResult(loss.__name__, loss_value, exec_time)
        
        log.debug(f"Storing config:\n {cp}")
        self.configurations[cp] = cpR

    def explore_function_wrapper(self, cluster_config, f_resource_map):
        log.info(f"Executing application with the following cluster config = \n{cluster_config}")
        log.info(f"Executing application with the following function resource configuration = \n{f_resource_map}")
        self.update_resources_func(cluster_config, f_resource_map)

        start = time.time()
        log.debug("Calling application")
        self.func_to_explore()
        exec_time = round((time.time() - start), 2)
        log.debug(f"Application returned. Exec_time = {exec_time}s")

        loss_v = self.loss(cluster_config, f_resource_map, exec_time)
        self.store_config_point(cluster_config, f_resource_map, self.loss, loss_v, exec_time)

        # return function to minimize
        return loss_v

    def explore(self, configs_to_test):
        raise NotImplementedError

    def get_best_config(self):
        log.info(f"Getting best configuration.")

        if len(self.configurations) == 0:
            log.error("Requesting best config but no configuration explored.")
            return []

        initial_configuration, initial_configuration_res = self.get_config_value(self.initial_cluster_config, self.initial_f_resource_map)
        best_config, best_config_res = min(self.configurations.items(), key=lambda c: c[1].loss_value)

        log.info(f"Initial configuration = \n{initial_configuration}")
        log.info(f"Best configuration found = \n{best_config}")
        log.info(f"Improvement over initial configuration = {(1 - round(initial_configuration_res.loss_value/best_config_res.loss_value,2))*100}%")
        return best_config

    def get_config_value(self, cluster_config, f_resource_map):
        cp = ConfigPoint(tuple(cluster_config.items()), tuple(f_resource_map.items()))
        if cp in self.configurations:
            return cp, self.configurations[cp]
        
        return cp, None

    # TODO use a more interesting loss function
    def get_loss_func(self):
        l_func = None
        def latency_loss(cluster_config, f_resource_map, execution_time):
            return -execution_time
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