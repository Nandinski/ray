from dataclasses import dataclass
import os
import time
import logging
log = logging.getLogger(__name__)
# configure logging
FORMAT = "[%(levelname)s] [%(filename)-20s:%(lineno)-s] %(message)s"
logging.basicConfig(format=FORMAT, level=os.environ.get("LOGLEVEL", "DEBUG"))

@dataclass
class ConfigPoint():
    cluster_config: dict
    function_resource_map: dict
    loss_used: str
    loss_value: float
    execution_time: float

class BaseExplorationStrategy():
    def __init__(self, initial_f_resource_map, initial_cluster_config, update_resources_func, func_to_explore):
        self.initial_f_resource_map = initial_f_resource_map
        self.initial_cluster_config = initial_cluster_config
        self.update_resources_func = update_resources_func
        self.func_to_explore = func_to_explore
        self.loss = self.get_loss_func()

        self.configurations = []
    
    def store_config_point(self, cluster_config, f_resource_map, loss, loss_value, exec_time):
        cp = ConfigPoint(cluster_config, f_resource_map, loss.__name__, loss_value, exec_time)
        log.debug(f"Storing config:\n {cp}")
        self.configurations.append(cp)

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

        initial_configuration = self.configurations[0]
        best_config = min(self.configurations, key=lambda c: c.loss_value)

        log.info(f"Initial configuration = \n{initial_configuration}")
        log.info(f"Best configuration found = \n{best_config}")
        log.info(f"Improvement over initial configuration = {(1 - round(initial_configuration.loss_value/best_config.loss_value,2))*100}%")
        return best_config

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