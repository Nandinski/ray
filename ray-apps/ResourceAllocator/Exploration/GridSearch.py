
from .exploration import BaseExplorationStrategy
import logging
import numpy as np
log = logging.getLogger(__name__)
from itertools import product

class GridExplorationStrategy(BaseExplorationStrategy):
    def explore(self, configs_to_test, attempts_per_config=3):
        # Experimenting with the initial configuration
        cluster_config = self.initial_cluster_config
        f_resource_map = self.initial_f_resource_map

        # start experimentation using user provided initial configuration?
        log.info(f"Experimenting with the initial configuration")
        # current_best_loss = self.explore_config(cluster_config, f_resource_map, attempts_per_config)
        # log.info(f"Loss obtained using initial configuration = {current_best_loss}")

        nc_min, nc_max = self.c_range_to_explore.node_count_range
        cn_min, cn_max = self.c_range_to_explore.cpu_per_node_range

        f_resource_grid = self.get_resource_grid()
        for node_count in range(nc_max,nc_min-1, -1):
            for cpu_per_node in range(cn_max, cn_min-1, -1):
                cluster_config = {"num_workers": node_count, "cpu": str(cpu_per_node), "memory": "1Gi"}
                for f_r_map in f_resource_grid:
                    loss = self.explore_config(cluster_config, f_r_map, attempts_per_config)
                        
    def explore_config(self, cluster_config, f_resource_map, attempts_per_config):
        # Before testing we check if this config was already tested before
        cp, cpR = self.get_config_value(cluster_config, f_resource_map) 
        if cpR:
            log.info(f"Requested configuration was already explored, returning previously obtained value.")
            return cpR.loss_value

        log.info(f"Experimenting with cluster_config:\n{cluster_config}")
        log.info(f"Function resource map:\n{f_resource_map}")

        current_config_loss = 0
        for attempt_n in range(attempts_per_config):
            log.info(f"Attempt no {attempt_n}")
            l = self.explore_function_wrapper(cluster_config, f_resource_map)
            log.info(f"Loss = {l}")
            current_config_loss +=  l * 1/attempts_per_config

        log.info(f"Obtained loss = {current_config_loss}")
        return current_config_loss

    def get_resource_grid(self):
        tc_min, tc_max = self.c_range_to_explore.task_cpu_range
        CPU_STEPS = 4
        resource_per_function = [np.linspace(tc_max, tc_min, CPU_STEPS) for func in self.initial_f_resource_map]
        f_resource_grid = (dict(zip(self.initial_f_resource_map.keys(), values)) for values in product(*resource_per_function))
        return f_resource_grid
      