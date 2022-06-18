from .exploration import BaseExplorationStrategy
from ..cluster_manager import make_cluster_config
import logging
import numpy as np
log = logging.getLogger(__name__)
from itertools import product

class GridExplorationStrategy(BaseExplorationStrategy):
    def explore(self, configs_to_test, attempts_per_config, per_func_pareto, cpu_step_size=2):
        # Experimenting with the initial configuration
        cluster_config = self.initial_cluster_config
        f_resource_map = self.initial_f_resource_map

        # start experimentation using user provided initial configuration?
        log.info(f"Experimenting with the initial configuration")
        current_best_loss = self.explore_config(cluster_config, f_resource_map, attempts_per_config)
        log.info(f"Loss obtained using initial configuration = {current_best_loss}")

        nc_min, nc_max = self.c_range_to_explore.worker_count_range
        cn_min, cn_max = self.c_range_to_explore.cpu_per_worker_range

        f_resource_grid = self.get_resource_grid(cpu_step_size, per_func_pareto)
        for node_count in range(nc_max,nc_min-1, -1):
            for cpu_per_node in range(cn_max, cn_min-1, -1):
                cluster_config = make_cluster_config(node_count, cpu_per_node)
                for i, f_r_map in enumerate(f_resource_grid):
                    print(f"Running exp {i} of {len(f_resource_grid)}")
                    loss = self.explore_config(cluster_config, f_r_map, attempts_per_config)

    def get_resource_grid(self, cpu_step_size, per_func_pareto):
        tc_min, tc_max = self.c_range_to_explore.task_cpu_range
        if per_func_pareto:
            resource_per_function = np.linspace(tc_max, tc_min, cpu_step_size)
            f_resource_grid = []
            default_f_resource = {func:tc_max for func in self.initial_f_resource_map}
            for func in self.initial_f_resource_map:
                for cpu in resource_per_function:
                    f_point = default_f_resource.copy()
                    f_point[func] = cpu
                    f_resource_grid.append(f_point)
        else:
            resource_per_function = [np.linspace(tc_max, tc_min, cpu_step_size) for func in self.initial_f_resource_map]
            f_resource_grid = [dict(zip(self.initial_f_resource_map.keys(), values)) for values in product(*resource_per_function)]
        
        return f_resource_grid
      