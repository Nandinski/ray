from .exploration import BaseExplorationStrategy
from ..cluster_manager import make_cluster_config
import logging
log = logging.getLogger(__name__)

# from bayes_opt import BayesianOptimization
from skopt import gp_minimize
RANDOM_SEED = 14
class BayesianExplorationStrategy(BaseExplorationStrategy):
    def explore(self, configs_to_test, attempts_per_config, r_starts=1):
        f_bounds = {}
        # Set bounds for each remote function CPU
        for f in self.initial_f_resource_map:
            f_bounds[f"{f}"] = self.c_range_to_explore.task_cpu_range # TODO change it to be a list of discrete values?

        # get function_names
        f_keys = self.initial_f_resource_map.keys()        
    
        bounds = [  self.c_range_to_explore.worker_count_range, 
                    self.c_range_to_explore.cpu_per_worker_range,
                    *f_bounds.values()]
        bounds = list(map(categorize_range_if_need, bounds))
        log.info(f"{bounds=}")

        cluster_config = self.initial_cluster_config
        f_resource_map = self.initial_f_resource_map
        initial_configuration = [str(cluster_config["num_workers"]), str(cluster_config["cpu_per_worker"]),
                                 *f_resource_map.values()]

        log.info(f"Initial_configuration = {initial_configuration=}")

        f_to_minimize = lambda params: self.f_wrapper(params, f_keys, attempts_per_config)
        res = gp_minimize(  f_to_minimize,              # the function to minimize
                            bounds,                     # the bounds on each dimension of x
                            x0= initial_configuration,
                            acq_func="LCB",             # the acquisition function
                            n_calls=r_starts + configs_to_test, # the number of evaluations of f
                            n_initial_points= r_starts, # the number of random initialization points
                            # noise=0.1**2,             # the noise level (optional)
                            random_state=RANDOM_SEED)   # the random seed
        

    # f_wrapper takes as inputs the list of parameters
    # parameter order:
    # node_count, cpu_per_node, task_cpu*
    def f_wrapper(self, params, f_keys, attempts_per_config):
        cluster_config = make_cluster_config(params[0], params[1])
        f_resource_map = make_f_map(f_keys, params[2:])
        loss, cost = self.explore_config(cluster_config, f_resource_map, attempts_per_config)
        return loss

def categorize_range_if_need(range_t):
    if range_t[0] == range_t[1]:
        return [str(range_t[0])]
    else:
        return range_t


def make_f_map(f_keys, f_resources):
    f_map = {}
    for i, f in enumerate(f_keys):
        f_map[f] = round(f_resources[i], 1)
    return f_map