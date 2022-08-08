from sqlalchemy import false
from . import cluster_manager
from datetime import datetime
import os
import numpy as np

import logging
log = logging.getLogger(__name__)
# configure logging
FORMAT = "[%(levelname)s] [%(filename)-20s:%(lineno)-s] %(message)s"
logging.basicConfig(format=FORMAT, level=os.environ.get("LOGLEVEL", "INFO"))
# logging.disable()

from .Exploration.NullExploration import NullExplorationStrategy
from .Exploration.GridSearch import GridExplorationStrategy
from .Exploration.BayesianSearch import BayesianExplorationStrategy
from .Exploration.OPTUNASearch import OPTUNASearchStrategy
# from .Exploration.OPTUNASearch import GPFLOWOPTSearchStrategy
resource_Exploration_Strategies = {
    "NullExploration": NullExplorationStrategy,
    "GridSearch": GridExplorationStrategy,
    "BayesianOptSearch": BayesianExplorationStrategy,
    "OPTUNASearch": OPTUNASearchStrategy,
    # "GPFLOWOPTSearch": GPFLOWOPTSearchStrategy,
}

from .utils import Singleton
class ResourceManager(metaclass=Singleton):
    def __init__(self):
        self.initial_f_resource_map = {}
        self.cls_mapper = {}

        self.unoptimize_cls_mapper = {}
        self.unopt_initial_f_resource_map = {}

    def register(self, func_name, initial_ncpus, cls_ptr, optimize):
        log.info(f"Registering function {func_name}")
        if optimize:
            self.cls_mapper[func_name] = cls_ptr
            self.initial_f_resource_map[func_name] = initial_ncpus
        else:
            self.unoptimize_cls_mapper[func_name] = cls_ptr
            self.unopt_initial_f_resource_map[func_name] = initial_ncpus

    def update_f_resources(self, f_resource_map):
        for func_name, cls_ptr in self.cls_mapper.items():
            f_resources = {"num_cpus": f_resource_map[func_name]}
            cls_ptr.update_resources(f_resources)

        # Force ray to regenerate function
        for func_name, cls_ptr in self.unoptimize_cls_mapper.items():
            f_resources = {"num_cpus": self.unopt_initial_f_resource_map[func_name]}
            cls_ptr.update_resources(f_resources)

    def update_fuction_wrappers_func(self, function_wrapper, *args, **kwargs):
        for _, cls_ptr in self.cls_mapper.items():
            cls_ptr.update_function_wrapper(function_wrapper, *args, **kwargs)

    """
        Receives a function to optimize.
        It is expected that this function itself requires 1cpu. 
        All other functions called with Ray tasks will be optimized to use the lowest CPU required to achieve the desired SLO.
        Args:
            func (callable): Function to optimize. 
            SLO (float): desired SLO in seconds. 
            fastest (bool): if specified, will try to make the fastest computation it can.
            max_configuration_attemps (int): Number of configurations to try. 
    """
    def optimize(self, func_to_explore, exploration_strategy="NullExploration", configs_to_test=1, save_searched_config=True, **kwargs):
        print(f"Optimizing function {func_to_explore.__name__}.")

        initial_cluster_config = cluster_manager.get_cluster_spec()
        exploration_strategyClass = self.get_exploration_strategy_class(exploration_strategy)
        expl_strat = exploration_strategyClass(self.initial_f_resource_map, 
                                               initial_cluster_config, 
                                               self.update_resources_func, 
                                               self.update_fuction_wrappers_func,
                                               func_to_explore)
        expl_strat.explore_config_space(configs_to_test, **kwargs)

        if save_searched_config:
            date = datetime.now().strftime("%Y_%m_%d-%I:%M:%S_%p")
            filename = f"optimize_{exploration_strategy}_{date}"
            # expl_strat.save_configs_to_file(filename) # no longer used?
            expl_strat.save_configs_to_csv_file(filename)

        # best_config = expl_strat.get_best_config()
        pareto_front_configs = expl_strat.pareto_front_configs()
        return pareto_front_configs

    def get_exploration_strategy_class(self, exploration_strategy):
        if exploration_strategy in resource_Exploration_Strategies:
            log.info(f"Using exploration strategy: {exploration_strategy}")
            return resource_Exploration_Strategies[exploration_strategy]
        else:
            raise NotImplementedError(f"Could not find exploration strategy: '{exploration_strategy}'")

    def update_resources_func(self, cluster_config, f_resource_map):
        cluster_manager.change_cluster_spec(cluster_config)
        self.update_f_resources(f_resource_map)
  
RManager = ResourceManager()

def resourceWrapper(func):
    print(f"Resource wraping function {func._function_name}.")
    class FuncWrapper:
        def __init__(self, func, initial_ncpus):
            self.og_func = func
            self.function = func
            self.resources = {"num_cpus": initial_ncpus}
            
        def update_function_wrapper(self, f_wrapper, *args, **kwargs):
            self.function = f_wrapper(self.og_func, *args, **kwargs)

        def remote(self, *args, **kwargs):
            # print(f"Func {func._function_name}, NCpus = {self.function._num_cpus}")
            return self.function.remote(*args, **kwargs)

        def update_resources(self, resources):
            self.resources = resources
            # print(f"Updated function resources: {self.resources}")
            # print(f"Updated resources of {func._function_name} to {self.resources}")
            self.function = func.options(**self.resources)
    
    fw = FuncWrapper(func, func._num_cpus)
    RManager.register(func._function_name, func._num_cpus, fw)
    return fw

from . import resource_isolation_simulator

import ray
def resourceWrapperStress(*args, **kwargs):
    def decorator(func):
        print(f"Resource wraping (w/stress) function {func.__name__}.")
        class FuncWrapper:
            def __init__(self, func, initial_ncpus):
                self.og_func = func
                self.function = func
                self.resources = {"num_cpus": initial_ncpus}
                self.stressFunc = resource_isolation_simulator.stressFunc.options(*args, **kwargs)
            
            def update_function_wrapper(self, f_wrapper, *args, **kwargs):
                self.function = f_wrapper(self.og_func, *args, **kwargs)

            def remote(self, *args, **kwargs):
                return self.stressFunc.remote(self.resources["num_cpus"], self.function, *args, **kwargs)

            def update_resources(self, resources):
                self.resources = resources
                # print(f"Updated function resources: {self.resources}")
                # print(f"Updated resources of {func.__name__} to {self.resources}")
        
        optimize = kwargs.pop("optimize", True)
        num_cpus = kwargs.get("num_cpus", 1)
        fw = FuncWrapper(func, num_cpus)
        RManager.register(func.__name__, num_cpus, fw, optimize)
        return fw

    # This is the case where the outside decorator receives the function
    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        return decorator(args[0])
    else:
        return decorator