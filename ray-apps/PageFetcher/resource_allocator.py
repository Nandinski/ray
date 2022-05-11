from audioop import avg
from email.policy import default
from pickletools import optimize
import resource
import ray 
import time
import functools

class BaseResourceManager():
    def __init__(self):
        self.f_resource_map = {}
        self.cls_mapper = {}

    def register(self, func_name, cls_ptr):
        self.f_resource_map[func_name] = 1
        self.cls_mapper[func_name] = cls_ptr

    def get_resources(self, func_name): 
        return {"num_cpus": self.f_resource_map[func_name]}

    def set_resources(self, f_resource_map):
       self.f_resource_map = f_resource_map

    def update_resources(self, f_resource_map):
        self.set_resources(f_resource_map)
        
        for func_name, cls_ptr in self.cls_mapper.items():
            cls_ptr.update_resources(self.get_resources(func_name))

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

    def optimize(self, func, SLO=0, fastest=True, max_configuration_attemps=1):
        raise NotImplemented

    def storeMetricForCorrentConfig(self, header, metric):
        raise NotImplemented

    def get_loss_function(self, SLO, penaltyForSLOViolation=10, minResources=True, penaltyResourceUsage=10):
        if SLO:
            def loss_SLO(latency, resources):
                
                loss = penaltyForSLOViolation*max(0, latency-SLO)
                if minResources:
                    # Add up all the resources being used
                    resourceSum = penaltyResourceUsage*sum(resources.values())
                    loss += resourceSum
                return -loss
            return loss_SLO
        else:
            def loss_fastest(latency, resources):
                loss = latency
                if minResources:
                    # Add up all the resources being used
                    resourceSum = penaltyResourceUsage*sum(resources.values())
                    loss += resourceSum
                return -loss
            return loss_fastest

    def optimize_function_wrapper(self, func, f_resource_map, loss):
        # print(f"Resource configuration = \n{f_resource_map}")
        self.update_resources(f_resource_map)

        start = time.time()
        func()
        # latency is measured in s
        latency = round((time.time() - start), 2)
        # print(f"Latency={latency}s")
        # return function to minimize
        return loss(latency, f_resource_map)
                 

from bayes_opt import BayesianOptimization
class BayesianResourceManager(BaseResourceManager):
    def optimize(self, func, SLO=None, max_config_attempts=50):
        print(f"Optimizing function {func.__name__}.")
        print(f"Configuration: \n {SLO=}\n {max_config_attempts=}\n")

        # Calling function to remove startups overhead from ray
        # func()

        loss = self.get_loss_function(SLO)
        
        pbounds = {}
        # Set bounds for each remote function CPU
        for f in self.f_resource_map:
            pbounds[f"{f}"] = (0.1, 1)

        optimizer = BayesianOptimization(
            f=lambda **resource_map: self.optimize_function_wrapper(func, resource_map, loss),
            pbounds=pbounds,
            verbose=2,
            random_state=14,
        )

        prob_cpus = [1, 0.5]
        probe_points = len(prob_cpus)
        for probe_point in prob_cpus:
            probe_full_cpu = [probe_point for _ in range(len(self.f_resource_map))]
            optimizer.probe( params=probe_full_cpu, lazy=True )        

        init_points = 10 - probe_points 
        optimizer.maximize(init_points=init_points, n_iter=0)
        bestLoss = optimizer.max
        print(f"Best configuration found randomly = {bestLoss}")
        defaultLoss = optimizer.res[0]

        max_config_attempts -= probe_points + init_points
        n_iter = 1
        MAX_ATTEMPTS = 20
        n_attempts = 0
        for _ in range(max_config_attempts):
            optimizer.maximize(init_points=0, n_iter=n_iter, acq="poi")
            currentBest = optimizer.max
            if currentBest["target"] > bestLoss["target"]:
                bestLoss = currentBest
                n_attempts = 0
                continue
            n_attempts += n_iter
            if n_attempts > MAX_ATTEMPTS:
                print(f"Best configuration was not improved for {n_attempts}. Stopping.")
                break

        print(f"Configuration with 1vcpu = {defaultLoss}")
        print(f"Best configuration found = {bestLoss}")
        print(f"Improvement over 1vcpu = {defaultLoss['target'] - bestLoss['target']}")
        
        # return [[res['params']['nCPU'], i, -res['target']] for i, res in enumerate(optimizer.res)]


class MonotonicDecreaseResourceManager(BaseResourceManager):
    def optimize(self, func, SLO=None, max_config_attempts=50):
        print(f"Optimizing function {func.__name__}.")
        print(f"Configuration: \n {SLO=}\n {max_config_attempts=}\n")

        loss = self.get_loss_function(SLO, minResources=False)

        # TODO: Generalize - may not be a good idea to start all tasks with 1 cpu
        # Start by measuring loss for default configuration with 1vCPU
        best_resource_map = self.f_resource_map.copy()
        for f_resource in best_resource_map:
            best_resource_map[f_resource] = 1
        
        stable_resource_map = best_resource_map.copy()
        ATTEMPTS_PER_CONFIG = 3
        defaultLoss = None
        bestLoss = None
        cpuChange = max(len(stable_resource_map)*3/max_config_attempts, 0.2)
        cpusToExperiment=[1, 0.5, 0.3, 0.2, 0.1]
        testCount = 0
        for f_to_improve in stable_resource_map:
            f_resource_map = stable_resource_map.copy()
            # currentCPU = 1
            for currentCPU in cpusToExperiment:
            # while currentCPU >= 0:
                testCount += 1
                avgLoss = 0
                f_resource_map[f_to_improve] = currentCPU
                for a_i in range(ATTEMPTS_PER_CONFIG):
                    obtainedLoss = self.optimize_function_wrapper(func, f_resource_map, loss)
                    t_str = f"{testCount}_{a_i}"
                    print(f"{t_str:5} | {f_resource_map=} | {obtainedLoss:5.3f}")
                    avgLoss += obtainedLoss
                avgLoss /= ATTEMPTS_PER_CONFIG

                print(f"{testCount:5} | {f_resource_map=} | {avgLoss:5.3f}")
                if defaultLoss == None: 
                    defaultLoss = avgLoss
                    bestLoss = defaultLoss
                if avgLoss > bestLoss:
                    bestLoss = avgLoss
                    best_resource_map[f_to_improve] = f_resource_map[f_to_improve]
                elif 0.90*avgLoss < bestLoss: # Tolerate cpu changes where we only see a 5% on the loss 
                    print(f"Decreasing cpu resulted in worse performance than previous. Accepting previous best config as best possible.")
                    print(f"Best configuration found = {bestLoss}")
                    break

                # currentCPU -= cpuChange
                # currentCPU = round(currentCPU, 2)

        print(f"Configuration with 1vcpu = {round(defaultLoss,2)}")
        print(f"Best configuration found = {round(bestLoss, 2)}")
        print(f"Best resource map found = {best_resource_map}")
        print(f"Improvement over 1vcpu = {round(bestLoss - defaultLoss, 2)}s - Speedup {round(defaultLoss/bestLoss,2)}")
        

resourceOptimizerDict = {
    "Bayesian" : BayesianResourceManager,
    "MonotonicDecrease": MonotonicDecreaseResourceManager,
}

class ResourceManagerController:
    def __init__(self):
        self.rm = None

    def setOptimizer(self, optimizer):
        if optimizer in resourceOptimizerDict:
            resourceManagerCls = resourceOptimizerDict[optimizer]
            self.rm = resourceManagerCls()
        else:
            print(f"Could not find {optimizer} as a valid resource optimizer")
            exit(1)

    def optimize(self, *args, **kwargs):
        self.rm.optimize(*args, **kwargs)

# rManagerC = ResourceManagerController()
# rManager = BayesianResourceManager()
rManager = MonotonicDecreaseResourceManager()

def resourceWrapper(func):
    print(f"Resource wraping function {func._function_name}.")
    class FuncWrapper:
        def __init__(self, rManager):
            self.rManager = rManager
            self.function = func
            self.resources = {}

        def get_resources(self):
            resources = self.rManager.get_resources(func._function_name)
            return resources

        def remote(self, *args, **kwargs):
            # print(f"Func {func._function_name}, NCpus = {self.function._num_cpus}")
            return self.function.remote(*args, **kwargs)

        def update_resources(self, resources):
            self.resources = resources
            # print(f"Updated function resources: {self.resources}")
            # print(f"Updated resources of {func._function_name} to {self.resources}")
            self.function = func.options(**self.resources)
    
    fw = FuncWrapper(rManager)
    rManager.register(func._function_name, fw,)
    return fw