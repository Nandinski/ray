
from .exploration import BaseExplorationStrategy
import logging
log = logging.getLogger(__name__)

class MonotonicDecreaseExplorationStrategy(BaseExplorationStrategy):
    def explore(self, configs_to_test):
        # TODO: Generalize - may not be a good idea to start all tasks with 1 cpu
        # Start by measuring loss for default configuration with 1vCPU
        best_resource_map = self.f_resource_map.copy()
        for f_resource in best_resource_map:
            best_resource_map[f_resource] = 0.1
        
        # stable_resource_map = best_resource_map.copy()
        ATTEMPTS_PER_CONFIG = 3
        defaultLoss = None
        bestLoss = None
        cpuChange = max(len(best_resource_map)*3/max_config_attempts, 0.2)
        cpusToExperiment=[1, 0.5, 0.3, 0.2, 0.1]
        # cpusToExperiment=[1, 0.1]
        cpusToExperiment=[0.1]
        testCount = 0
        for f_to_improve in best_resource_map:
            f_resource_map = best_resource_map.copy()
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

                if 1 in cpusToExperiment:
                    cpusToExperiment = cpusToExperiment[1:]

                # currentCPU -= cpuChange
                # currentCPU = round(currentCPU, 2)
      