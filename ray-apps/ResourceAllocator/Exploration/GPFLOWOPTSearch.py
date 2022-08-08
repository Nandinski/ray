from .exploration import BaseExplorationStrategy
from ..cluster_manager import make_cluster_config
import logging
log = logging.getLogger(__name__)

import optuna

RANDOM_SEED = 14
class GPFLOWOPTSearchStrategy(BaseExplorationStrategy):
    def explore(self, configs_to_test, attempts_per_config, per_func_pareto, cpu_step_size=4):
        def objective(trial):
            worker_count = trial.suggest_int("worker_count", *self.c_range_to_explore.worker_count_range)
            cpu_per_worker = trial.suggest_int("cpu_per_worker", *self.c_range_to_explore.cpu_per_worker_range)
            cluster_config = make_cluster_config(worker_count, cpu_per_worker)
            
            f_resource_map = {}
            cpu_increment = (self.c_range_to_explore.task_cpu_range[1] - self.c_range_to_explore.task_cpu_range[0]) / (cpu_step_size-1)
            for function_name in self.initial_f_resource_map.keys():
                f_resource_map[function_name] = round(trial.suggest_discrete_uniform(f"{function_name}_cpu", *self.c_range_to_explore.task_cpu_range, cpu_increment),1)

            loss, cost = self.explore_config(cluster_config, f_resource_map, attempts_per_config)
            return loss, cost


        cluster_config = self.initial_cluster_config
        initial_cluster_config_dict = {"worker_count": cluster_config["num_workers"], "cpu_per_worker": cluster_config["cpu_per_worker"]}
        f_resource_map = self.initial_f_resource_map
        f_resource_map_dict = {f"{function_name}_cpu": f_resource_map[function_name] for function_name in f_resource_map}
        initial_config = dict(initial_cluster_config_dict, **f_resource_map_dict)

        study = optuna.multi_objective.create_study(directions=["minimize", "minimize"])
        # Enqueue initial grid search
        study.enqueue_trial(initial_config)

        try:
            study.optimize(objective, timeout=60*5, callbacks=[])
        except EarlyStoppingExceeded:
            print(f'EarlyStopping Exceeded: No new best scores on iters {OPTUNA_EARLY_STOPING}')


# Taken from: https://github.com/optuna/optuna/issues/1001
OPTUNA_EARLY_STOPING = 10
class EarlyStoppingExceeded(optuna.exceptions.OptunaError):
    early_stop = OPTUNA_EARLY_STOPING
    early_stop_count = 0
    best_trials = None

def registered_improvement(study_best_trials, recorded_best_trials):
    for s_t in study_best_trials:
        new_improvement = True
        for r_t in recorded_best_trials:
            if s_t.values == r_t.values:
                new_improvement = False
        
        if new_improvement:
            return True
    
    return False


def early_stopping_opt(study, trial):
    if EarlyStoppingExceeded.best_trials == None:
      EarlyStoppingExceeded.best_trials = study.best_trials
      print(study.best_trials)

    if registered_improvement(study.best_trials, EarlyStoppingExceeded.best_trials):
        EarlyStoppingExceeded.best_trials = study.best_trials
        EarlyStoppingExceeded.early_stop_count = 0
    else:
      if EarlyStoppingExceeded.early_stop_count > EarlyStoppingExceeded.early_stop:
            EarlyStoppingExceeded.early_stop_count = 0
            EarlyStoppingExceeded.best_trials = None
            raise EarlyStoppingExceeded()
      else:
            EarlyStoppingExceeded.early_stop_count=EarlyStoppingExceeded.early_stop_count+1
    #print(f'EarlyStop counter: {EarlyStoppingExceeded.early_stop_count}, Best score: {study.best_value} and {EarlyStoppingExceeded.best_score}')
    return
    