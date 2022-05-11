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