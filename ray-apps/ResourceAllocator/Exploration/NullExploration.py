from .exploration import BaseExplorationStrategy
import logging
log = logging.getLogger(__name__)

class NullExplorationStrategy(BaseExplorationStrategy):
    def explore(self, configs_to_test):
        cluster_config = self.initial_cluster_config
        f_resource_map = self.initial_f_resource_map

        # Experimenting with the initial configuration
        log.info(f"Experimenting with the initial configuration")
        self.explore_function_wrapper(cluster_config, f_resource_map)