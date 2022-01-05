import copy
import threading
from collections import defaultdict, OrderedDict
import logging
import time
from typing import Any, Dict, List

import botocore
from ray.autoscaler._private.resource_demand_scheduler import NodeID

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import STATUS_SETTING_UP, TAG_RAY_CLUSTER_NAME, TAG_RAY_NODE_NAME, \
    TAG_RAY_LAUNCH_CONFIG, TAG_RAY_NODE_KIND, TAG_RAY_USER_NODE_TYPE
from ray.autoscaler._private.constants import BOTO_MAX_RETRIES, \
    BOTO_CREATE_MAX_RETRIES
from ray.autoscaler._private.log_timer import LogTimer

from ray.autoscaler._private.aws.utils import boto_exception_handler, \
    resource_cache, client_cache
from ray.autoscaler._private.cli_logger import cli_logger, cf
import ray.ray_constants as ray_constants

from ray.autoscaler._private.aws.cloudwatch.cloudwatch_helper import \
    CloudwatchHelper, CLOUDWATCH_AGENT_INSTALLED_AMI_TAG,\
    CLOUDWATCH_AGENT_INSTALLED_TAG

from ray.autoscaler._private.fargate.config import bootstrap_fargate, \
    fillout_resources_fargate

logger = logging.getLogger(__name__)

TAG_BATCH_DELAY = 1

STATUS_STOPPED = "STOPPED"

def make_ec2_client(region, max_retries, aws_credentials=None):
    """Make client, retrying requests up to `max_retries`."""
    aws_credentials = aws_credentials or {}
    return resource_cache("ec2", region, max_retries, **aws_credentials)

class FargateNodeProvider(NodeProvider):
    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        self.cache_stopped_nodes = provider_config.get("cache_stopped_nodes",
                                                       False)
        aws_credentials = provider_config.get("aws_credentials")

        self.ecs = client_cache('ecs', provider_config["region"])
        self.rgTask = client_cache('resourcegroupstaggingapi', provider_config["region"])

        self.ec2 = make_ec2_client(
            region=provider_config["region"],
            max_retries=BOTO_MAX_RETRIES,
            aws_credentials=aws_credentials)
        self.ec2_fail_fast = make_ec2_client(
            region=provider_config["region"],
            max_retries=0,
            aws_credentials=aws_credentials)

        # Tags that we believe to actually be on EC2.
        self.tag_cache = {}
        # Tags that we will soon upload.
        self.tag_cache_pending = defaultdict(dict)
        # Number of threads waiting for a batched tag update.
        self.batch_thread_count = 0
        self.batch_update_done = threading.Event()
        self.batch_update_done.set()
        self.ready_for_new_batch = threading.Event()
        self.ready_for_new_batch.set()
        self.tag_cache_lock = threading.Lock()
        self.count_lock = threading.Lock()

        # Cache of task objects from the last tasks() call. This avoids
        # excessive DescribeTasks requests.
        self.cached_tasks = {}

    @staticmethod
    def fillout_available_node_types_resources(cluster_config):
        """Fills out missing "resources" field for available_node_types."""
        return fillout_resources_fargate(cluster_config)

    @staticmethod
    def bootstrap_config(cluster_config):
        return bootstrap_fargate(cluster_config)

    def convert_to_aws_tag(self, tag_filters):
        filters = [{"Key": TAG_RAY_CLUSTER_NAME, "Values": [self.cluster_name]}]
        for k, v in tag_filters.items():
            filters.append({
                "Key": k,
                "Values": [v],
            })
        return filters

    def non_terminated_nodes(self, tag_filters):
        filters = self.convert_to_aws_tag(tag_filters)

        with boto_exception_handler(
                "Failed to fetch running tasks from AWS."):
            tasks = self._get_tasks_w_tags(filters)

        # Populate the tag cache with initial information if necessary
        for task in tasks:
            if task["taskArn"] in self.tag_cache:
                continue
            self.tag_cache[task["taskArn"]] = task["Tags"]

        self.cached_tasks = {task["taskArn"]: task for task in tasks}
        return [task["taskArn"] for task in tasks]

    def is_running(self, node_id):
        task = self._get_cached_task(node_id)
        return task["lastStatus"] == "RUNNING"

    def is_terminated(self, node_id):
        task = self._get_cached_task(node_id)
        if task:
            status = task["lastStatus"]
            return status in ["DEACTIVATING", "STOPPING", "DEPROVISIONING", "STOPPED"]
        else:
            return True


    def _get_task_IP(self, task, internal_IP=True):
        nic = None
        for attachment in task["attachments"]:
            if attachment["type"] == "ElasticNetworkInterface":
                nic = attachment
        assert nic != None, "Could not find ElasticNetworkInterface of task with arn: {}".format(task["taskArn"])
        # Expected to return None if IP cannot yet be found
        if  nic["Status"] in ["PRECREATED", "CREATED", "ATTACHING"]:
            return None

        if internal_IP:
            for detail in nic["details"]:
                if internal_IP and detail["name"] == "privateIPv4Address":
                    return detail["value"]
        else:
            for detail in nic["details"]:
                nid = None
                if detail["name"] == "networkInterfaceId":
                    nid = detail["value"]
            interface = self.ec2.describe_network_interfaces(NetworkInterfaceIds=[nid])['NetworkInterfaces'][0]
            public_IP = interface['Association']['PublicIp']
            return public_IP

    # Expected to return None if ip is not yet available
    def external_ip(self, node_id):
        task = self._get_cached_task(node_id)
        assert task != None, "Trying to get external IP of nonexistent task" 
        return self._get_task_IP(task, internal_IP=False)

    # Expected to return None if ip is not yet available
    def internal_ip(self, node_id):
        task = self._get_cached_task(node_id)
        assert task != None, "Trying to get external IP of nonexistent task" 
        return self._get_task_IP(task, internal_IP=True)

    def set_node_tags(self, node_id, tags):
        # Multiple threads can call this function but only one commits changes
        # To do that one of the threads is designated the batching thread and 
        # sleeps for a while before commit changes
        is_batching_thread = False
        with self.tag_cache_lock:
            if not self.tag_cache_pending:
                is_batching_thread = True
                # Wait for threads in the last batch to exit
                self.ready_for_new_batch.wait()
                self.ready_for_new_batch.clear()
                self.batch_update_done.clear()
            self.tag_cache_pending[node_id].update(tags)

        if is_batching_thread:
            time.sleep(TAG_BATCH_DELAY)
            with self.tag_cache_lock:
                self._update_node_tags()
                self.batch_update_done.set()

        with self.count_lock:
            self.batch_thread_count += 1
        self.batch_update_done.wait()

        with self.count_lock:
            self.batch_thread_count -= 1
            if self.batch_thread_count == 0:
                self.ready_for_new_batch.set()

    def _update_node_tags(self):
        batch_updates = defaultdict(list)

        for taskArn, tags in self.tag_cache_pending.items():
            for tagK, tagV in tags.items: 
                batch_updates[taskArn].append({"key": tagK, "value": tagV})
            self.tag_cache[taskArn].update(tags)

        self.tag_cache_pending = defaultdict(dict)

        for taskArn, tags in batch_updates.items():
            m = "Set tags {} on taskArn {}".format(tags, taskArn)
            with LogTimer("FargateNodeProvider: {}".format(m)):
                self.ecs.tag_resource(
                    resourceArn=taskArn,
                    Tags=tags,
                )

    def _get_tasks_w_tags(self, tag_filters, w_details=True):
        """ Returns task arns of tasks matching tag_filters that are either on a pending or ready state
        """
        tasks_arn_tags = self.rgTask.get_resources(ResourceTypeFilters=["ecs:task"], TagFilters=tag_filters)["ResourceTagMappingList"]

        if w_details:
            tasks_w_details = self._get_tasks_details([t["ResourceARN"] for t in tasks_arn_tags])
            return tasks_w_details
        else:
            return tasks_arn_tags

    def _get_tasks_definition_w_tags(self, tag_filters):
        """ Returns task arns of tasks matching tag_filters that are either on a pending or ready state
        """
        tasks_arn_tags = self.rgTask.get_resources(ResourceTypeFilters=["ecs:task-definition"], TagFilters=tag_filters)["ResourceTagMappingList"]
        return tasks_arn_tags

    def _get_tasks_details(self, task_arns):
        if len(task_arns) == 0:
            return []
        task_details = self.ecs.describe_tasks(cluster=self.cluster_name,
        tasks=task_arns,
        include=['TAGS'])["tasks"]
        return task_details

    # Note: node id represents the task arn
    def node_tags(self, node_id):
        with self.tag_cache_lock:
            d1 = self.tag_cache[node_id]
            d2 = self.tag_cache_pending.get(node_id, {})
            return dict(d1, **d2)

    
    def create_node(self, node_config, tags, count) -> Dict[str, Any]:
        """Creates tasks.

        Returns dict mapping task arn to task object for the created tasks.
        """
        # sort tags by key to support deterministic unit test stubbing
        tags = OrderedDict(sorted(copy.deepcopy(tags).items()))

        # Check if there's already a task definition created for the request
        taskDef = self._get_tasks_definition_w_tags({"Name": TAG_RAY_LAUNCH_CONFIG, 
                                           "Values": [tags[TAG_RAY_LAUNCH_CONFIG]]})
        
        all_created_nodes = {}
        if not taskDef:
            taskDef = self._create_task_definition(node_config, tags)

        assert len(taskDef) == 1, print("Found multiple task definitions with the same configuration")
        taskDefArn = taskDef[0]["ResourceArn"]

        # Try to reuse previously stopped tasks with compatible configs
        if self.cache_stopped_nodes:
            reused_nodes_dict = self.reuse_nodes(tags, count)
            count -= len(reused_nodes_dict)
            all_created_nodes = reused_nodes_dict
        if count:
            created_tasks_dict = self._run_task(taskDefArn, count)
            all_created_nodes.update(created_tasks_dict)

        return all_created_nodes

        
    def _create_task_definition(self, node_config, tags):
        task_def_spec = node_config["spec"]
        task_def_spec["tags"] = self._get_tasks_w_tags(tags)
        self.ecs.register_task_definition(**task_def_spec)

    # TODO We can override task config! If we do it, don't forget to override the tag with config hash
    def _run_task(self, taskDefArn, node_config, count):
        spawnedTasks = []
        cli_logger_tags = {}   

        subnet_idx = 0
        subnet_ids = [node_config["SubnetIds"][0]] # TODO limiting this to one subnet for now
        sg_id = node_config["SecurityGroupIds"][0]

        max_tries = max(BOTO_CREATE_MAX_RETRIES, len(subnet_ids))
        missingCount = count
        for attempt in range(1, max_tries + 1):
            try:
                subnet_id = subnet_ids[subnet_idx % len(subnet_ids)]
                cli_logger_tags["subnet_id"] = subnet_id

                while True:
                    # Apperently aws only allows us to spawn 10 tasks at once
                    # TODO handle case where subnet is full?
                    toSpawn = min(10, missingCount)
                    spawnedTasks.append(self.ecs.run_task(
                        taskDefinition=taskDefArn,
                        cluster=self.cluster_name,
                        networkConfiguration={
                            'awsvpcConfiguration': {
                                'subnets': [ subnet_id ],
                                'securityGroups': [ sg_id ],
                            }
                        },
                        # placementStrategy=[{
                        #     "type": "binpack",
                        #     "field": "cpu", # also supports mem
                        # }],
                        count=toSpawn
                    )["tasks"])
                    missingCount -= toSpawn
                    if missingCount == 0:
                        break

                with cli_logger.group(
                        "Launched {} tasks", count, _tags=cli_logger_tags):
                    for task in spawnedTasks:
                            cli_logger.print(
                            "Launched task {}",
                            task["taskArn"],
                            _tags=dict(
                                state=task["lastStatus"]))
                break

            except botocore.exceptions.ClientError as exc:
                if attempt == max_tries:
                    cli_logger.abort(
                        "Failed to launch instances. Max attempts exceeded.",
                        exc=exc,
                    )
                else:
                    cli_logger.warning(
                        "run_task: Attempt failed with {}, retrying.",
                        exc)

                # Launch failure may be due to instance type availability in
                # the given AZ
                subnet_idx += 1
            
        spawnedTasksDict = {spawnedTasks["taskArn"]: task for task in spawnedTasks}
        return spawnedTasksDict

    # def _get_task_def(self, task_definition_family):
    #     try:
    #         task_def = self.ecs.describe_task_definition(taskDefinition=task_definition_family)
    #         return task_def
    #     except botocore.exceptions.ClientError as exc:
    #         if exc.response.get("Error", {}).get("Message") == "Unable to describe task definition.":
    #             return None
    #         else:
    #             cli_logger.print(
    #                 "Failed to fetch Task Definition data for {} from AWS.",
    #                 cf.bold(task_definition_family))
    #             raise exc


    def reuse_nodes(self, tags, count):
        # TODO(ekl) this is breaking the abstraction boundary a little by
        # peeking into the tag set.
        filters = self.convert_to_aws_tag(tags)
        filters["TAG_RAY_NODE_STATUS"] = STATUS_STOPPED

        reuse_tasks = self._get_tasks_w_tags(filters)[:count]
        reuse_task_arns = [task["taskArn"] for task in reuse_tasks]
        reused_nodes_dict = {task["taskArn"]: self.get_task_details(task) for task in reuse_tasks}
        if reuse_tasks:
            cli_logger.print(
                # todo: handle plural vs singular?
                "Reusing nodes {}. "
                "To disable reuse, set `cache_stopped_nodes: False` "
                "under `provider` in the cluster configuration.",
                cli_logger.render_list(reuse_task_arns))

            # todo: timed?
            with cli_logger.group("Stopping instances to reuse"):
                for task in reuse_tasks:
                    self.tag_cache[task["taskArn"]] = task["tags"]
                    # TODO Handle stopping case

            # self.ec2.meta.client.start_instances(InstanceIds=reuse_node_ids)
            # Use task
            for node_id in reuse_task_arns:
                self.set_node_tags(node_id, tags)
        return reused_nodes_dict

    def _get_task(self, node_id):
        """Refresh and get info for this node, updating the cache."""
        self.non_terminated_nodes({})  # Side effect: updates cache

        if node_id in self.cached_nodes:
            return self.cached_nodes[node_id]

        task_details = self._get_tasks_details([node_id])["tasks"]
        # Tasks are deleted after a while, so we might not be able to find this one
        if len(task_details["tasks"]) == 1:
            return task_details["tasks"][0]
        else:
            # Make sure the reason for failure is due to missing arn
            failure = task_details["failures"][0]
            assert failure["arn"] == node_id and failure["reason"] == "MISSING", \
                "Problem fetching task with arn {}".format(node_id)
            return None

    def _get_cached_task(self, node_id):
        """Return task info from cache if possible, otherwise fetches it."""
        if node_id in self.cached_nodes:
            return self.cached_nodes[node_id]

        return self._get_task(node_id)


    def terminate_node(self, node_id):
        task = self._get_cached_task(node_id)
        
        # TODO this can end badly if the task is not found
        self.ecs.stop_task(
            cluster=self.cluster_name,
            task=node_id,
            reason='Ray terminated'
        )

        # TODO (Alex): We are leaking the tag cache here. Naively, we would
        # want to just remove the cache entry here, but terminating can be
        # asyncrhonous or error, which would result in a use after free error.
        # If this leak becomes bad, we can garbage collect the tag cache when
        # the node cache is updated.
        pass