import copy
import threading
from collections import defaultdict, OrderedDict
import logging
import time
from typing import Any, Dict, List

import botocore

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import STATUS_SETTING_UP, TAG_RAY_CLUSTER_NAME, TAG_RAY_NODE_NAME, \
    TAG_RAY_LAUNCH_CONFIG, TAG_RAY_NODE_KIND, TAG_RAY_USER_NODE_TYPE
from ray.autoscaler._private.constants import BOTO_MAX_RETRIES, \
    BOTO_CREATE_MAX_RETRIES
from ray.autoscaler._private.aws.config import bootstrap_aws
from ray.autoscaler._private.log_timer import LogTimer

from ray.autoscaler._private.aws.utils import boto_exception_handler, \
    resource_cache, client_cache
from ray.autoscaler._private.cli_logger import cli_logger, cf
import ray.ray_constants as ray_constants

from ray.autoscaler._private.aws.cloudwatch.cloudwatch_helper import \
    CloudwatchHelper, CLOUDWATCH_AGENT_INSTALLED_AMI_TAG,\
    CLOUDWATCH_AGENT_INSTALLED_TAG

from ray.autoscaler._private.fargate.config import _get_task_def, bootstrap_fargate, \
    fillout_resources_fargate

logger = logging.getLogger(__name__)

TAG_BATCH_DELAY = 1

STATUS_STOPPED = "stopped"

class FargateNodeProvider(NodeProvider):
    max_terminate_nodes = 1000

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

    # @staticmethod
    # def fillout_available_node_types_resources(cluster_config):
    #     """Fills out missing "resources" field for available_node_types."""
    #     return fillout_resources_fargate(cluster_config)

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

    def _get_tasks_details(self, task_arn):
        task_details = self.ecsClient.describe_tasks(cluster=self.cluster_name,
        tasks=task_arn,
        include=['TAGS'])
        return task_details["tasks"]

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
            created_tasks_dict = self.run_task(taskDefArn, count)
            all_created_nodes.update(created_tasks_dict)

        return all_created_nodes

        
    def _create_task_definition(self, node_config, tags):
        task_def_spec = node_config["spec"]
        task_def_spec["tags"] = self._get_tasks_w_tags(tags)
        self.ecsClient.register_task_definition(**task_def_spec)

    # TODO We can override task config! If we do it, don't forget to override the tag with config hash
    def run_task(self, taskDefArn, node_config, count):
        # Apperently aws only allows us to spawn 10 tasks at once
        missingCount = count
        spawnedTasks = []
        cli_logger_tags = {}   

        subnet_id = node_config["SubnetIds"][0]
        sg_id = node_config["SecurityGroupIds"][0]

        cli_logger_tags["subnet_id"] = subnet_id
        with cli_logger.group(
                        "Launched {} tasks", count, _tags=cli_logger_tags):
            while True:
                # TODO handle case where subnet is full?
                toSpawn = min(10, missingCount)
                spawnedTasks.append(self.ecsClient.run_task(
                    taskDefinition=taskDefArn,
                    cluster=self.cluster_name,
                    networkConfiguration={
                        'awsvpcConfiguration': {
                            'subnets': [
                                node_config["SubnetIds"][0],
                            ],
                            'securityGroups': [
                                node_config["SecurityGroupIds"][0],
                            ],
                        }
                    },
                    count=toSpawn
                )["tasks"])
                missingCount -= toSpawn
                if missingCount == 0:
                    break
            
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

    
    def _create_node(self, node_config, tags, count):
        created_nodes_dict = {}
        conf = node_config.copy()

        tags = self.convert_to_aws_tag(tags)
        
        # SubnetIds is not a real config key: we must resolve to a
        # single SubnetId before invoking the AWS API.
        subnet_ids = conf.pop("SubnetIds")

        # update config with min/max node counts and tag specs
        conf.update({
            "MinCount": 1,
            "MaxCount": count,
            "TagSpecifications": tags
        })

        # Try to always launch in the first listed subnet.
        subnet_idx = 0
        cli_logger_tags = {}
        # NOTE: This ensures that we try ALL availability zones before
        # throwing an error.
        max_tries = max(BOTO_CREATE_MAX_RETRIES, len(subnet_ids))
        for attempt in range(1, max_tries + 1):
            try:
                if "NetworkInterfaces" in conf:
                    net_ifs = conf["NetworkInterfaces"]
                    # remove security group IDs previously copied from network
                    # interfaces (create_instances call fails otherwise)
                    conf.pop("SecurityGroupIds", None)
                    cli_logger_tags["network_interfaces"] = str(net_ifs)
                else:
                    subnet_id = subnet_ids[subnet_idx % len(subnet_ids)]
                    conf["SubnetId"] = subnet_id
                    cli_logger_tags["subnet_id"] = subnet_id

                # Continue from here !!!!
                created = self.ec2_fail_fast.create_instances(**conf)
                created_nodes_dict = {n.id: n for n in created}

                # todo: timed?
                # todo: handle plurality?
                cli_logger_tags["subnet_id"] = subnet_id

                with cli_logger.group(
                        "Launched {} nodes", count, _tags=cli_logger_tags):
                    for instance in created:
                        # NOTE(maximsmol): This is needed for mocking
                        # boto3 for tests. This is likely a bug in moto
                        # but AWS docs don't seem to say.
                        # You can patch moto/ec2/responses/instances.py
                        # to fix this (add <stateReason> to EC2_RUN_INSTANCES)

                        # The correct value is technically
                        # {"code": "0", "Message": "pending"}
                        state_reason = instance.state_reason or {
                            "Message": "pending"
                        }

                        cli_logger.print(
                            "Launched instance {}",
                            instance.instance_id,
                            _tags=dict(
                                state=instance.state["Name"],
                                info=state_reason["Message"]))
                break
            except botocore.exceptions.ClientError as exc:
                if attempt == max_tries:
                    cli_logger.abort(
                        "Failed to launch instances. Max attempts exceeded.",
                        exc=exc,
                    )
                else:
                    cli_logger.warning(
                        "create_instances: Attempt failed with {}, retrying.",
                        exc)

                # Launch failure may be due to instance type availability in
                # the given AZ
                subnet_idx += 1

        return created_nodes_dict