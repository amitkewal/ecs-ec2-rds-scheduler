######################################################################################################################
#  Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                           #
#                                                                                                                    #
#  Licensed under the Apache License Version 2.0 (the "License"). You may not use this file except in compliance     #
#  with the License. A copy of the License is located at                                                             #
#                                                                                                                    #
#      http://www.apache.org/licenses/                                                                               #
#                                                                                                                    #
#  or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES #
#  OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions    #
#  and limitations under the License.                                                                                #
######################################################################################################################

import copy
import re

import schedulers
import re
import copy

from boto_retry import get_client_with_retries
from configuration.instance_schedule import InstanceSchedule
from configuration.running_period import RunningPeriod
from configuration.scheduler_config_builder import SchedulerConfigBuilder
from configuration.setbuilders.weekday_setbuilder import WeekdaySetBuilder

RESTRICTED_ECS_TAG_VALUE_SET_CHARACTERS = r"[^a-zA-Z0-9\s_\.:+/=\\@-]"

ERR_STARTING_INSTANCE = "Error starting rds {} {} ({})"
ERR_STOPPING_INSTANCE = "Error stopping rds {} {}, ({})"
ERR_DELETING_SNAPSHOT = "Error deleting snapshot {}"

INF_FETCHED_CLUSTERS = "Number of fetched ecs clusters is {}, number of instances in a schedulable state is {}"

INF_ADD_TAGS = "Adding {} tags {} to instance {}"
INF_DELETE_SNAPSHOT = "Deleted previous snapshot {}"
INF_FETCHED = "Number of fetched rds {} is {}, number of schedulable  resources is {}"
INF_FETCHING_RESOURCES = "Fetching rds {} for account {} in region {}"
INF_REMOVE_KEYS = "Removing {} key(s) {} from instance {}"
INF_STOPPED_RESOURCE = "Stopped rds {} \"{}\""

DEBUG_READ_REPLICA = "Can not schedule rds instance \"{}\" because it is a read replica of instance {}"
DEBUG_READ_REPLICA_SOURCE = "Can not schedule rds instance \"{}\" because it is the source for read copy instance(s) {}"
DEBUG_SKIPPING_INSTANCE = "Skipping rds {} {} because it is not in a start or stop-able state ({})"
DEBUG_WITHOUT_SCHEDULE = "Skipping rds {} {} without schedule"
DEBUG_SELECTED = "Selected rds instance {} in state ({}) for schedule {}"
DEBUG_NO_SCHEDULE_TAG = "Instance {} has no schedule tag named {}"

WARN_TAGGING_STARTED = "Error setting start or stop tags to started instance {}, ({})"
WARN_TAGGING_STOPPED = "Error setting start or stop tags to stopped instance {}, ({})"
WARN_RDS_TAG_VALUE = "Tag value \"{}\" for tag \"{}\" changed to \"{}\" because it did contain characters that are not allowed " \
                    "in RDS tag values. The value can only contain only the set of Unicode letters, digits, " \
                     "white-space, '_', '.', '/', '=', '+', '-'"

MAINTENANCE_SCHEDULE_NAME = "RDS preferred Maintenance Window Schedule"
MAINTENANCE_PERIOD_NAME = "RDS preferred Maintenance Window Period"


class EcsService:
    ECS_STATE_AVAILABLE = "ACTIVE"
    ECS_STATE_STOPPED = "stopped"

    ECS_SCHEDULABLE_STATES = {ECS_STATE_AVAILABLE, ECS_STATE_STOPPED}

    def __init__(self):
        self.service_name = "ecs"
        self.allow_resize = False
        self._instance_tags = None

        self._context = None
        self._session = None
        self._region = None
        self._account = None
        self._logger = None
        self._tagname = None
        self._stack_name = None
        self._config = None

    def _init_scheduler(self, args):
        
        """
        Ini.
        tializes common parameters
        :param args: action parameters
        :return:
        """
        self._account = args.get(schedulers.PARAM_ACCOUNT)
        self._context = args.get(schedulers.PARAM_CONTEXT)
        self._logger = args.get(schedulers.PARAM_LOGGER)
        self._region = args.get(schedulers.PARAM_REGION)
        self._stack_name = args.get(schedulers.PARAM_STACK)
        self._session = args.get(schedulers.PARAM_SESSION)
        self._tagname = args.get(schedulers.PARAM_CONFIG).tag_name
        self._config = args.get(schedulers.PARAM_CONFIG)
        self._instance_tags = None

    @property
    def rds_resource_tags(self):

        if self._instance_tags is None:
            tag_client = get_client_with_retries("resourcegroupstaggingapi",
                                                 methods=["get_resources"],
                                                 session=self._session,
                                                 context=self._context,
                                                 region=self._region)

            args = {
                "TagFilters": [{"key": self._tagname}],
                "ResourcesPerPage": 50,
                "ResourceTypeFilters": ["rds:db", "rds:cluster"]
            }

            self._instance_tags = {}

            while True:

                resp = tag_client.get_resources_with_retries(**args)

                for resource in resp.get("ResourceTagMappingList", []):
                    self._instance_tags[resource["ResourceARN"]] = {tag["key"]: tag["value"]
                                                                    for tag in resource.get("tags", {})
                                                                    if tag["key"] in ["Name", self._tagname]}

                if resp.get("PaginationToken", "") != "":
                    args["PaginationToken"] = resp["PaginationToken"]
                else:
                    break

        return self._instance_tags

    @staticmethod
    def build_schedule_from_maintenance_window(period_str):
        """
        Builds a Instance running schedule based on an RDS preferred maintenance windows string in format ddd:hh:mm-ddd:hh:mm
        :param period_str: rds maintenance windows string
        :return: Instance running schedule with timezone UTC
        """

        # get elements of period
        start_string, stop_string = period_str.split("-")
        start_day_string, start_hhmm_string = start_string.split(":", 1)
        stop_day_string, stop_hhmm_string = stop_string.split(":", 1)

        # weekday set builder
        weekdays_builder = WeekdaySetBuilder()

        start_weekday = weekdays_builder.build(start_day_string)
        start_time = SchedulerConfigBuilder.get_time_from_string(start_hhmm_string)
        end_time = SchedulerConfigBuilder.get_time_from_string(stop_hhmm_string)

        # windows with now day overlap, can do with one period for schedule
        if start_day_string == stop_day_string:
            periods = [
                {
                    "period": RunningPeriod(name=MAINTENANCE_PERIOD_NAME,
                                            begintime=start_time,
                                            endtime=end_time,
                                            weekdays=start_weekday)
                }]
        else:
            # window with day overlap, need two periods for schedule
            end_time_day1 = SchedulerConfigBuilder.get_time_from_string("23:59")
            begin_time_day2 = SchedulerConfigBuilder.get_time_from_string("00:00")
            stop_weekday = weekdays_builder.build(stop_day_string)
            periods = [
                {
                    "period": RunningPeriod(name=MAINTENANCE_PERIOD_NAME + "-{}".format(start_day_string),
                                            begintime=start_time,
                                            endtime=end_time_day1,
                                            weekdays=start_weekday),
                    "instancetype": None
                },
                {
                    "period": RunningPeriod(name=MAINTENANCE_PERIOD_NAME + "-{}".format(stop_day_string),
                                            begintime=begin_time_day2,
                                            endtime=end_time,
                                            weekdays=stop_weekday),
                    "instancetype": None
                }]

        # create schedule with period(s) and timezone UTC
        schedule = InstanceSchedule(name=MAINTENANCE_SCHEDULE_NAME, periods=periods, timezone="UTC", enforced=True)

        return schedule

    
    # get services and handle paging
    def get_schedulable_instances(self, kwargs):
        self._init_scheduler(kwargs)
        self._session = kwargs[schedulers.PARAM_SESSION]
        context = kwargs[schedulers.PARAM_CONTEXT]
        region = kwargs[schedulers.PARAM_REGION]
        account = kwargs[schedulers.PARAM_ACCOUNT]
        self._logger = kwargs[schedulers.PARAM_LOGGER]
        tagname = kwargs[schedulers.PARAM_CONFIG].tag_name
        config = kwargs[schedulers.PARAM_CONFIG]
        mixed_clusters = []
        mixed_services = []
        services = []
        mixed_clusters =  self.get_schedulable_ecs_clusters(context, region)

        if mixed_clusters['clusters_without_schedule']:
            for cluster in mixed_clusters['clusters_without_schedule']:
                services = self.get_schedulable_ecs_services(cluster, context, region)
                # services = mixed_services['services_with_schedule']
                # for no_tag_service in mixed_services['services_without_schedule']:
                #     no_tag_service['ecs_service_force_stop'] = True
                #     services = services.extend(no_tag_service)
                
                    

        if mixed_clusters['clusters_with_schedule']:
            for cluster in mixed_clusters['clusters_with_schedule'] :
                #fetch all mixed_services
                services.extend(self.get_all_services(cluster, context, region))
            

        return services
        
    def get_schedulable_ecs_clusters(self, context, region):
        
        client = get_client_with_retries("ecs", ["list_clusters"], context=context, session=self._session,
                                         region=region)
        
        args = {}
        clusters = []
        done = False

        while not done:
            ecs_cluster_resp = client.list_clusters_with_retries(**args)
            
            clusters.extend(ecs_cluster_resp['clusterArns'])
            
            if "NextToken" in ecs_cluster_resp:
                args["NextToken"] = ecs_cluster_resp["NextToken"]
            else:
                done = True
        
        mixed_clusters = self._validate_cluster_tag_values(clusters, context, region)
        # self._logger.info(INF_FETCHED_CLUSTERS, , len(clusters), mixed_clusters)
        return mixed_clusters
    
    
    def _validate_cluster_tag_values(self, clusters, context, region):
        client = get_client_with_retries("ecs", ["describe_clusters"], context=context, session=self._session,
                                         region=region)
        args = {"clusters":clusters,"include":['TAGS']}
        mixed_clusters = { "clusters_with_schedule" : [], "clusters_without_schedule" : []}
        done = False
        
        while not done:
            ecs_response = client.describe_clusters_with_retries(**args)
            for cluster_data in ecs_response['clusters']:
                is_cluster_scheduled = self._filter_resource_based_on_tags(cluster_data)
                if is_cluster_scheduled:
                    mixed_clusters['clusters_with_schedule'].append(cluster_data.get('clusterName'))
                else :
                    mixed_clusters['clusters_without_schedule'].append(cluster_data.get('clusterName'))
            if "NextToken" in ecs_response:
                args["NextToken"] = ecs_response["NextToken"]
            else:
                done = True
        
        return mixed_clusters
            
    def get_tags(self, inst):
        return { tag["key"]: tag["value"] for tag in inst["tags"]} if "tags" in inst else {}
    
    def _filter_resource_based_on_tags(self, ecs_resource):
        tags = self.get_tags(ecs_resource)
        name = tags.get(self._tagname)
        if name is None:
            return False
        return True
    
    def get_schedulable_ecs_services(self, cluster, context, region):
   
        client = get_client_with_retries("ecs", ["list_services"], context=context, session=self._session,
                                         region=region)
        
        args = {"cluster": cluster}
        services = []
        done = False
        #aws ecs describe-clusters --cluster test-cluster --include TAGS
        
        while not done:
            ecs_service_resp = client.list_services_with_retries(**args)
            
            services.extend(ecs_service_resp['serviceArns'])
            
            if "NextToken" in ecs_service_resp:
                args["NextToken"] = ecs_service_resp["NextToken"]
            else:
                done = True

        all_services = self._validate_service_tag_values(services, cluster, context, region)

        # self._logger.info(INF_FETCHED_CLUSTERS, , len(clusters), clusters_with_schedule)
        return all_services
    
    def _validate_service_tag_values(self, services, cluster, context, region):
        client = get_client_with_retries("ecs", ["describe_services"], context=context, session=self._session,
                                         region=region)
        args = {"cluster": cluster ,"services":services, "include":['TAGS']}
        mixed_services = { "services_with_schedule" : [], "services_without_schedule" : []}
        all_services = []
        # no_tag_services = []
        done = False
        # tag_name = "Schedule"
        while not done:
            ecs_response = client.describe_services_with_retries(**args)
            for service_data in ecs_response['services']:
                is_service_scheduled = self._filter_resource_based_on_tags(service_data)
                # data = self._select_resource_data(service_data)
                if is_service_scheduled:
                    data = self._select_resource_data(service_data)
                    all_services.append(data)
                else:
                    data = self._select_resource_data(service_data, True)
                    all_services.append(data)

            if "NextToken" in ecs_response:
                args["NextToken"] = ecs_response["NextToken"]
            else:
                done = True
        
        return all_services
    
    
    def get_all_services(self, cluster, context, region):
        #fetch service lists
        
        client = get_client_with_retries("ecs", ["list_services"], context=context, session=self._session,
                                         region=region)
        
        args = {"cluster": cluster}
        services = []
        done = False
        
        while not done:
            ecs_service_resp = client.list_services_with_retries(**args)
            
            services.extend(ecs_service_resp['serviceArns'])
            
            if "NextToken" in ecs_service_resp:
                args["NextToken"] = ecs_service_resp["NextToken"]
            else:
                done = True
        client = get_client_with_retries("ecs", ["describe_services"], context=context, session=self._session,
                                         region=region)
        args = {"cluster": cluster ,"services":services, "include":['TAGS']}
        all_services = []
        done = False
        while not done:
            ecs_response = client.describe_services_with_retries(**args)
            for service_data in ecs_response['services']:
                data = self._select_resource_data(service_data)
                all_services.append(data)

            if "NextToken" in ecs_response:
                args["NextToken"] = ecs_response["NextToken"]
            else:
                done = True
        
        return all_services

    def _select_resource_data(self, ecs_resource, tag_service_having_no_tag = False):
        
        def get_tags(inst):
            return {tag["key"]: tag["value"] for tag in inst["tags"]} if "tags" in inst else {}

        tags = get_tags(ecs_resource)
        
        state = ecs_resource["status"]
        state_name = "running"
        is_running = ecs_resource['runningCount']>0
        launch_type = ecs_resource['launchType']
        instance_data = {
            schedulers.INST_ID: ecs_resource["serviceArn"],
            schedulers.INST_ARN:  ecs_resource["serviceArn"],
            schedulers.INST_CLUSTER_ARN:  ecs_resource["clusterArn"],
            schedulers.INST_ALLOW_RESIZE: self.allow_resize,
            schedulers.INST_HIBERNATE: False,
            schedulers.INST_STATE: state,
            schedulers.INST_STATE_NAME: state_name,
            schedulers.INST_IS_RUNNING: is_running,
            schedulers.INST_IS_TERMINATED: False,
            schedulers.INST_CURRENT_STATE: InstanceSchedule.STATE_RUNNING if is_running else InstanceSchedule.STATE_STOPPED,
            schedulers.INST_INSTANCE_TYPE: launch_type,
            schedulers.INST_SERVICE_STOP_TAG: tag_service_having_no_tag,
            # schedulers.INST_ENGINE_TYPE: rds_resource["Engine"],
            schedulers.INST_MAINTENANCE_WINDOW: None,
            schedulers.INST_TAGS: tags,
            schedulers.INST_NAME: tags.get("Name", ""),
            schedulers.INST_SCHEDULE: tags.get(self._tagname, None),
            schedulers.INST_RUNNING_COUNT: ecs_resource['runningCount'],
            schedulers.INST_DESIRED_COUNT: ecs_resource['desiredCount']
        }
        
        return instance_data

    def resize_instance(self, kwargs):
        pass

    def _validate_ecs_tag_values(self, tags):
        result = copy.deepcopy(tags)
        for t in result:
            original_value = t.get("value", "")
            value = re.sub(RESTRICTED_ECS_TAG_VALUE_SET_CHARACTERS, " ", original_value)
            value = value.replace("\n", " ")
            if value != original_value:
                self._logger.warning(WARN_RDS_TAG_VALUE, original_value, t, value)
                t["value"] = value
        return result

    def _stop_instance(self, client, inst):

        def does_snapshot_exist(name):

            try:
                resp = client.describe_db_snapshots_with_retries(DBSnapshotIdentifier=name, SnapshotType="manual")
                snapshot = resp.get("DBSnapshots", None)
                return snapshot is not None
            except Exception as ex:
                if type(ex).__name__ == "DBSnapshotNotFoundFault":
                    return False
                else:
                    raise ex

        args = {
            "DBInstanceIdentifier": inst.id
        }

        if self._config.create_rds_snapshot:
            snapshot_name = "{}-stopped-{}".format(self._stack_name, inst.id).replace(" ", "")
            args["DBSnapshotIdentifier"] = snapshot_name

            try:
                if does_snapshot_exist(snapshot_name):
                    client.delete_db_snapshot_with_retries(DBSnapshotIdentifier=snapshot_name)
                    self._logger.info(INF_DELETE_SNAPSHOT, snapshot_name)
            except Exception as ex:
                self._logger.error(ERR_DELETING_SNAPSHOT, snapshot_name)

        try:
            client.stop_db_instance_with_retries(**args)
            self._logger.info(INF_STOPPED_RESOURCE, "instance", inst.id)
        except Exception as ex:
            self._logger.error(ERR_STOPPING_INSTANCE, "instance", inst.instance_str, str(ex))

    def _tag_stopped_resource(self, client, ecs_resource):
        stop_tags = self._validate_ecs_tag_values(self._config.stopped_tags)
        started_tags = self._config.started_tags

        for t in stop_tags:
            if 'Key' in t and 'Value' in t:
                t['key'] = t.pop('Key')
                t['value'] = t.pop('Value')
        
        if len(started_tags) > 0:
            for t in started_tags:
                if 'Key' in t and 'Value' in t:
                    t['key'] = t.pop('Key')
                    t['value'] = t.pop('Value')
            
        stop_tags.append({'key': 'ScheduledLastDesiredCount', 'value': str(ecs_resource.desired_count)})
            
        if stop_tags is None:
            stop_tags = []
        stop_tags_key_names = [t["key"] for t in stop_tags]
        start_tags_keys = [t["key"] for t in started_tags if t["key"] not in stop_tags_key_names]

        try:
            if start_tags_keys is not None and len(start_tags_keys):
                self._logger.info(INF_REMOVE_KEYS, "start",
                                  ",".join(["\"{}\"".format(k) for k in start_tags_keys]), ecs_resource.arn)
                client.untag_resource_with_retries(resourceArn=ecs_resource.arn, tags=start_tags_keys)
            if len(stop_tags) > 0:
                self._logger.info(INF_ADD_TAGS, "stop", str(stop_tags), ecs_resource.arn)

                client.tag_resource_with_retries(resourceArn=ecs_resource.arn, tags=stop_tags)
        except Exception as ex:
            self._logger.warning(WARN_TAGGING_STOPPED, ecs_resource.id, str(ex))

    def _tag_started_instances(self, client, ecs_resource):
        
        start_tags = self._validate_ecs_tag_values(self._config.started_tags)
        stopped_tags = self._config.stopped_tags
        for t in start_tags:
            if 'Key' in t and 'Value' in t:
                t['key'] = t.pop('Key')
                t['value'] = t.pop('Value')
        
        if start_tags is None:
            start_tags = []
        
        if len(stopped_tags) > 0:    
            for t in stopped_tags:
                if 'Key' in t and 'Value' in t:
                    t['key'] = t.pop('Key')
                    t['value'] = t.pop('Value')
            
        start_tags_key_names = [t["key"] for t in start_tags]

        stop_tags_keys = [t["key"] for t in stopped_tags if t["key"] not in start_tags_key_names]
        try:
            if stop_tags_keys is not None and len(stop_tags_keys):
                self._logger.info(INF_REMOVE_KEYS, "stop",
                                  ",".join(["\"{}\"".format(k) for k in stop_tags_keys]), ecs_resource.arn)
                client.untag_resource_with_retries(resourceArn=ecs_resource.arn, tags=stop_tags_keys)
            if start_tags is not None and len(start_tags) > 0:
                self._logger.info(INF_ADD_TAGS, "start", str(start_tags), ecs_resource.arn)
                client.tag_resource_with_retries(resourceArn=ecs_resource.arn, tags=start_tags)
        except Exception as ex:
            self._logger.warning(WARN_TAGGING_STARTED, ecs_resource.id, str(ex))

    # noinspection PyMethodMayBeStatic
    def stop_instances(self, kwargs):

        self._init_scheduler(kwargs)
        methods = ["update_service",
                   "stop_db_cluster",
                   "describe_db_snapshots",
                   "delete_db_snapshot",
                   "tag_resource",
                   "untag_resource"]

        client = get_client_with_retries("ecs", methods, context=self._context, session=self._session, region=self._region)

        stopped_instances = kwargs["stopped_instances"]
        try:
            for ecs_resource in stopped_instances:
            
                args = {"cluster": ecs_resource.cluster_arn ,"service":ecs_resource.arn, "desiredCount":0}
                
                client.update_service_with_retries(**args)
                self._tag_stopped_resource(client, ecs_resource)
                yield ecs_resource.id, InstanceSchedule.STATE_STOPPED
        except Exception as ex:
            self._logger.error(ERR_STOPPING_INSTANCE, "cluster" if ecs_resource.is_cluster else "instance",
                                  ecs_resource.arn, str(ex))


    # noinspection PyMethodMayBeStatic
    def start_instances(self, kwargs):
        self._init_scheduler(kwargs)

        methods = ["update_service",
                   "start_db_cluster",
                   "tag_resource",
                   "untag_resource"]

        client = get_client_with_retries("ecs", methods, context=self._context, session=self._session, region=self._region)

        started_instances = kwargs["started_instances"]
        try:
            for ecs_resource in started_instances:
               
                args = {"cluster": ecs_resource.cluster_arn ,"service":ecs_resource.arn, "desiredCount": int(ecs_resource.tags['ScheduledLastDesiredCount'])}
                client.update_service_with_retries(**args)
                
                self._tag_started_instances(client, ecs_resource)
                yield ecs_resource.id, InstanceSchedule.STATE_RUNNING
        except Exception as ex:
                self._logger.error(ERR_STARTING_INSTANCE, "cluster" if ecs_resource.is_cluster else "instance",
                                  ecs_resource.arn, str(ex))
                return
        
