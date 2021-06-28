import functools
import itertools
import math

import numpy as np

from .qos_based_scheduling import BaseQoSSchedulerPlanner


class BaseLoadShaddingSchedulerPlanner(BaseQoSSchedulerPlanner):
    """Base class for scheduler planning with load shedding capability"""

    def get_bufferstream_required_loadshedding_rate_for_worker(self, worker):
        throughput = float(worker['monitoring']['throughput'])
        max_events_capacity = self.adaptation_delta * throughput
        events_capacity = worker['resources']['planned'].get(self.events_capacity_key, 0)

        if events_capacity >= 0:
            return 0

        loadshedding_rate = (events_capacity * -1) / max_events_capacity
        return min(1, loadshedding_rate)


class SingleBestForQoSSinglePolicyLSSchedulerPlanner(BaseLoadShaddingSchedulerPlanner):
    """
    """

    def __init__(self, parent_service, scheduler_cmd_stream_key, ce_endpoint_stream_key):
        super(SingleBestForQoSSinglePolicyLSSchedulerPlanner, self).__init__(
            parent_service,
            scheduler_cmd_stream_key,
            ce_endpoint_stream_key
        )
        self.strategy_name = 'single_best_load_shedding'

    def update_workers_planned_resources(self, required_services, buffer_stream_plan, required_events):
        worst_loadshedding_rate = 0
        for dataflow_index, service in enumerate(required_services):
            worker_key = buffer_stream_plan[dataflow_index][0]
            worker = self.all_services_worker_pool[service][worker_key]
            events_capacity = worker['resources']['planned'].get(self.events_capacity_key, 0)
            updated_events_capacity = events_capacity - required_events
            worker['resources']['planned'][self.events_capacity_key] = updated_events_capacity
            worst_loadshedding_rate = max(
                worst_loadshedding_rate, self.get_bufferstream_required_loadshedding_rate_for_worker(worker))
        return worst_loadshedding_rate

    def create_buffer_stream_plan(self, buffer_stream_entity):
        required_services = self.get_buffer_stream_required_services(buffer_stream_entity)
        first_query = list(buffer_stream_entity['queries'].values())[0]
        qos_policy_name, qos_policy_value = list(first_query['qos_policies'].items())[0]
        required_events = self.get_bufferstream_planned_event_count(buffer_stream_entity)

        buffer_stream_plan = []
        for service in required_services:
            worker_pool = self.all_services_worker_pool[service]
            worker_pool = self.initialize_service_workers_planned_capacity(worker_pool)
            selected_worker_pool = self.filter_overloaded_service_worker_pool_or_all_if_empty(worker_pool)

            qos_sorted_workers_keys = self.workers_key_sorted_by_qos(
                selected_worker_pool, qos_policy_name, qos_policy_value)

            best_worker_key = qos_sorted_workers_keys[0]
            buffer_stream_plan.append([best_worker_key])
        loadshedding_rate = self.update_workers_planned_resources(
            required_services, buffer_stream_plan, required_events
        )
        buffer_stream_plan.append([self.ce_endpoint_stream_key])
        return buffer_stream_plan, loadshedding_rate

    def create_buffer_stream_choices_plan(self, buffer_stream_entity):
        buffer_stream_plan, loadshedding_rate = self.create_buffer_stream_plan(buffer_stream_entity)

        plan_cum_weight = None
        return [(loadshedding_rate, plan_cum_weight, buffer_stream_plan)]
