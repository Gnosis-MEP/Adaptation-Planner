import functools
import itertools
import math

import numpy as np

from ..conf import QUERY_SERVICE_CHAIN_FIELD
from .scheduling import BaseSchedulerPlanner


class BaseQoSSchedulerPlanner(BaseSchedulerPlanner):
    """Base class for scheduler planning"""

    def __init__(self, parent_service, scheduler_cmd_stream_key, ce_endpoint_stream_key):
        super(BaseQoSSchedulerPlanner, self).__init__(
            parent_service,
            scheduler_cmd_stream_key,
            ce_endpoint_stream_key
        )
        self.adaptation_delta = 10
        self.events_capacity_key = 'events_capacity'

    def get_bufferstream_planned_event_count(self, buffer_stream_entity):
        fps = float(buffer_stream_entity['fps'])
        num_events = self.adaptation_delta * fps
        return math.ceil(num_events)

    def initialize_planned_worker_event_capacity(self, worker):
        if self.events_capacity_key not in worker['resources']['planned']:
            queue_size = int(worker['monitoring']['queue_size'])
            throughput = float(worker['monitoring']['throughput'])
            max_events_capacity = self.adaptation_delta * throughput
            events_capacity = max_events_capacity - queue_size
            worker['resources']['planned'][self.events_capacity_key] = events_capacity

    def initialize_service_workers_planned_capacity(self, worker_pool):
        for worker_key, worker in worker_pool.items():
            self.initialize_planned_worker_event_capacity(worker)
        return worker_pool

    def get_worker_events_capacity(self, worker):
        return worker['resources']['planned'].get(self.events_capacity_key, 0)

    def is_worker_overloaded(self, worker):
        return self.get_worker_events_capacity(worker) <= 0

    def filter_overloaded_service_worker_pool_or_all_if_empty(self, worker_pool):
        selected_worker_pool = dict(filter(
            lambda kv: not self.is_worker_overloaded(kv[1]), worker_pool.items()
        ))
        if len(selected_worker_pool) == 0:
            selected_worker_pool = worker_pool
        return selected_worker_pool

    def prepare_local_queries_entities(self, knowledge_queries):
        # get the query about the subscriber_query
        query_ref = next(filter(lambda q: 'gnosis-mep:subscriber_query' in q, knowledge_queries))
        query = knowledge_queries[query_ref]
        data_triples = query['data']

        for subj, pred, obj in data_triples:
            query_id = subj.split('/')[-1]
            attribute = pred.split('#')[-1]
            value = obj
            self.all_queries.setdefault(
                query_id,
                {
                    QUERY_SERVICE_CHAIN_FIELD: [],
                    'qos_policies': {}
                }
            )
            if attribute == 'query':
                attribute = 'query_text'

            if attribute == 'qos_policies':
                qos_k, qos_v = value.split(':')
                value = {qos_k: qos_v}
                self.all_queries[query_id][attribute].update(value)
                continue

            if attribute not in self.all_queries[query_id].keys():
                self.all_queries[query_id][attribute] = value
            else:
                if not isinstance(self.all_queries[query_id][attribute], list):
                    self.all_queries[query_id][attribute] = [
                        self.all_queries[query_id][attribute]
                    ]
                self.all_queries[query_id][attribute].append(value)


class SingleBestForQoSSinglePolicySchedulerPlanner(BaseQoSSchedulerPlanner):
    """
    """

    def __init__(self, parent_service, scheduler_cmd_stream_key, ce_endpoint_stream_key):
        super(SingleBestForQoSSinglePolicySchedulerPlanner, self).__init__(
            parent_service,
            scheduler_cmd_stream_key,
            ce_endpoint_stream_key
        )
        self.strategy_name = 'single_best'

    def workers_key_sorted_by_qos(self, worker_pool, qos_policy_name, qos_policy_value):
        reverse = True if qos_policy_value == 'max' else False

        metric_name = qos_policy_name
        # latency actually will check for the oposity of throughput:
        # min latency == max worker throughput
        if qos_policy_name == 'latency':
            metric_name = 'throughput'
            reverse = False if reverse else True

        sorted_workers = sorted(
            worker_pool.keys(), key=lambda w_key: float(worker_pool[w_key]['monitoring'][metric_name]),
            reverse=reverse
        )

        return sorted_workers

    def update_workers_planned_resources(self, required_services, buffer_stream_plan, required_events):
        for dataflow_index, service in enumerate(required_services):
            worker_key = buffer_stream_plan[dataflow_index][0]
            worker = self.all_services_worker_pool[service][worker_key]
            events_capacity = worker['resources']['planned'].get(self.events_capacity_key, 0)
            updated_events_capacity = events_capacity - required_events
            worker['resources']['planned'][self.events_capacity_key] = updated_events_capacity

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
        self.update_workers_planned_resources(required_services, buffer_stream_plan, required_events)
        buffer_stream_plan.append([self.ce_endpoint_stream_key])
        return buffer_stream_plan

    def create_buffer_stream_choices_plan(self, buffer_stream_entity):
        buffer_stream_plan = self.create_buffer_stream_plan(buffer_stream_entity)
        plan_cum_weight = None
        return [(plan_cum_weight, buffer_stream_plan)]


class WeightedRandomQoSSinglePolicySchedulerPlanner(BaseQoSSchedulerPlanner):
    """
    Bufferstream Choices Plan:
        [<Corrected_dataflow_choice>]

    Corrected_dataflow_choice:
        [Dataflow cum. Weight, ([worker_stream_key], [worker_stream_key]...[ce_endpoint_stream_key])]

    cumsum_dataflow Choices:
        [best_weight_and_index_tuple, [<cumsum dataflow choice>]]

    cumsum dataflow choice:
        (Dataflow cum. weight, Dataflow Choice)

    (Bufferstream) Dataflow Choice:
        [<service worker weight tuple>]

    service worker weight tuple:
        (worker_weight, service, worker_stream_key)
    """

    def __init__(self, parent_service, scheduler_cmd_stream_key, ce_endpoint_stream_key):
        super(WeightedRandomQoSSinglePolicySchedulerPlanner, self).__init__(
            parent_service,
            scheduler_cmd_stream_key,
            ce_endpoint_stream_key
        )
        self.strategy_name = 'weighted_random'

    def create_filtered_and_weighted_workers_pool(
            self, required_services, planned_event_count, qos_policy_name, qos_policy_value):
        per_service_worker_keys_with_weights = {}
        for service in required_services:
            worker_pool = self.all_services_worker_pool[service]
            service_workers_tuple_list = []
            selected_worker_pool = self.filter_overloaded_service_worker_pool_or_all_if_empty(worker_pool)
            for worker_key, worker in selected_worker_pool.items():
                worker_weight = self.get_worker_choice_weight_for_qos_policy(
                    worker, planned_event_count, qos_policy_name, qos_policy_value
                )
                service_workers_tuple_list.append((worker_weight, service, worker_key))
            per_service_worker_keys_with_weights[service] = service_workers_tuple_list
        return per_service_worker_keys_with_weights

    def get_worker_congestion_impact_rate(self, worker, planned_event_count):
        # get_bufferstream_planned_event_count
        worker_events_capacity = self.get_worker_events_capacity(worker)
        # if already overloaded, then make it so that the events capacity
        # affects very strongly the final weigth
        if worker_events_capacity < 0:
            worker_events_capacity = 1 / (worker_events_capacity * -1)
        else:
            worker_events_capacity += 1
        # congestion impact rate based on the planned events counts
        # the plus one is in case the capacity is at 0, we don't want the rate to be 0
        congestion_impact_rate = (worker_events_capacity) / planned_event_count
        # we don't care if the worker is more than capable of handling the ammount
        # of workload, only care when it cannot handle, since this would cause congestion
        if congestion_impact_rate > 1:
            congestion_impact_rate = 1
        return congestion_impact_rate

    def get_worker_choice_weight_for_qos_policy(self, worker, planned_event_count, qos_policy_name, qos_policy_value):
        inverse = True if qos_policy_value == 'min' else False

        if qos_policy_name == 'latency':
            qos_policy_name = 'throughput'
            inverse = False if inverse else True

        weight = float(worker['monitoring'][qos_policy_name])
        if inverse:
            weight = 1 / weight

        congestion_impact_rate = self.get_worker_congestion_impact_rate(worker, planned_event_count)
        weight_with_congestion = weight * congestion_impact_rate
        return weight_with_congestion

    def get_dataflow_choice_min_weight(self, dataflow_choice):
        if len(dataflow_choice) == 1:
            dataflow_weight = dataflow_choice[0][0]
        else:
            dataflow_weight = functools.reduce(
                lambda w_tuple_a, w_tuple_b:
                    [min(w_tuple_a[0], w_tuple_b[0])],
                dataflow_choice
            )[0]
        return dataflow_weight

    def create_cartesian_product_dataflow_choices(self, per_service_worker_keys_with_weights):
        cartesian_product_dataflows_choices = list(itertools.product(*per_service_worker_keys_with_weights.values()))
        return cartesian_product_dataflows_choices

    def create_dataflow_choices_weights(self, cartesian_product_dataflows_choices):
        dataflow_choices_weights = [
            self.get_dataflow_choice_min_weight(dataflow_choice)
            for dataflow_choice in cartesian_product_dataflows_choices
        ]
        return dataflow_choices_weights

    def create_dataflow_choices_with_cum_weights(self, cartesian_product_dataflows_choices, dataflow_choices_weights):
        cum_sum_weights = np.cumsum(dataflow_choices_weights, dtype=float)

        dataflow_choices_with_cum_weights = zip(cum_sum_weights, cartesian_product_dataflows_choices)

        return list(dataflow_choices_with_cum_weights)

    def create_dataflow_choices_with_cum_weights_and_relative_weights(self, per_service_worker_keys_with_weights):
        cartesian_product_dataflows_choices = self.create_cartesian_product_dataflow_choices(
            per_service_worker_keys_with_weights
        )
        dataflow_choices_weights = self.create_dataflow_choices_weights(cartesian_product_dataflows_choices)

        dataflow_choices_with_cum_weights = self.create_dataflow_choices_with_cum_weights(
            cartesian_product_dataflows_choices, dataflow_choices_weights
        )

        return dataflow_choices_with_cum_weights, dataflow_choices_weights

    def update_workers_planned_resources(
            self, dataflow_choices_with_cum_weights, dataflow_choices_weights, planned_event_count):

        total_cum_weight = dataflow_choices_with_cum_weights[-1][0]
        for dataflow_index, dataflow_choice in enumerate(dataflow_choices_with_cum_weights):
            service_worker_key_tuples = dataflow_choice[1]

            relative_weight = dataflow_choices_weights[dataflow_index]
            probability = relative_weight / total_cum_weight
            proportional_used_resources = planned_event_count * probability
            for _, service_key, worker_key in service_worker_key_tuples:
                worker = self.all_services_worker_pool[service_key][worker_key]
                events_capacity = self.get_worker_events_capacity(worker)
                updated_events_capacity = events_capacity - proportional_used_resources
                worker['resources']['planned'][self.events_capacity_key] = updated_events_capacity
                self.all_services_worker_pool[service_key][worker_key] = worker

    def format_dataflow_choices_to_buffer_stream_choices_plan(self, dataflow_choices):
        buffer_stream_choices_plan = []
        for dataflow_choice in dataflow_choices:
            plan_cum_weight = dataflow_choice[0]
            service_worker_key_tuples = dataflow_choice[1]
            dataflow = [[df[-1]] for df in service_worker_key_tuples]
            dataflow.append([self.ce_endpoint_stream_key])
            buffer_stream_choice = (plan_cum_weight, dataflow)
            buffer_stream_choices_plan.append(buffer_stream_choice)
        return buffer_stream_choices_plan

    def create_buffer_stream_choices_plan(self, buffer_stream_entity):
        required_services = self.get_buffer_stream_required_services(buffer_stream_entity)
        first_query = list(buffer_stream_entity['queries'].values())[0]
        qos_policy_name, qos_policy_value = list(first_query['qos_policies'].items())[0]

        planned_event_count = self.get_bufferstream_planned_event_count(buffer_stream_entity)
        per_service_worker_keys_with_weights = self.create_filtered_and_weighted_workers_pool(
            required_services, planned_event_count, qos_policy_name, qos_policy_value
        )
        dataflow_choices, weights = self.create_dataflow_choices_with_cum_weights_and_relative_weights(
            per_service_worker_keys_with_weights
        )
        self.update_workers_planned_resources(
            dataflow_choices, weights, planned_event_count)
        buffer_stream_choices_plan = self.format_dataflow_choices_to_buffer_stream_choices_plan(
            dataflow_choices
        )
        return buffer_stream_choices_plan
