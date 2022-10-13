import functools
import itertools

import numpy as np

from .base import BaseQoSSchedulerPlanner


class QQoS_W_HP_SchedulerPlanner(BaseQoSSchedulerPlanner):
    """
    Query-aware QoS Weighted High Parallelism scheduler.
    """

    def __init__(self, parent_service, ce_endpoint_stream_key):
        super(QQoS_W_HP_SchedulerPlanner, self).__init__(parent_service, ce_endpoint_stream_key)
        self.strategy_name = 'QQoS-W-HP'

    def create_filtered_and_weighted_workers_pool(
            self, required_services, planned_event_count, qos_policy_name, qos_policy_value):
        per_service_worker_keys_with_weights = {}
        for service in required_services:
            service_workers_tuple_list = []
            selected_worker_pool = self.get_init_workers_filter_based_on_qos_policy(
                service, qos_policy_name, qos_policy_value)
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

        congestion_impact_rate = self.get_worker_congestion_impact_rate(
            worker, planned_event_count)
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
        qos_policies = list(first_query['parsed_query']['qos_policies'].items())
        if not qos_policies:
            raise RuntimeError(f'No QoS policy defined for query "{first_query}"')
        qos_policy_name, qos_policy_value = qos_policies[0]

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


class QQoS_TK_LP_SchedulerPlanner(BaseQoSSchedulerPlanner):
    """
    Query-aware QoS Top-k Low Parallelism scheduler.
    """

    def __init__(self, parent_service, ce_endpoint_stream_key):
        super(QQoS_TK_LP_SchedulerPlanner, self).__init__(parent_service, ce_endpoint_stream_key)
        self.strategy_name = 'QQoS-TK-LP'

    def workers_key_sorted_by_qos(self, worker_pool, qos_policy_name, qos_policy_value):
        "deprecation: this method will be removed after the slr rank is implemented"
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

    def worker_key_sorted_by_slr_rank(self, worker_pool, service_type, slr_profile_id):
        slr_profile = self.slr_profiles_by_service.get(service_type, {}).get(slr_profile_id)

        if slr_profile is None:
            self.parent_service.logger.warning((
                f'No slr profile for: service type: "{service_type}" and slr_profile_id: {slr_profile_id}'
                'Will not sort the worker pool based on the slr rank.'
            ))
            return list(worker_pool.keys())

        sorted_worker_keys = []
        for rank_index in slr_profile['ranking_index']:
            worker_key = slr_profile['alternatives_ids'][rank_index]
            if worker_key in worker_pool.keys():
                sorted_worker_keys.append(worker_key)

        return sorted_worker_keys

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
        slr_profile_id = first_query.get('slr_profile_id', None)
        qos_policies = list(first_query['parsed_query']['qos_policies'].items())
        if not slr_profile_id:
            raise RuntimeError(f'No SLR Profile ID for query "{first_query}"')
        qos_policy_name, qos_policy_value = qos_policies[0]
        required_events = self.get_bufferstream_planned_event_count(buffer_stream_entity)

        buffer_stream_plan = []
        for service in required_services:
            selected_worker_pool = self.get_init_workers_filter_based_on_qos_policy(
                service, qos_policy_name, qos_policy_value)

            slr_sorted_workers_keys = self.worker_key_sorted_by_slr_rank(
                selected_worker_pool, service, slr_profile_id
            )

            best_worker_key = slr_sorted_workers_keys[0]
            buffer_stream_plan.append([best_worker_key])
        self.update_workers_planned_resources(required_services, buffer_stream_plan, required_events)
        buffer_stream_plan.append([self.ce_endpoint_stream_key])
        return buffer_stream_plan

    def create_buffer_stream_choices_plan(self, buffer_stream_entity):
        buffer_stream_plan = self.create_buffer_stream_plan(buffer_stream_entity)
        plan_cum_weight = None
        return [(plan_cum_weight, buffer_stream_plan)]
