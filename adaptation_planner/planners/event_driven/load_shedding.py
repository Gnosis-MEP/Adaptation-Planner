import math

from .qqos_based import (
    QQoS_W_HP_SchedulerPlanner
)


class BaseLoadShaddingSchedulerPlannerMixin():
    """Base mixin for scheduler planning with load shedding capability"""

    def get_bufferstream_required_loadshedding_rate_for_worker(self, worker, required_events):
        events_capacity = worker['resources']['planned'].get(self.events_capacity_key, 0)
        service_type = worker['monitoring']['service_type']
        is_service_type_overloaded = self.required_services_workload_status.get(
            service_type, {}).get('is_overloaded', False)

        if events_capacity >= 0 or not is_service_type_overloaded:
            return 0
        overloaded_events = (events_capacity * -1)

        loadshedding_rate = overloaded_events / required_events
        return min(1, loadshedding_rate)


class QQoS_W_HP_LS_SchedulerPlanner(
        QQoS_W_HP_SchedulerPlanner, BaseLoadShaddingSchedulerPlannerMixin):
    """
    Query-aware QoS Top-K Low Parallel with Load Shedding scheduler.
    """

    def __init__(self, parent_service, ce_endpoint_stream_key):
        super(QQoS_W_HP_LS_SchedulerPlanner, self).__init__(parent_service, ce_endpoint_stream_key)
        self.strategy_name = 'QQoS-W-HP-LS'

    def update_workers_planned_resources(
            self, dataflow_choices_with_cum_weights, dataflow_choices_weights, planned_event_count):

        total_cum_weight = dataflow_choices_with_cum_weights[-1][0]
        load_shedding_choices = []
        for dataflow_index, dataflow_choice in enumerate(dataflow_choices_with_cum_weights):
            service_worker_key_tuples = dataflow_choice[1]

            relative_weight = dataflow_choices_weights[dataflow_index]
            probability = relative_weight / total_cum_weight
            proportional_used_resources = math.ceil(planned_event_count * probability)
            worst_loadshedding_rate = 0
            for _, service_key, worker_key in service_worker_key_tuples:
                worker = self.all_services_worker_pool[service_key][worker_key]
                events_capacity = self.get_worker_events_capacity(worker)
                updated_events_capacity = events_capacity - proportional_used_resources
                worker['resources']['planned'][self.events_capacity_key] = updated_events_capacity
                self.all_services_worker_pool[service_key][worker_key] = worker
                worst_loadshedding_rate = max(
                    worst_loadshedding_rate, self.get_bufferstream_required_loadshedding_rate_for_worker(
                        worker, proportional_used_resources)
                )
            load_shedding_choices.append(worst_loadshedding_rate)

        return load_shedding_choices

    def format_dataflow_choices_to_buffer_stream_choices_plan(self, dataflow_choices, loadshedding_rate_choices):
        buffer_stream_choices_plan = []
        for i, dataflow_choice in enumerate(dataflow_choices):
            loadshedding_rate = loadshedding_rate_choices[i]
            plan_cum_weight = dataflow_choice[0]
            service_worker_key_tuples = dataflow_choice[1]
            dataflow = [[df[-1]] for df in service_worker_key_tuples]
            dataflow.append([self.ce_endpoint_stream_key])
            buffer_stream_choice = (loadshedding_rate, plan_cum_weight, dataflow)
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
        loadshedding_rate_choices = self.update_workers_planned_resources(
            dataflow_choices, weights, planned_event_count)

        # only apply loadshedding on latency min policies
        if qos_policy_name != 'latency' or qos_policy_value != 'min':
            loadshedding_rate_choices = [0 for i in loadshedding_rate_choices]

        buffer_stream_choices_plan = self.format_dataflow_choices_to_buffer_stream_choices_plan(
            dataflow_choices, loadshedding_rate_choices
        )
        return buffer_stream_choices_plan
