from .qos_based_scheduling import (
    SingleBestForQoSSinglePolicySchedulerPlanner,
    WeightedRandomQoSSinglePolicySchedulerPlanner
)


class BaseLoadShaddingSchedulerPlannerMixin():
    """Base mixin for scheduler planning with load shedding capability"""

    def get_bufferstream_required_loadshedding_rate_for_worker(self, worker, required_events):
        events_capacity = worker['resources']['planned'].get(self.events_capacity_key, 0)
        if events_capacity >= 0:
            return 0
        overloaded_events = (events_capacity * -1)

        loadshedding_rate = overloaded_events / required_events
        return min(1, loadshedding_rate)


class SingleBestForQoSSinglePolicyLSSchedulerPlanner(
        SingleBestForQoSSinglePolicySchedulerPlanner, BaseLoadShaddingSchedulerPlannerMixin):
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
                worst_loadshedding_rate, self.get_bufferstream_required_loadshedding_rate_for_worker(
                    worker, required_events)
            )
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
        # ignore loadshedding on accuracy maximisation policies
        if qos_policy_name == 'accuracy' and qos_policy_value == 'max':
            loadshedding_rate = 0
        buffer_stream_plan.append([self.ce_endpoint_stream_key])
        return buffer_stream_plan, loadshedding_rate

    def create_buffer_stream_choices_plan(self, buffer_stream_entity):
        buffer_stream_plan, loadshedding_rate = self.create_buffer_stream_plan(buffer_stream_entity)

        plan_cum_weight = None
        return [(loadshedding_rate, plan_cum_weight, buffer_stream_plan)]


class WeightedRandomQoSSinglePolicyLSSchedulerPlanner(
        WeightedRandomQoSSinglePolicySchedulerPlanner, BaseLoadShaddingSchedulerPlannerMixin):

    def __init__(self, parent_service, scheduler_cmd_stream_key, ce_endpoint_stream_key):
        super(WeightedRandomQoSSinglePolicyLSSchedulerPlanner, self).__init__(
            parent_service,
            scheduler_cmd_stream_key,
            ce_endpoint_stream_key
        )
        self.strategy_name = 'weighted_random_load_shedding'

    def update_workers_planned_resources(
            self, dataflow_choices_with_cum_weights, dataflow_choices_weights, planned_event_count):

        total_cum_weight = dataflow_choices_with_cum_weights[-1][0]
        load_shedding_choices = []
        for dataflow_index, dataflow_choice in enumerate(dataflow_choices_with_cum_weights):
            service_worker_key_tuples = dataflow_choice[1]

            relative_weight = dataflow_choices_weights[dataflow_index]
            probability = relative_weight / total_cum_weight
            proportional_used_resources = planned_event_count * probability
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
            service_worker_key_tuples = dataflow_choice[2]
            dataflow = [[df[-1]] for df in service_worker_key_tuples]
            dataflow.append([self.ce_endpoint_stream_key])
            buffer_stream_choice = (loadshedding_rate, plan_cum_weight, dataflow)
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
        loadshedding_rate_choices = self.update_workers_planned_resources(
            dataflow_choices, weights, planned_event_count)

        # ignore loadshedding on accuracy maximisation policies
        if qos_policy_name == 'accuracy' and qos_policy_value == 'max':
            loadshedding_rate_choices = [0 for i in loadshedding_rate_choices]

        buffer_stream_choices_plan = self.format_dataflow_choices_to_buffer_stream_choices_plan(
            dataflow_choices, loadshedding_rate_choices
        )
        return buffer_stream_choices_plan
