import functools
import itertools
from .base import BaseSchedulerPlanner


class RandomSchedulerPlanner(BaseSchedulerPlanner):

    def __init__(self, parent_service, ce_endpoint_stream_key):
        super(RandomSchedulerPlanner, self).__init__(parent_service, ce_endpoint_stream_key)
        self.strategy_name = 'random'

    def calculate_worker_queue_space_percentage(self, worker):
        return worker['resources']['planned']['queue_space'] / worker['monitoring']['queue_limit']

    def get_non_floaded_queue_workers(self, worker_pool, min_queue_space_percent):
        return dict(filter(
            lambda x: self.calculate_worker_queue_space_percentage(x[1]) >= min_queue_space_percent, worker_pool.items()
        ))

    def workers_keys_sorted_by_best_energy_consumption(self, worker_pool):
        sorted_workers = sorted(
            worker_pool.keys(), key=lambda w_key: worker_pool[w_key]['resources']['usage']['energy_consumption']
        )

        return sorted_workers

    def update_worker_planned_resource(self, worker, min_queue_space_percent):
        planned_usage = int(worker['monitoring']['queue_limit'] * (min_queue_space_percent))
        worker['resources']['planned']['queue_space'] -= planned_usage
        return worker

    def get_worker_choice_weight(self, worker):
        energy_consumption = worker['resources']['usage']['energy_consumption']
        queue_space_percentage = self.calculate_worker_queue_space_percentage(worker)
        # rating = space_perc * (1 / energy)
        # the lower the energy consumption the higher the rating
        # the higher the space, the higher the rating
        if queue_space_percentage < 0:
            queue_space_percentage = 0.001
        rating = queue_space_percentage / (energy_consumption**2)
        return rating

    def get_per_service_nonfloaded_workers(self, required_services, min_queue_space_percent):
        per_service_nonfloaded_workers = {}
        for service in required_services:
            per_service_nonfloaded_workers[service] = self.get_service_nonfloaded_workers(
                service, min_queue_space_percent)
        return per_service_nonfloaded_workers

    def get_dataflow_choice_weight(self, dataflow_choice):
        if len(dataflow_choice) == 1:
            dataflow_weight = dataflow_choice[0][-1]
        else:
            dataflow_weight = functools.reduce(
                lambda w_tuple_a, w_tuple_b:
                    min(w_tuple_a[-1], w_tuple_b[-1]),
                dataflow_choice
            )
        return dataflow_weight

    def calculate_cumsum_for_dataflow_choice(self, dataflow_index, dataflow_choice, prev_cum_weight, best_weight_and_index):
        # dataflow weight is the min weight of the whole dataflow workers
        # (if one step is bad it will tend to reduce the whole dataflow to that level)
        dataflow_weight = self.get_dataflow_choice_weight(dataflow_choice)
        dataflow_cum_weight = prev_cum_weight + dataflow_weight
        if best_weight_and_index is None:
            best_weight_and_index = (dataflow_weight, dataflow_index)
        else:
            if dataflow_weight >= best_weight_and_index[0]:
                best_weight_and_index = (dataflow_weight, dataflow_index)
        prev_cum_weight = dataflow_cum_weight

        dataflow_weighted_choice = [dataflow_cum_weight, dataflow_choice]

        return dataflow_weighted_choice, prev_cum_weight, best_weight_and_index

    def create_dataflow_choices_with_cum_weights_and_best_dataflow(self, per_service_nonfloaded_workers):
        cartesian_product_dataflows_choices = itertools.product(*per_service_nonfloaded_workers.values())

        dataflow_choices_with_cum_weights = []
        best_weight_and_index = None
        prev_cum_weight = 0
        for i, dataflow_choice in enumerate(cartesian_product_dataflows_choices):
            dataflow_weighted_choice, prev_cum_weight, best_weight_and_index = self.calculate_cumsum_for_dataflow_choice(
                i, dataflow_choice, prev_cum_weight, best_weight_and_index
            )

            dataflow_choices_with_cum_weights.append(dataflow_weighted_choice)

        return dataflow_choices_with_cum_weights, best_weight_and_index

    def clean_up_dataflow_choices_for_correct_format(self, dataflow_choices_with_cum_weights):
        correct_format = []
        for cum_weight, dataflow_choice in dataflow_choices_with_cum_weights:
            correct_dataflow_choice = tuple([df[1]] for df in dataflow_choice)
            correct_dataflow_choice += ([self.ce_endpoint_stream_key],)
            correct_weighted_format = [cum_weight, correct_dataflow_choice]
            correct_format.append(correct_weighted_format)
        return correct_format

    def create_buffer_stream_choices_plan(self, buffer_stream_entity):
        min_queue_space_percent = 0.3
        required_services = self.get_buffer_stream_required_services(buffer_stream_entity)

        per_service_nonfloaded_workers = self.get_per_service_nonfloaded_workers(
            required_services, min_queue_space_percent)

        dataflow_choices_with_cum_weights, best_weight_and_index = self.create_dataflow_choices_with_cum_weights_and_best_dataflow(
            per_service_nonfloaded_workers
        )
        # update resources planned utilization based on the best dataflow change to be chosen
        # since this will be the dataflow that most-likely will be used chosen in the scheduler
        best_dataflow = dataflow_choices_with_cum_weights[best_weight_and_index[1]]
        total_cum_weight = dataflow_choices_with_cum_weights[-1][0]
        best_dataflow_weight = best_weight_and_index[0]
        self.update_workers_planned_resources(
            best_dataflow, best_dataflow_weight, total_cum_weight, min_queue_space_percent)

        cleaned_dataflow_choices_with_cum_weights = self.clean_up_dataflow_choices_for_correct_format(
            dataflow_choices_with_cum_weights)

        return cleaned_dataflow_choices_with_cum_weights

    def get_service_nonfloaded_workers(self, service, min_queue_space_percent):
        worker_pool = self.all_services_worker_pool[service]
        return list(map(
            lambda kv: (service, kv[0], 0),
            worker_pool.items()
        ))

    def update_workers_planned_resources(self, dataflow_choice_weighted, dataflow_weight, total_cum_weight, min_queue_space_percent):
        # Change of getting this dataflow
        cum_weight, dataflow = dataflow_choice_weighted

        rate_worker_usage = len(dataflow)
        resource_usage_rating = (dataflow_weight / rate_worker_usage)

        # change of getting best dataflow times the expected usage before a new plan would be required
        resource_usage = resource_usage_rating * min_queue_space_percent
        for worker in dataflow:
            service_type, worker_key, _ = worker
            actual_worker_reference = self.all_services_worker_pool[service_type][worker_key]
            self.update_worker_planned_resource(actual_worker_reference, resource_usage)


class RoundRobinSchedulerPlanner(RandomSchedulerPlanner):

    def __init__(self, parent_service, ce_endpoint_stream_key):
        super(RoundRobinSchedulerPlanner, self).__init__(
            parent_service,
            ce_endpoint_stream_key
        )
        self.strategy_name = 'round_robin'
