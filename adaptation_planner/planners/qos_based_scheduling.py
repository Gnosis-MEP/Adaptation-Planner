import functools
import itertools
import math

from ..conf import QUERY_SERVICE_CHAIN_FIELD
from .scheduling import BaseSchedulerPlanner


class BaseQoSSchedulerPlanner(BaseSchedulerPlanner):
    """Base class for scheduler planning"""

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
        self.adaptation_delta = 10
        self.events_capacity_key = 'events_capacity'

    def get_bufferstream_planned_event_count(self, buffer_stream_entity):
        fps = float(buffer_stream_entity['fps'])
        num_events = self.adaptation_delta * fps
        return math.ceil(num_events)

    def calculate_worker_queue_space_percentage(self, worker):
        return worker['resources']['planned']['queue_space'] / worker['monitoring']['queue_limit']

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

    def is_worker_overloaded(self, worker):
        return worker['resources']['planned'].get(self.events_capacity_key, 0) <= 0

    def filter_overloaded_service_worker_pool_or_all_if_empty(self, worker_pool):
        selected_worker_pool = dict(filter(
            lambda kv: not self.is_worker_overloaded(kv[1]), worker_pool.items()
        ))
        if len(selected_worker_pool) == 0:
            selected_worker_pool = worker_pool
        return selected_worker_pool

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
        super(SingleBestForQoSSinglePolicySchedulerPlanner, self).__init__(
            parent_service,
            scheduler_cmd_stream_key,
            ce_endpoint_stream_key
        )
        self.strategy_name = 'weighted_random'

    def filter_available_service_worker_pool(self, worker_pool):
        # dict(filter(
        #     lambda x: self.calculate_worker_queue_space_percentage(x[1]) >= min_queue_space_percent, worker_pool.items()
        # ))
        return worker_pool

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

    def get_dataflow_choice_min_weight(self, dataflow_choice):
        if len(dataflow_choice) == 1:
            dataflow_weight = dataflow_choice[0][-1]
        else:
            dataflow_weight = functools.reduce(
                lambda w_tuple_a, w_tuple_b:
                    min(w_tuple_a[-1], w_tuple_b[-1]),
                dataflow_choice
            )
        return dataflow_weight

    def calculate_cumsum_for_dataflow_choice(
            self, dataflow_index, dataflow_choice, prev_cum_weight, best_weight_and_index):
        # dataflow weight is the min weight of the whole dataflow workers
        # (if one step is bad it will tend to reduce the whole dataflow to that level)
        dataflow_weight = self.get_dataflow_choice_min_weight(dataflow_choice)
        dataflow_cum_weight = prev_cum_weight + dataflow_weight
        if best_weight_and_index is None:
            best_weight_and_index = (dataflow_weight, dataflow_index)
        else:
            if dataflow_weight >= best_weight_and_index[0]:
                best_weight_and_index = (dataflow_weight, dataflow_index)
        prev_cum_weight = dataflow_cum_weight

        dataflow_weighted_choice = [dataflow_cum_weight, dataflow_choice]

        return best_weight_and_index, prev_cum_weight, dataflow_weighted_choice

    def create_dataflow_choices_with_cum_weights_and_best_dataflow(self, per_service_workers_with_weights):
        cartesian_product_dataflows_choices = itertools.product(*per_service_workers_with_weights.values())

        dataflow_choices_with_cum_weights = []
        best_weight_and_index = None
        prev_cum_weight = 0
        for i, dataflow_choice in enumerate(cartesian_product_dataflows_choices):
            ret_tuple = self.calculate_cumsum_for_dataflow_choice(
                i, dataflow_choice, prev_cum_weight, best_weight_and_index
            )
            best_weight_and_index, prev_cum_weight, dataflow_weighted_choice = ret_tuple
            dataflow_choices_with_cum_weights.append(dataflow_weighted_choice)

        return best_weight_and_index, dataflow_choices_with_cum_weights

    # def update_workers_planned_resources(
    #         self, dataflow_choice_weighted, dataflow_weight, total_cum_weight, min_queue_space_percent):
    #     # Change of getting this dataflow
    #     resource_usage_rating = (dataflow_weight / total_cum_weight)

    #     # change of getting best dataflow times the expected usage before a new plan would be required
    #     resource_usage = resource_usage_rating * min_queue_space_percent
    #     cum_weight, dataflow = dataflow_choice_weighted
    #     for worker in dataflow:
    #         service_type, worker_key, _ = worker
    #         actual_worker_reference = self.all_services_worker_pool[service_type][worker_key]
    #         self.update_worker_planned_resource(actual_worker_reference, resource_usage)

    def create_buffer_stream_plan(self, buffer_stream_entity):
        required_services = self.get_buffer_stream_required_services(buffer_stream_entity)
        first_query = list(buffer_stream_entity['queries'].values())[0]
        qos_policy_name, qos_policy_value = list(first_query['qos_policies'].items())[0]

        buffer_stream_plan = []
        for service in required_services:
            worker_pool = self.all_services_worker_pool[service]
            selected_worker_pool = self.filter_available_service_worker_pool(worker_pool)
            if len(selected_worker_pool) == 0:
                selected_worker_pool = worker_pool

            qos_sorted_workers_keys = self.workers_key_sorted_by_qos(
                selected_worker_pool, qos_policy_name, qos_policy_value)

            best_worker_key = qos_sorted_workers_keys[0]
            buffer_stream_plan.append([best_worker_key])

            self.update_worker_planned_resource(
                worker_pool[best_worker_key], min_queue_space_percent
            )

        buffer_stream_plan.append([self.ce_endpoint_stream_key])
        return buffer_stream_plan

    def create_buffer_stream_choices_plan(self, buffer_stream_entity):
        buffer_stream_plan = self.create_buffer_stream_plan(buffer_stream_entity)
        plan_cum_weight = None
        return [(plan_cum_weight, buffer_stream_plan)]


