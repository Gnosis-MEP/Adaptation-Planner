import functools
import itertools
from ..conf import QUERY_SERVICE_CHAIN_FIELD
from .scheduling import BaseSchedulerPlanner


class BaseQoSSchedulerPlanner(BaseSchedulerPlanner):
    """Base class for scheduler planning"""

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

    def create_buffer_stream_plan(self, buffer_stream_entity):
        required_services = self.get_buffer_stream_required_services(buffer_stream_entity)

        buffer_stream_plan = []
        for service in required_services:
            worker_pool = self.all_services_worker_pool[service]
            selected_worker_pool = worker_pool
            qos_sorted_workers_keys = self.workers_key_sorted_by_qos(selected_worker_pool)
            best_worker_key = qos_sorted_workers_keys[0]
            buffer_stream_plan.append([best_worker_key])
        buffer_stream_plan.append([self.ce_endpoint_stream_key])
        return buffer_stream_plan

    def create_buffer_stream_choices_plan(self, buffer_stream_entity):
        buffer_stream_plan = self.create_buffer_stream_plan(buffer_stream_entity)
        plan_cum_weight = None
        return [(plan_cum_weight, buffer_stream_plan)]
