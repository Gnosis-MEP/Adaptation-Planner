import functools
import itertools
from ..conf import QUERY_SERVICE_CHAIN_FIELD


class BaseSchedulerPlanner(object):
    """Base class for scheduler planning"""

    def __init__(self, parent_service, scheduler_cmd_stream_key, ce_endpoint_stream_key):
        super(BaseSchedulerPlanner, self).__init__()
        self.parent_service = parent_service
        self.scheduler_cmd_stream = self.parent_service.get_destination_streams(scheduler_cmd_stream_key)
        self.ce_endpoint_stream_key = ce_endpoint_stream_key
        self.all_services_worker_pool = {}
        self.all_buffer_streams = {}
        self.all_queries = {}

    def get_query_required_services(self, query):
        if QUERY_SERVICE_CHAIN_FIELD in query.keys():
            return list(set(filter(lambda x: x != 'WindowManager', query[QUERY_SERVICE_CHAIN_FIELD])))

        # backward compat so that if no service chain is present, we use a mocked one
        services = [('object_detection', 'ObjectDetection')]

        required_services = []
        for service in services:
            if service[0] in query['query_text'].lower():
                required_services.append(service[1])

        return required_services

    def ask_knowledge_for_all_entities_of_namespace(self, namespace):
        k_query_text = """
        SELECT DISTINCT ?s ?p ?o
            WHERE {
                ?s ?p ?o.
                ?s rdf:type ?t.
            }
        """
        query = {
            'query_text': k_query_text,
            'bindings': {
                't': namespace,  # 'gnosis-mep:buffer_stream'
            },
            # for simple internal reference of this query.
            'query_ref': f'{self.parent_service.service_based_random_event_id()}-{namespace}',
        }
        self.parent_service.query_knowledge(query)
        return query

    def get_buffer_stream_required_services(self, buffer_stream_entity):
        required_services = []
        for query in buffer_stream_entity['queries'].values():
            required_services.extend(self.get_query_required_services(query))

        return required_services

    def create_scheduling_plan(self):
        scheduling_dataflows = {}
        for buffer_stream_key, buffer_stream_entity in self.all_buffer_streams.items():
            buffer_stream_plan = self.create_buffer_stream_choices_plan(buffer_stream_entity)
            scheduling_dataflows[buffer_stream_key] = buffer_stream_plan
        return {
            'strategy': {
                'name': self.strategy_name,
                'dataflows': scheduling_dataflows
            }
        }

    def plan_stage_preparation_start(self, cause, plan):
        plan['stage'] = self.parent_service.PLAN_STAGE_WAITING_KNOWLEDGE_QUERIES
        ongoing_knowledge_queries = plan.setdefault('ongoing_knowledge_queries', {})

        queries_knowledge_query = self.ask_knowledge_for_all_entities_of_namespace('gnosis-mep:subscriber_query')
        ongoing_knowledge_queries[queries_knowledge_query['query_ref']] = queries_knowledge_query

        buffers_knowledge_query = self.ask_knowledge_for_all_entities_of_namespace('gnosis-mep:buffer_stream')
        ongoing_knowledge_queries[buffers_knowledge_query['query_ref']] = buffers_knowledge_query

        services_knowledge_query = self.ask_knowledge_for_all_entities_of_namespace('gnosis-mep:service_worker')
        ongoing_knowledge_queries[services_knowledge_query['query_ref']] = services_knowledge_query
        return plan

    def prepare_data_structures_from_knowledge_queries_data(self, ongoing_knowledge_queries):
        self.all_queries = {}
        self.all_buffer_streams = {}
        self.all_services_worker_pool = {}
        self.prepare_local_queries_entities(ongoing_knowledge_queries)
        self.prepare_local_services_with_workers(ongoing_knowledge_queries)
        self.prepare_local_buffer_stream_entities(ongoing_knowledge_queries)

    def prepare_local_queries_entities(self, knowledge_queries):
        # get the query about the subscriber_query
        query_ref = next(filter(lambda q: 'gnosis-mep:subscriber_query' in q, knowledge_queries))
        query = knowledge_queries[query_ref]
        data_triples = query['data']

        for subj, pred, obj in data_triples:
            query_id = subj.split('/')[-1]
            attribute = pred.split('#')[-1]
            value = obj
            self.all_queries.setdefault(query_id, {QUERY_SERVICE_CHAIN_FIELD: []})
            if attribute == 'query':
                attribute = 'query_text'

            if attribute not in self.all_queries[query_id].keys():
                self.all_queries[query_id][attribute] = value
            else:
                if not isinstance(self.all_queries[query_id][attribute], list):
                    self.all_queries[query_id][attribute] = [
                        self.all_queries[query_id][attribute]
                    ]
                self.all_queries[query_id][attribute].append(value)

    def prepare_local_buffer_stream_entities(self, knowledge_queries):
        # get the query about the buffer streams
        query_ref = next(filter(lambda q: 'gnosis-mep:buffer_stream' in q, knowledge_queries))
        query = knowledge_queries[query_ref]
        data_triples = query['data']
        # subject_sorted_triples = sorted(data_triples, key=lambda t: t[0])
        # buffer_stream_keys = set([o for s, p, o in subject_sorted_triples if '#buffer_stream_key' in p])
        buffer_entity_id_stream_key_map = dict(
            [(s, o) for s, p, o in data_triples if '#buffer_stream_key' in p])

        for subj, pred, obj in data_triples:
            buffer_stream_key = buffer_entity_id_stream_key_map[subj]
            attribute = pred.split('#')[-1]
            value = obj
            self.all_buffer_streams.setdefault(buffer_stream_key, {})
            if attribute == 'query_ids':
                queries = self.all_buffer_streams[buffer_stream_key].setdefault('queries', {})
                query_id = value.split('/')[-1]
                queries[query_id] = self.all_queries[query_id]
                continue

            if attribute not in self.all_buffer_streams[buffer_stream_key].keys():
                self.all_buffer_streams[buffer_stream_key][attribute] = value
            else:
                if not isinstance(self.all_buffer_streams[buffer_stream_key][attribute], list):
                    self.all_buffer_streams[buffer_stream_key][attribute] = [
                        self.all_buffer_streams[buffer_stream_key][attribute]
                    ]
                self.all_buffer_streams[buffer_stream_key][attribute].append(value)

    def prepare_local_services_with_workers(self, knowledge_queries):
        query_ref = next(filter(lambda q: 'gnosis-mep:service_worker' in q, knowledge_queries))
        query = knowledge_queries[query_ref]
        data_triples = query['data']
        workers = {}
        for subj, pred, obj in data_triples:
            worker_id = subj.split('/')[-1]
            attribute = pred.split('#')[-1]
            value = obj
            if attribute in ['queue_space', 'queue_limit']:
                value = int(value)
            elif attribute in ['queue_space_percent']:
                value = float(value)

            worker_monitoring_dict = workers.setdefault(worker_id, {})
            worker_monitoring_dict[attribute] = value

        for worker_id, worker_monitoring_dict in workers.items():
            service_type = worker_monitoring_dict['service_type']
            service_dict = self.all_services_worker_pool.setdefault(service_type, {})

            service_worker_dict = service_dict.setdefault(worker_id, {})
            service_worker_dict_monitoring = service_worker_dict.setdefault('monitoring', {})
            service_worker_dict_monitoring.update(worker_monitoring_dict)
            service_worker_dict_resources = service_worker_dict.setdefault('resources', {'planned': {}, 'usage': {}})
            service_worker_dict_resources['planned']['queue_space'] = service_worker_dict_monitoring['queue_space']
            if 'energy_consumption' in service_worker_dict_monitoring:
                service_worker_dict_resources['usage']['energy_consumption'] = float(
                    service_worker_dict_monitoring['energy_consumption']
                )

    def plan_stage_waiting_knowledge_queries(self, cause, plan):
        ongoing_knowledge_queries = plan.get('ongoing_knowledge_queries', {})
        if self.parent_service.check_ongoing_knowledge_queries_are_done(ongoing_knowledge_queries):
            try:
                self.prepare_data_structures_from_knowledge_queries_data(ongoing_knowledge_queries)
            except Exception as e:
                self.parent_service.logger.warning(
                    "Could not prepare the data structures from the Knowledge queries data. Will ignore this planning request"
                )
                self.parent_service.logger.exception(e)
            else:
                plan['ongoing_knowledge_queries'] = {}
                execution_plan = self.create_scheduling_plan()
                plan['execution_plan'] = execution_plan
                plan['stage'] = self.parent_service.PLAN_STAGE_IN_EXECUTION
                self.parent_service.last_executed = plan
                self.send_plan_to_scheduler(execution_plan)
        else:
            pass

        return plan

    def send_plan_to_scheduler(self, adaptive_plan):
        new_event_data = {
            'id': self.parent_service.service_based_random_event_id(),
            'action': 'executeAdaptivePlan',
        }
        new_event_data.update(adaptive_plan)
        self.parent_service.logger.debug(f'Sending event "{new_event_data}" to Scheduler')
        self.parent_service.write_event_with_trace(new_event_data, self.scheduler_cmd_stream)

    def plan(self, change_request=None, plan=None):
        if plan is None:
            plan = {}
            plan.update({
                'type': 'reSchedulingPlan',
                'execution_plan': None,
                'executor': self.scheduler_cmd_stream,
                'change_request': change_request
            })
            plan = self.parent_service.prepare_plan(plan)

        if change_request is None:
            change_request = plan['change_request']

        cause = change_request['cause']
        if plan['stage'] == self.parent_service.PLAN_STAGE_PREPARATION_START:
            plan = self.plan_stage_preparation_start(cause, plan)

        elif plan['stage'] == self.parent_service.PLAN_STAGE_WAITING_KNOWLEDGE_QUERIES:
            plan = self.plan_stage_waiting_knowledge_queries(cause, plan)
        return plan


class SimpleFixedSchedulerPlanner(BaseSchedulerPlanner):
    """
        This is the very basic scheduler, which, based on the mocked object detection stream key configuration,
        it will always use that as the only available destination for events scheduled
    """

    def __init__(self, parent_service, scheduler_cmd_stream_key, ce_endpoint_stream_key, mocked_od_stream_key):
        super(SimpleFixedSchedulerPlanner, self).__init__(
            parent_service,
            scheduler_cmd_stream_key,
            ce_endpoint_stream_key
        )

        self.strategy_name = 'single_best'
        self.mocked_od_stream_key = mocked_od_stream_key
        self.services_to_streams = {
            'ObjectDetection': self.mocked_od_stream_key
        }

    def create_buffer_stream_plan(self, buffer_stream_entity):
        required_services = self.get_buffer_stream_required_services(buffer_stream_entity)
        buffer_stream_plan = []
        for service in required_services:
            service_stream = self.services_to_streams[service]
            buffer_stream_plan.append([service_stream])

        buffer_stream_plan.append([self.ce_endpoint_stream_key])

        return buffer_stream_plan

    def create_buffer_stream_choices_plan(self, buffer_stream_entity):
        buffer_stream_plan = self.create_buffer_stream_plan(buffer_stream_entity)
        plan_cum_weight = None
        return [(plan_cum_weight, buffer_stream_plan)]


class MaxEnergyForQueueLimitSchedulerPlanner(BaseSchedulerPlanner):
    """
    Based on VideoEdge, limits queue size to reduce latency, and total execution time.
    Overloaded workers (>70% queue size) are not considered as valid workers
    (unless all workers are overloaded, then all are considered)
    from the list of valid workers for each bufferstream required service dataflow,
    the planner selects only the best one in terms of energy consumption
    """

    def __init__(self, parent_service, scheduler_cmd_stream_key, ce_endpoint_stream_key):
        super(MaxEnergyForQueueLimitSchedulerPlanner, self).__init__(
            parent_service,
            scheduler_cmd_stream_key,
            ce_endpoint_stream_key
        )
        self.strategy_name = 'single_best'

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

    def create_buffer_stream_plan(self, buffer_stream_entity):
        required_services = self.get_buffer_stream_required_services(buffer_stream_entity)
        min_queue_space_percent = 0.3

        buffer_stream_plan = []
        for service in required_services:
            worker_pool = self.all_services_worker_pool[service]
            non_floaded_worker_pool = self.get_non_floaded_queue_workers(worker_pool, min_queue_space_percent)
            selected_worker_pool = non_floaded_worker_pool
            if len(selected_worker_pool) == 0:
                selected_worker_pool = worker_pool
            energy_sorted_workers_keys = self.workers_keys_sorted_by_best_energy_consumption(selected_worker_pool)
            best_worker_key = energy_sorted_workers_keys[0]
            buffer_stream_plan.append([best_worker_key])
            worker_pool[best_worker_key] = self.update_worker_planned_resource(
                worker_pool[best_worker_key], min_queue_space_percent
            )
        buffer_stream_plan.append([self.ce_endpoint_stream_key])
        return buffer_stream_plan

    def create_buffer_stream_choices_plan(self, buffer_stream_entity):
        buffer_stream_plan = self.create_buffer_stream_plan(buffer_stream_entity)
        plan_cum_weight = None
        return [(plan_cum_weight, buffer_stream_plan)]


class WeightedRandomMaxEnergyForQueueLimitSchedulerPlanner(BaseSchedulerPlanner):
    """
        Weighted random planner that prepares a list of choises
        for the scheduler to randomize from for each buffer stream.
        The scheduler randomization utilizes the weights defined by this planner,
        which is basically a the inverse of the total sum of energy consumption for that dataflow choise.
    """

    def __init__(self, parent_service, scheduler_cmd_stream_key, ce_endpoint_stream_key):
        super(WeightedRandomMaxEnergyForQueueLimitSchedulerPlanner, self).__init__(
            parent_service,
            scheduler_cmd_stream_key,
            ce_endpoint_stream_key
        )
        self.strategy_name = 'weighted_random'

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

    def get_service_nonfloaded_workers(self, service, min_queue_space_percent):
        worker_pool = self.all_services_worker_pool[service]
        non_floaded_worker_pool = self.get_non_floaded_queue_workers(worker_pool, min_queue_space_percent)

        selected_worker_pool = non_floaded_worker_pool
        # # check what to do here
        if len(non_floaded_worker_pool) == 0:
            selected_worker_pool = worker_pool

        return list(map(
            lambda kv: (service, kv[0], self.get_worker_choice_weight(kv[1])),
            selected_worker_pool.items()
        ))

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

    def update_workers_planned_resources(self, dataflow_choice_weighted, dataflow_weight, total_cum_weight, min_queue_space_percent):
        # Change of getting this dataflow
        resource_usage_rating = (dataflow_weight / total_cum_weight)

        # change of getting best dataflow times the expected usage before a new plan would be required
        resource_usage = resource_usage_rating * min_queue_space_percent
        cum_weight, dataflow = dataflow_choice_weighted
        for worker in dataflow:
            service_type, worker_key, _ = worker
            actual_worker_reference = self.all_services_worker_pool[service_type][worker_key]
            self.update_worker_planned_resource(actual_worker_reference, resource_usage)

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


class RandomSchedulerPlanner(WeightedRandomMaxEnergyForQueueLimitSchedulerPlanner):

    def __init__(self, parent_service, scheduler_cmd_stream_key, ce_endpoint_stream_key):
        super(RandomSchedulerPlanner, self).__init__(
            parent_service,
            scheduler_cmd_stream_key,
            ce_endpoint_stream_key
        )
        self.strategy_name = 'random'

    def create_scheduling_plan(self):
        scheduling_plan = super(RandomSchedulerPlanner, self).create_scheduling_plan()
        scheduling_plan['strategy']['name'] = self.strategy_name
        return scheduling_plan

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

    def __init__(self, parent_service, scheduler_cmd_stream_key, ce_endpoint_stream_key):
        super(RoundRobinSchedulerPlanner, self).__init__(
            parent_service,
            scheduler_cmd_stream_key,
            ce_endpoint_stream_key
        )
        self.strategy_name = 'round_robin'
