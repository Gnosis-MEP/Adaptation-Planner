
class BaseSchedulerPlanner(object):
    """Base class for scheduler planning using event driven approach"""

    def __init__(self, parent_service, ce_endpoint_stream_key):
        super(BaseSchedulerPlanner, self).__init__()
        self.parent_service = parent_service
        self.ce_endpoint_stream_key = ce_endpoint_stream_key
        # self.all_services_worker_pool = {}
        # self.all_buffer_streams = {}
        # self.all_queries = {}
        # self.required_services_workload_status = {}
        self.adaptation_delta = 10
        self.strategy_name = None

    # def get_query_required_services(self, query):
    #     service_chain = query['service_chain']
    #     return list(set(filter(lambda x: x != 'WindowManager', service_chain)))

    # def get_buffer_stream_required_services(self, buffer_stream_entity):
    #     required_services = []
    #     for query in buffer_stream_entity.values():
    #         required_services.extend(self.get_query_required_services(query))

    #     return required_services

    # def create_scheduling_plan(self):
    #     scheduling_dataflows = {}
    #     for buffer_stream_key, buffer_stream_entity in self.all_buffer_streams.items():
    #         buffer_stream_plan = self.create_buffer_stream_choices_plan(buffer_stream_entity)
    #         scheduling_dataflows[buffer_stream_key] = buffer_stream_plan
    #     return {
    #         'strategy': {
    #             'name': self.strategy_name,
    #             'dataflows': scheduling_dataflows
    #         }
    #     }

    # def prepare_data_structures_from_knowledge_queries_data(self, ongoing_knowledge_queries):
    #     self.all_queries = {}
    #     self.all_buffer_streams = {}
    #     self.all_services_worker_pool = {}
    #     self.required_services_workload_status = {}
    #     self.prepare_local_queries_entities(ongoing_knowledge_queries)
    #     self.prepare_local_services_with_workers(ongoing_knowledge_queries)
    #     self.prepare_local_buffer_stream_entities(ongoing_knowledge_queries)
    #     self.prepare_required_services_workload_status()

    # def prepare_local_queries_entities(self, knowledge_queries):
    #     # get the query about the subscriber_query
    #     query_ref = next(filter(lambda q: 'gnosis-mep:subscriber_query' in q, knowledge_queries))
    #     query = knowledge_queries[query_ref]
    #     data_triples = query['data']

    #     for subj, pred, obj in data_triples:
    #         query_id = subj.split('/')[-1]
    #         attribute = pred.split('#')[-1]
    #         value = obj
    #         self.all_queries.setdefault(query_id, {QUERY_SERVICE_CHAIN_FIELD: []})
    #         if attribute == 'query':
    #             attribute = 'query_text'

    #         if attribute not in self.all_queries[query_id].keys():
    #             self.all_queries[query_id][attribute] = value
    #         else:
    #             if not isinstance(self.all_queries[query_id][attribute], list):
    #                 self.all_queries[query_id][attribute] = [
    #                     self.all_queries[query_id][attribute]
    #                 ]
    #             self.all_queries[query_id][attribute].append(value)

    # def prepare_local_services_with_workers(self, knowledge_queries):
    #     query_ref = next(filter(lambda q: 'gnosis-mep:service_worker' in q, knowledge_queries))
    #     query = knowledge_queries[query_ref]
    #     data_triples = query['data']
    #     workers = {}
    #     for subj, pred, obj in data_triples:
    #         worker_id = subj.split('/')[-1]
    #         attribute = pred.split('#')[-1]
    #         value = obj
    #         if attribute in ['queue_space', 'queue_limit']:
    #             value = int(value)
    #         elif attribute in ['queue_space_percent']:
    #             value = float(value)

    #         worker_monitoring_dict = workers.setdefault(worker_id, {})
    #         worker_monitoring_dict[attribute] = value

    #     for worker_id, worker_monitoring_dict in workers.items():
    #         service_type = worker_monitoring_dict['service_type']
    #         service_dict = self.all_services_worker_pool.setdefault(service_type, {})
    #         service_worker_dict = service_dict.setdefault(worker_id, {})
    #         service_worker_dict_monitoring = service_worker_dict.setdefault('monitoring', {})
    #         service_worker_dict_monitoring.update(worker_monitoring_dict)
    #         service_worker_dict_resources = service_worker_dict.setdefault('resources', {'planned': {}, 'usage': {}})
    #         service_worker_dict_resources['planned']['queue_space'] = service_worker_dict_monitoring['queue_space']

    #         service_type_workload = self.required_services_workload_status.setdefault(
    #             service_type, {'system': 0, 'input': 0, 'is_overloaded': False})

    #         capacity = math.ceil(float(service_worker_dict_monitoring['throughput']) * self.adaptation_delta)
    #         capacity -= int(service_worker_dict_monitoring['queue_size'])
    #         has_overloaded_worker = capacity < 0
    #         capacity = max(capacity, 0)
    #         service_type_workload['system'] += capacity
    #         service_type_workload['has_overloaded_worker'] = service_type_workload.get('has_overloaded_worker', False) or has_overloaded_worker

    #         if 'energy_consumption' in service_worker_dict_monitoring:
    #             service_worker_dict_resources['usage']['energy_consumption'] = float(
    #                 service_worker_dict_monitoring['energy_consumption']
    #             )

    # def prepare_local_buffer_stream_entities(self, knowledge_queries):
    #     # get the query about the buffer streams
    #     query_ref = next(filter(lambda q: 'gnosis-mep:buffer_stream' in q, knowledge_queries))
    #     query = knowledge_queries[query_ref]
    #     data_triples = query['data']
    #     # subject_sorted_triples = sorted(data_triples, key=lambda t: t[0])
    #     # buffer_stream_keys = set([o for s, p, o in subject_sorted_triples if '#buffer_stream_key' in p])
    #     buffer_entity_id_stream_key_map = dict(
    #         [(s, o) for s, p, o in data_triples if '#buffer_stream_key' in p])

    #     for subj, pred, obj in data_triples:
    #         buffer_stream_key = buffer_entity_id_stream_key_map[subj]
    #         attribute = pred.split('#')[-1]
    #         value = obj
    #         self.all_buffer_streams.setdefault(buffer_stream_key, {})
    #         if attribute == 'query_ids':
    #             queries = self.all_buffer_streams[buffer_stream_key].setdefault('queries', {})
    #             query_id = value.split('/')[-1]
    #             queries[query_id] = self.all_queries[query_id]
    #             continue

    #         if attribute not in self.all_buffer_streams[buffer_stream_key].keys():
    #             self.all_buffer_streams[buffer_stream_key][attribute] = value
    #         else:
    #             if not isinstance(self.all_buffer_streams[buffer_stream_key][attribute], list):
    #                 self.all_buffer_streams[buffer_stream_key][attribute] = [
    #                     self.all_buffer_streams[buffer_stream_key][attribute]
    #                 ]
    #             self.all_buffer_streams[buffer_stream_key][attribute].append(value)

    # def prepare_required_services_workload_status(self):
    #     is_system_overloaded = False

    #     for buffer_stream_key, buffer_stream_entity in self.all_buffer_streams.items():
    #         required_services = self.get_buffer_stream_required_services(buffer_stream_entity)
    #         for service_type in required_services:
    #             service_type_workload = self.required_services_workload_status.setdefault(
    #                 service_type, {'system': 0, 'input': 0, 'is_overloaded': False})

    #             service_type_workload['input'] += (float(buffer_stream_entity['fps']) * self.adaptation_delta)
    #             is_service_type_overloaded = service_type_workload['system'] < service_type_workload['input']
    #             has_overloaded_worker = service_type_workload.get('has_overloaded_worker', False)
    #             is_overloaded = is_service_type_overloaded and has_overloaded_worker
    #             service_type_workload['is_overloaded'] = is_overloaded
    #             if not is_system_overloaded and is_overloaded:
    #                 is_system_overloaded = True

    #     self.required_services_workload_status['_status'] = is_system_overloaded


    def plan(self, change_request):
        plan = self.parent_service.initialize_plan(change_request)

        return plan



class QQoS_TK_LP_LS_SchedulerPlanner(BaseSchedulerPlanner):
    pass
