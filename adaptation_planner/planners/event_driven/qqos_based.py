import math
import copy


class BaseSchedulerPlanner(object):
    """Base class for scheduler planning using event driven approach"""

    def __init__(self, parent_service, ce_endpoint_stream_key):
        super(BaseSchedulerPlanner, self).__init__()
        self.parent_service = parent_service
        self.ce_endpoint_stream_key = ce_endpoint_stream_key
        self.all_services_worker_pool = {}
        self.all_buffer_streams = {}
        self.all_queries = {}
        self.required_services_workload_status = {}
        self.adaptation_delta = 10
        self.strategy_name = None

    def __str__(self):
        return f'{self.strategy_name}'

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

    def prepare_legacy_data_structures(self):
        self.all_queries = {}
        self.all_buffer_streams = {}
        self.all_services_worker_pool = {}
        self.required_services_workload_status = {}
        self.prepare_local_queries_entities()
        self.prepare_local_services_with_workers()
        self.prepare_local_buffer_stream_entities()
        # self.prepare_required_services_workload_status()

    def prepare_local_queries_entities(self):
        self.all_queries = copy.deepcopy(self.parent_service.all_queries)

    def prepare_local_services_with_workers(self):
        all_services_worker_pool_base = copy.deepcopy(self.parent_service.all_services_worker_pool)
        workers = {}
        for service_type, service in all_services_worker_pool_base.items():
            for worker_id, worker in service['workers'].items():
                workers[worker_id] = worker

        for worker_id, worker_monitoring_dict in workers.items():
            service_type = worker_monitoring_dict['service_type']
            service_dict = self.all_services_worker_pool.setdefault(service_type, {})
            service_worker_dict = service_dict.setdefault(worker_id, {})
            service_worker_dict_monitoring = service_worker_dict.setdefault('monitoring', {})
            service_worker_dict_monitoring.update(worker_monitoring_dict)
            service_worker_dict_resources = service_worker_dict.setdefault('resources', {'planned': {}, 'usage': {}})
            service_worker_dict_resources['planned']['queue_space'] = service_worker_dict_monitoring['queue_space']

            service_type_workload = self.required_services_workload_status.setdefault(
                service_type, {'system': 0, 'input': 0, 'is_overloaded': False})

            capacity = math.ceil(float(service_worker_dict_monitoring['throughput']) * self.adaptation_delta)
            capacity -= int(service_worker_dict_monitoring['queue_size'])
            has_overloaded_worker = capacity < 0
            capacity = max(capacity, 0)
            service_type_workload['system'] += capacity
            service_type_workload['has_overloaded_worker'] = service_type_workload.get('has_overloaded_worker', False) or has_overloaded_worker

            if 'energy_consumption' in service_worker_dict_monitoring:
                service_worker_dict_resources['usage']['energy_consumption'] = float(
                    service_worker_dict_monitoring['energy_consumption']
                )

    def prepare_local_buffer_stream_entities(self):
        for query_id, query in self.all_queries.items():
            buffer_stream_dict = query['buffer_stream'].copy()
            buffer_stream_key = buffer_stream_dict['buffer_stream_key']
            self.all_buffer_streams.setdefault(buffer_stream_key, buffer_stream_dict)
            queries = self.all_buffer_streams[buffer_stream_key].setdefault('queries', {})
            queries[query_id] = query

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
        self.prepare_legacy_data_structures()
        import ipdb; ipdb.set_trace()

        return plan


class QQoS_TK_LP_LS_SchedulerPlanner(BaseSchedulerPlanner):

    def __init__(self, parent_service, ce_endpoint_stream_key):
        super(QQoS_TK_LP_LS_SchedulerPlanner, self).__init__(parent_service, ce_endpoint_stream_key)
        self.strategy_name = 'QQoS-TK-LP-LS'
    pass
