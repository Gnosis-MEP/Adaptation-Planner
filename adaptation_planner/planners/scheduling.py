import random


class SchedulerPlanner(object):
    def __init__(self, parent_service, scheduler_cmd_stream_key, ce_endpoint_stream_key, mocked_od_stream_key):
        super(SchedulerPlanner, self).__init__()
        self.parent_service = parent_service
        self.scheduler_cmd_stream = self.parent_service.get_destination_streams(scheduler_cmd_stream_key)
        self.ce_endpoint_stream_key = ce_endpoint_stream_key

        self.services_to_streams = {
            'object_detection': mocked_od_stream_key
        }

    #-- Mocked data until we have this info somewere
    def get_queries_required_services(self, query_text_list):
        required_services = []
        if any(['object_detection' in query_text.lower() for query_text in query_text_list]):
            required_services.append('object_detection')
        return required_services
    #-- end of mocked data

    def build_buffer_stream_plan_from_required_services(self, buffer_stream_key, required_services):
        buffer_stream_plan = []
        for service in required_services:
            service_stream = self.services_to_streams[service]
            buffer_stream_plan.append([service_stream])

        buffer_stream_plan.append([self.ce_endpoint_stream_key])

        return {
            'dataflow': {
                buffer_stream_key: buffer_stream_plan,
            }
        }

    def ask_knowledge_for_queries_text_from_ids(self, query_ids):
        # only get first query for now, since there's no diff in geting more than one for the same buffer
        k_query_text = "SELECT DISTINCT ?o WHERE {?s ?p ?o.}"
        query = {
            'query_text': k_query_text,
            'bindings': {
                's': query_ids[0],
                'p': 'gnosis-mep:subscriber_query#query'
            },
            # for simple internal reference of this query.
            'query_ref': self.parent_service.service_based_random_event_id(),
        }
        self.parent_service.query_knowledge(query)
        return query

    def plan_stage_preparation_start(self, cause, plan):
        buffer_stream_entity = cause
        query_ids = buffer_stream_entity['gnosis-mep:buffer_stream#query_ids']
        knowledge_query = self.ask_knowledge_for_queries_text_from_ids(query_ids)

        plan['stage'] = self.parent_service.PLAN_STAGE_WAITING_KNOWLEDGE_QUERIES
        knowledge_queries = plan.setdefault('ongoing_knowledge_queries', {})
        query_ref = knowledge_query['query_ref']
        knowledge_queries[query_ref] = knowledge_query
        return plan

    def plan_stage_waiting_kowledge(self, cause, plan):
        buffer_stream_entity = cause
        if self.parent_service.check_ongoing_knowledge_queries_are_done(plan.get('ongoing_knowledge_queries', {})):
            k_response = list(plan['ongoing_knowledge_queries'].values())[0].get('data', [])
            plan['ongoing_knowledge_queries'] = {}
            query_text_list = [t[0] for t in k_response]

            required_services = self.get_queries_required_services(query_text_list)
            buffer_stream_key = buffer_stream_entity['gnosis-mep:buffer_stream#buffer_stream_key']
            buffer_stream_plan = self.build_buffer_stream_plan_from_required_services(
                buffer_stream_key, required_services)

            plan['execution_plan'] = buffer_stream_plan
            plan['stage'] = self.parent_service.PLAN_STAGE_IN_EXECUTION
            self.send_plan_to_scheduler(buffer_stream_plan)
        else:
            pass

        return plan

    def plan(self, change_request=None, plan=None):
        if plan is None:
            plan = {}
            plan.update({
                'type': 'correctSchedulerPlan',
                'execution_plan': None,
                'executor': 'sc-cmd',
                'change_request': change_request
            })
            plan = self.parent_service.prepare_plan(plan)

        if change_request is None:
            change_request = plan['change_request']

        cause = change_request['cause']
        if plan['stage'] == self.parent_service.PLAN_STAGE_PREPARATION_START:
            plan = self.plan_stage_preparation_start(cause, plan)

        elif plan['stage'] == self.parent_service.PLAN_STAGE_WAITING_KNOWLEDGE_QUERIES:
            plan = self.plan_stage_waiting_kowledge(cause, plan)
        self.parent_service.update_plan_on_knoledge(plan)
        return plan

    def send_plan_to_scheduler(self, adaptive_plan):
        new_event_data = {
            'id': self.parent_service.service_based_random_event_id(),
            'action': 'executeAdaptivePlan',
        }
        new_event_data.update(adaptive_plan)
        self.parent_service.logger.debug(f'Sending event "{new_event_data}" to Scheduler')
        self.parent_service.write_event_with_trace(new_event_data, self.scheduler_cmd_stream)


class MaxEnergyForQueueLimitSchedulerPlanner(object):
    """Based on VideoEdge"""

    def __init__(self, parent_service, scheduler_cmd_stream_key, ce_endpoint_stream_key):
        super(MaxEnergyForQueueLimitSchedulerPlanner, self).__init__()
        self.parent_service = parent_service
        self.scheduler_cmd_stream = self.parent_service.get_destination_streams(scheduler_cmd_stream_key)
        self.ce_endpoint_stream_key = ce_endpoint_stream_key
        self.all_services_worker_pool = {}
        self.all_buffer_streams = {}
        self.all_queries = {}

    # ----------------mocked since we don't have this yet

    def get_query_required_services(self, query):
        services = [('object_detection', 'ObjectDetection')]

        required_services = []
        for service in services:
            if service[0] in query['query_text'].lower():
                required_services.append(service[1])

        return required_services

    def mocked_buffer_streams(self):
        self.all_buffer_streams = {
            'f32c1d9e6352644a5894305ecb478b0d': {
                'query_ids': ['f860ba666ed657944d19ca051e58cd2c'],
                'queries': {
                    'f860ba666ed657944d19ca051e58cd2c': {
                        'query_text': 'select object_detection from publisher1 where (object1.label = car) within TUMBLING_COUNT_WINDOW(4) withconfidence >50'
                    }
                }
            }
        }
        self.all_queries = {
            'f860ba666ed657944d19ca051e58cd2c': {
                'query_text': 'select object_detection from publisher1 where (object1.label = car) within TUMBLING_COUNT_WINDOW(4) withconfidence >50'
            }
        }

    def mocked_services(self):
        self.all_services_worker_pool = {
            'ObjectDetection': {
                # 'od-yolo-data': {
                #     'resources': {
                #         'usage': {
                #             'energy_consumption': 20,
                #             'time': 1,
                #         },
                #         'planned': {
                #             'queue_space': 30,
                #         }
                #     },
                #     'monitoring': {
                #         'queue_space': 30,
                #         'queue_space_percent': 0.30,
                #         'queue_limit': 100
                #     },
                # },
                'object-detection-ssd-gpu-data': {
                    'resources': {
                        'usage': {
                            'energy_consumption': 6,
                            'time': 1,
                        },
                        'planned': {
                        #     'queue_space': 35,
                        }
                    },
                    # 'monitoring': {
                    #     'queue_space': 35,
                    #     'queue_space_percent': 0.35,
                    #     'queue_limit': 100
                    # },
                },
                'object-detection-ssd-data': {
                    'resources': {
                        'usage': {
                            'energy_consumption': 10,
                            'time': 1,
                        },
                        'planned': {
                        #     'queue_space': 45,
                        }
                    },
                    # 'monitoring': {
                    #     'queue_space': 45,
                    #     'queue_space_percent': 0.45,
                    #     'queue_limit': 100
                    # },
                },
            },
        }

    # ----------------end of mocked

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

    def calculate_worker_queue_space_percentage(self, worker):
        return worker['resources']['planned']['queue_space'] / worker['monitoring']['queue_limit']

    def get_non_floaded_queue_workers(self, worker_pool, min_queue_space_percent):
        return dict(filter(
            lambda x: self.calculate_worker_queue_space_percentage(x[1]) >= min_queue_space_percent, worker_pool.items()
        ))

    # def workers_sorted_by_best_energy_consumption(self, worker_pool):
    #     sorted_workers = sorted(worker_pool.items(), key=lambda w: w[1]['resource_usage']['energy_consumption'])

    #     workers_by_energy_consumption = dict(map(
    #         lambda w: (w[1]['resource_usage']['energy_consumption'], w[0]),
    #         sorted_workers
    #     ))

    #     return workers_by_energy_consumption

    def workers_keys_sorted_by_best_energy_consumption(self, worker_pool):
        sorted_workers = sorted(
            worker_pool.keys(), key=lambda w_key: worker_pool[w_key]['resources']['usage']['energy_consumption']
        )

        return sorted_workers

    def update_worker_planned_resource(self, worker, min_queue_space_percent):
        planned_usage = int(worker['monitoring']['queue_limit'] * (min_queue_space_percent))
        worker['resources']['planned']['queue_space'] -= planned_usage
        return worker

    def get_buffer_stream_required_services(self, buffer_stream_entity):
        required_services = []
        for query in buffer_stream_entity['queries'].values():
            required_services.extend(self.get_query_required_services(query))

        return required_services

    def create_buffer_stream_plan(self, buffer_stream_entity):
        required_services = self.get_buffer_stream_required_services(buffer_stream_entity)
        min_queue_space_percent = 0.3

        buffer_stream_plan = []
        for service in required_services:
            worker_pool = self.all_services_worker_pool[service]
            non_floaded_worker_pool = self.get_non_floaded_queue_workers(worker_pool, min_queue_space_percent)
            if len(worker_pool) == 0:
                best_worker_key = random.choice(list(worker_pool.keys()))
            energy_sorted_workers_keys = self.workers_keys_sorted_by_best_energy_consumption(non_floaded_worker_pool)
            best_worker_key = energy_sorted_workers_keys[0]
            buffer_stream_plan.append([best_worker_key])
            worker_pool[best_worker_key] = self.update_worker_planned_resource(
                worker_pool[best_worker_key], min_queue_space_percent
            )
        buffer_stream_plan.append([self.ce_endpoint_stream_key])
        return buffer_stream_plan

    def create_scheduling_plan(self):
        scheduling_dataflow_plan = {}
        for buffer_stream_key, buffer_stream_entity in self.all_buffer_streams.items():
            buffer_stream_plan = self.create_buffer_stream_plan(buffer_stream_entity)
            scheduling_dataflow_plan[buffer_stream_key] = buffer_stream_plan

        return {
            'dataflow': scheduling_dataflow_plan
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

    def prepare_local_queries_entities(self, knowledge_queries):
        # get the query about the subscriber_query
        query_ref = next(filter(lambda q: 'gnosis-mep:subscriber_query' in q, knowledge_queries))
        query = knowledge_queries[query_ref]
        data_triples = query['data']

        for subj, pred, obj in data_triples:
            query_id = subj.split('/')[-1]
            attribute = pred.split('#')[-1]
            value = obj
            self.all_queries.setdefault(query_id, {})
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
        self.mocked_services()
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
            if attribute == 'service_type':
                service_dict = self.all_services_worker_pool.setdefault(value, {})
                service_worker_dict = service_dict.setdefault(worker_id, {})
                service_worker_dict_monitoring = service_worker_dict.setdefault('monitoring', {})
                service_worker_dict_resources = service_worker_dict.setdefault('resources', {'planned': {}})
                service_worker_dict_monitoring.update(worker_monitoring_dict)
                print(f'{worker_id}')
                service_worker_dict_resources['planned']['queue_space'] = service_worker_dict_monitoring['queue_space']
            else:
                worker_monitoring_dict[attribute] = value
        # for worker_id, worker in workers.items():
        #     service_type = worker['service_type']
        #     service_dict[worker_id] = worker

    def prepare_data_structures_from_knowledge_queries_data(self, ongoing_knowledge_queries):
        self.all_queries = {}
        self.all_buffer_streams = {}
        self.all_services_worker_pool = {}
        self.prepare_local_queries_entities(ongoing_knowledge_queries)
        self.prepare_local_services_with_workers(ongoing_knowledge_queries)
        self.prepare_local_buffer_stream_entities(ongoing_knowledge_queries)

    def plan_stage_waiting_knowledge_queries(self, cause, plan):
        ongoing_knowledge_queries = plan.get('ongoing_knowledge_queries', {})
        if self.parent_service.check_ongoing_knowledge_queries_are_done(ongoing_knowledge_queries):
            self.prepare_data_structures_from_knowledge_queries_data(ongoing_knowledge_queries)
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
            # plan = self.mocked_preparation_stage(cause, plan)
            plan = self.plan_stage_preparation_start(cause, plan)

        elif plan['stage'] == self.parent_service.PLAN_STAGE_WAITING_KNOWLEDGE_QUERIES:
            plan = self.plan_stage_waiting_knowledge_queries(cause, plan)
        return plan
