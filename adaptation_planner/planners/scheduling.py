import functools
import random


class SchedulerPlanner(object):
    def __init__(self, parent_service, scheduler_cmd_stream_key, ce_endpoint_stream_key):
        super(SchedulerPlanner, self).__init__()
        self.parent_service = parent_service
        self.scheduler_cmd_stream = self.parent_service.get_destination_streams(scheduler_cmd_stream_key)
        self.ce_endpoint_stream_key = ce_endpoint_stream_key

        self.services_to_streams = {
            'object_detection': 'object-detection-data'
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
            #for simple internal reference of this query.
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

        # self.workers = {
        #     'ObjectDetection': {
        #         'w1': {
        #             'resources': {
        #                 'usage': {
        #                     'energy_consumption': 10,
        #                     'time': 2,
        #                 },
        #                 'planned': {
        #                     'queue_space': 5,
        #                 }
        #             },
        #             'monitoring': {
        #                 'queue_space': 5,
        #                 'queue_space_percent': 0.05,
        #                 'queue_limit': 100
        #             },
        #         },
        #         'w2': {
        #             'resources': {
        #                 'usage': {
        #                     'energy_consumption': 10,
        #                     'time': 2,
        #                 },
        #                 'planned': {
        #                     'queue_space': 5,
        #                 }
        #             },
        #             'monitoring': {
        #                 'queue_space': 5,
        #                 'queue_space_percent': 0.05,
        #                 'queue_limit': 100
        #             },
        #         },
        #     },
        # }

    # def _check_worker_resource_is_fits_query_constraints(self, resource, constraint_function, worker_key_val):
    #     worker_dict = worker_key_val[1]
    #     worker_resource_usage = worker_dict['resource_usage'][resource]
    #     return constraint_function(worker_resource_usage)

    # def get_query_resource_contrained_workers(self, worker_pool, query_resource_constraints):
    #     resource_bounded_workers = worker_pool
    #     for resource, contraint_function in query_resource_constraints:
    #         filter_resource_worker_function = functools.partial(
    #             self._check_worker_resource_is_fits_query_constraints, resource, contraint_function
    #         )

    #         # filter out workers that are not within the this query resource constraint
    #         next_resource_bounded_workers = dict(
    #             filter(filter_resource_worker_function, resource_bounded_workers.items())
    #         )

    #         # if there is at least a worker... then keepgoing,
    #         # othersise, just ignore this resource constraint, since there's not much else to do
    #         if next_resource_bounded_workers:
    #             resource_bounded_workers = next_resource_bounded_workers
    #         else:
    #             continue

    # ----------------mocked since we don't have this yet
    def get_query_required_services(self, query):
        services = [('object_detection', 'Object Detection')]

        required_services = []
        for service in services:
            if services[0] in query['query_text'].lower():
                required_services.append(service[1])

        return required_services

    def mocked_preparation_stage(self, cause, plan):
        """Pretending this info was received by queries from the k and
        formated into this structure
        """
        self.all_buffer_streams = {
            'buffer_stream_key': {
                'query_ids': [],
                'queries': {
                    'id': '',
                    'query_text': ''
                }
            }
        }
        self.all_queries = {

        }

        self.all_services_worker_pool = {
            'ObjectDetection': {
                'w1': {
                    'resources': {
                        'usage': {
                            'energy_consumption': 10,
                            'time': 2,
                        },
                        'planned': {
                            'queue_space': 5,
                        }
                    },
                    'monitoring': {
                        'queue_space': 5,
                        'queue_space_percent': 0.05,
                        'queue_limit': 100
                    },
                },
                'w2': {
                    'resources': {
                        'usage': {
                            'energy_consumption': 10,
                            'time': 2,
                        },
                        'planned': {
                            'queue_space': 5,
                        }
                    },
                    'monitoring': {
                        'queue_space': 5,
                        'queue_space_percent': 0.05,
                        'queue_limit': 100
                    },
                },
            },
        }

        self.plan_stage_waiting_knowledge_queries(cause, plan)
    # ----------------end of mocked

    def calculate_worker_queue_space_percentage(self, worker):
        return worker['resource']['planned']['queue_space'] / worker['monitoring']['queue_limit']

    def get_non_floaded_queue_workers(self, worker_pool, min_queue_space_percent):
        return dict(filter(
            lambda x: self.calculate_worker_queue_space_percentage(x[1]) > min_queue_space_percent, worker_pool.items()
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
            worker_pool.keys(), key=lambda w_key: worker_pool[w_key]['resource_usage']['energy_consumption']
        )

        return sorted_workers

    def update_worker_planned_resource(self, worker, min_queue_space_percent):
        planned_usage = worker['monitoring']['queue_limit'] * (min_queue_space_percent)
        worker['resource']['planned']['queue_space'] -= planned_usage
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
            non_floaded_worker_pool = self.get_non_floaded_queue_workers_or_all(worker_pool, min_queue_space_percent)
            if len(worker_pool) == 0:
                best_worker = random.choice(list(worker_pool.keys()))

            energy_sorted_workers_keys = self.workers_sorted_by_best_energy_consumption(non_floaded_worker_pool)
            best_worker_key = energy_sorted_workers_keys[0]
            buffer_stream_plan.append([best_worker_key])
            worker_pool[best_worker] = self.update_worker_planned_resource(worker_pool[best_worker])
        buffer_stream_plan.append([self.ce_endpoint_stream_key])
        return buffer_stream_plan

    def create_scheduling_plan(self):
        scheduling_dataflow_plan = {}
        for buffer_stream_key, buffer_stream_entity in self.all_buffer_streams.items():
            buffer_stream_plan = self.create_buffer_stream_plan(buffer_stream_entity)
            buffer_stream_key = buffer_stream_entity['gnosis-mep:buffer_stream#buffer_stream_key']
            scheduling_dataflow_plan[buffer_stream_key] = buffer_stream_plan

        return {
            'dataflow': scheduling_dataflow_plan
        }

    def plan_stage_preparation_start(self, cause, plan):
        ongoing_knowledge_queries = plan.setdefault('ongoing_knowledge_queries', {})

        buffers_knowledge_query = self.ask_knowledge_for_buffer_stream_entities()
        ongoing_knowledge_queries[buffers_knowledge_query['query_ref']] = buffers_knowledge_query

        queries_knowledge_query = self.ask_knowledge_for_queries_entities()
        ongoing_knowledge_queries[queries_knowledge_query['query_ref']] = queries_knowledge_query

        services_knowledge_query = self.ask_knowledge_for_services_entities()
        ongoing_knowledge_queries[services_knowledge_query['query_ref']] = services_knowledge_query

        plan['stage'] = self.parent_service.PLAN_STAGE_WAITING_KNOWLEDGE_QUERIES
        return plan

    def prepare_local_buffer_stream_entities(self, knowledge_queries):
        self.all_buffer_streams = {}

    def prepare_local_services_with_workers(self, knowledge_queries):
        self.all_services_worker_pool = {}

    def prepare_data_structures_from_knowledge_queries_data(self, plan):
        is_mocked = len(plan['ongoing_knowledge_queries'].keys()) == 0
        if not is_mocked:
            self.prepare_local_buffer_stream_entities(plan['ongoing_knowledge_queries'])
            self.prepare_local_services_with_workers(plan['ongoing_knowledge_queries'])

    def plan_stage_waiting_knowledge_queries(self, cause, plan):
        ongoing_knowledge_queries = plan.get('ongoing_knowledge_queries', {})
        if self.parent_service.check_ongoing_knowledge_queries_are_done(ongoing_knowledge_queries):
            self.prepare_data_structures_from_knowledge_queries_data(ongoing_knowledge_queries)
            plan['ongoing_knowledge_queries'] = {}
            execution_plan = self.create_scheduling_plan()
            plan['execution_plan'] = execution_plan
            plan['stage'] = self.parent_service.PLAN_STAGE_IN_EXECUTION
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
            plan = self.plan_stage_waiting_kowledge(cause, plan)
        self.parent_service.update_plan_on_knoledge(plan)
        return plan
