import threading

from event_service_utils.logging.decorators import timer_logger
from event_service_utils.services.event_driven import BaseEventDrivenCMDService
from event_service_utils.tracing.jaeger import init_tracer

from adaptation_planner.planners.event_driven.qqos_based import (
    QQoS_TK_LP_LS_SchedulerPlanner
)


class AdaptationPlanner(BaseEventDrivenCMDService):

    def __init__(self,
                 service_stream_key, service_cmd_key_list,
                 pub_event_list, service_details,
                 stream_factory,
                 scheduler_planner_type,
                 logging_level,
                 tracer_configs):
        tracer = init_tracer(self.__class__.__name__, **tracer_configs)
        super(AdaptationPlanner, self).__init__(
            name=self.__class__.__name__,
            service_stream_key=service_stream_key,
            service_cmd_key_list=service_cmd_key_list,
            pub_event_list=pub_event_list,
            service_details=service_details,
            stream_factory=stream_factory,
            logging_level=logging_level,
            tracer=tracer,
        )
        self.cmd_validation_fields = ['id']
        self.data_validation_fields = ['id']

        self.plans_being_planned = {}
        self.last_executed = {}

        self.ce_endpoint_stream_key = 'wm-data'
        self.scheduler_planner_type = scheduler_planner_type
        self.available_scheduler_planners = {}
        self.scheduler_planner = None
        self.setup_scheduler_planner()
        self.all_services_worker_pool = {}
        self.all_buffer_streams = {}
        self.all_queries = {}

        self.request_type_to_plan_map = {
            'ServiceWorkerOverloadedPlanRequested': 'ServiceWorkerOverloadedPlanned',
            'ServiceWorkerBestIdlePlanRequested': 'ServiceWorkerBestIdlePlanned',
            'UnnecessaryLoadSheddingPlanRequested': 'UnnecessaryLoadSheddingPlanned',
            'QuerySchedulingPlanRequested': 'QuerySchedulingPlanned',
        }

    def setup_scheduler_planner(self):
        self.available_scheduler_planners = {
            'QQoS-TK-LP-LS': QQoS_TK_LP_LS_SchedulerPlanner(
                self, self.ce_endpoint_stream_key
            ),
        }
        #     'single_best': MaxEnergyForQueueLimitSchedulerPlanner(
        #         self, self.scheduler_cmd_stream_key, self.ce_endpoint_stream_key,
        #     ),
        #     'weighted_random': WeightedRandomMaxEnergyForQueueLimitSchedulerPlanner(
        #         self, self.scheduler_cmd_stream_key, self.ce_endpoint_stream_key,
        #     ),
        #     'random': RandomSchedulerPlanner(
        #         self, self.scheduler_cmd_stream_key, self.ce_endpoint_stream_key,
        #     ),
        #     'round_robin': RoundRobinSchedulerPlanner(
        #         self, self.scheduler_cmd_stream_key, self.ce_endpoint_stream_key,
        #     ),
        #     'qos_single_best': SingleBestForQoSSinglePolicySchedulerPlanner(
        #         self, self.scheduler_cmd_stream_key, self.ce_endpoint_stream_key
        #     ),
        #     'qos_weighted_random': WeightedRandomQoSSinglePolicySchedulerPlanner(
        #         self, self.scheduler_cmd_stream_key, self.ce_endpoint_stream_key
        #     ),
        #     'qos_weighted_random_load_shedding': WeightedRandomQoSSinglePolicyLSSchedulerPlanner(
        #         self, self.scheduler_cmd_stream_key, self.ce_endpoint_stream_key
        #     ),
        #     'qos_single_best_load_shedding': SingleBestForQoSSinglePolicyLSSchedulerPlanner(
        #         self, self.scheduler_cmd_stream_key, self.ce_endpoint_stream_key
        #     ),

        # }

        self.scheduler_planner = self.available_scheduler_planners[self.scheduler_planner_type]

    # def update_plan_on_knoledge(self, plan):
    #     # arg... I want to change the current model to a more event driven,
    #     # at least on the control flow of the system
    #     # untill then I'll just send this directly to the analyser so that it can update
    #     # its internal db with the latest plan created.

    #     if plan is not None and plan.get('execution_plan') is not None:
    #         clean_plan = {k: v for k, v in plan.items() if k != 'executor'}
    #         new_event_data = {
    #             'id': self.service_based_random_event_id(),
    #             'action': 'currentAdaptationPlan',
    #             'data': clean_plan
    #         }
    #         self.write_event_with_trace(new_event_data, self.analyser_cmd_stream)
    #     return

    # def check_ongoing_knowledge_queries_are_done(self, knowledge_queries):
    #     for query_ref, query in knowledge_queries.items():
    #         if query.get('data') is None:
    #             return False
    #     return True

    # def query_knowledge(self, query):
    #     new_event_data = {
    #         'id': self.service_based_random_event_id(),
    #         'action': 'queryKnowledge',
    #         'query': query,
    #         'answer_to_stream': self.service_cmd_key
    #     }
    #     self.write_event_with_trace(new_event_data, self.knowledge_cmd_stream)

    # def update_plan(self, change_request=None, plan=None):
    #     if change_request is None:
    #         change_request = plan['change_request']

    #     changed = False
    #     if change_request['type'] == 'incorrectSchedulerPlan':
    #         self.scheduler_planner.plan(change_request, plan=plan)
    #         changed = True

    #     if change_request['type'] in ['serviceWorkerOverloaded', 'serviceWorkerBestIdle']:
    #         self.scheduler_planner.plan(change_request, plan=plan)
    #         changed = True

    #     if changed:
    #         self.update_plan_on_knoledge(plan)

    # def plan_for_change_request(self, event_data):
    #     change_request = event_data['change']
    #     self.update_plan(change_request=change_request, plan=None)

    # def update_plans_from_queries_result(self, query, query_data):
    #     query_ref = query['query_ref']
    #     for plan_id, plan in self.plans_being_planned.items():
    #         plan_ongoing_queries = plan.get('ongoing_knowledge_queries', {})
    #         if query_ref not in plan_ongoing_queries.keys():
    #             continue
    #         else:
    #             ongoing_query = plan_ongoing_queries[query_ref]
    #             ongoing_query['data'] = query_data
    #             self.update_plan(plan=plan)

    def initialize_plan(self, change_request):
        request_type = change_request['change']['type']
        plan_type = self.request_type_to_plan_map[request_type]
        new_plan = {
            'type': plan_type,
            'execution_plan': None,
            'change_request': change_request
        }
        return new_plan

    def publish_adaptation_plan(self, event_type, new_plan):
        new_event_data = {
            'id': self.parent_service.service_based_random_event_id(),
            'plan': new_plan
        }
        self.parent_service.publish_event_type_to_stream(
            event_type=event_type, new_event_data=new_event_data
        )

    # def process_action(self, action, event_data, json_msg):
    #     if not super(AdaptationPlanner, self).process_action(action, event_data, json_msg):
    #         return False
    #     if action == 'changePlanRequest':
    #         self.plan_for_change_request(event_data)
    #     elif action == 'answerQueryKnowledge':
    #         query = event_data['query']
    #         query_data = event_data['data']
    #         self.update_plans_from_queries_result(query, query_data)

    def update_bufferstreams_from_new_query(self, new_query):
        query_bufferstream_dict = new_query.get('buffer_stream', None)
        buffer_stream_key = query_bufferstream_dict.get('buffer_stream_key', None)
        if query_bufferstream_dict is None or buffer_stream_key is None:
            raise RuntimeError(f'Missing Bufferstream data on new query: {new_query}')

        bufferstream_dict = self.all_buffer_streams.setdefault(buffer_stream_key, {})
        bufferstream_dict[new_query['query_id']] = new_query

    def process_query_created(self, event_data):
        query_id = event_data['query_id']
        new_query = {}
        for k, v in event_data.items():
            if k != 'tracer':
                new_query[k] = v
        self.all_queries[query_id] = new_query
        self.update_bufferstreams_from_new_query(new_query)

    def process_plan_requested(self, event_data):
        change_request = event_data['change']
        new_plan = self.scheduler_planner.plan(change_request=change_request)
        event_type = new_plan['type']
        self.publish_adaptation_plan(event_type, new_plan=new_plan)

    def process_event_type(self, event_type, event_data, json_msg):
        if not super(AdaptationPlanner, self).process_event_type(event_type, event_data, json_msg):
            return False
        plan_requests_types = [
            'QuerySchedulingPlanRequested',
            'ServiceWorkerOverloadedPlanRequested',
            'ServiceWorkerBestIdlePlanRequested',
            'UnnecessaryLoadSheddingPlanRequested',
        ]
        if event_type == 'QueryCreated':
            self.process_query_created(event_data)
        # elif event_type == 'ServiceWorkerMonitoring':
        #     self.process_service_worker_monitoring(event_data)
        elif event_type in plan_requests_types:
            self.process_plan_requested(event_data)

    def get_destination_streams(self, destination):
        return self.stream_factory.create(destination, stype='streamOnly')

    def log_state(self):
        super(AdaptationPlanner, self).log_state()
        self._log_dict('Last plan executed:', self.last_executed)
        # self.logger.info(f'Last execution_plan: {self.last_executed.get("execution_plan", {})}')
        self._log_dict('All queries', self.all_queries)
        self.logger.debug(f'- Scheduler Planner: {self.scheduler_planner}')

    def run(self):
        super(AdaptationPlanner, self).run()
        self.log_state()
        self.cmd_thread = threading.Thread(target=self.run_forever, args=(self.process_cmd,))
        self.cmd_thread.start()
        self.cmd_thread.join()
