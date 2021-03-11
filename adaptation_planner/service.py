import threading

from event_service_utils.logging.decorators import timer_logger
from event_service_utils.services.tracer import BaseTracerService
from event_service_utils.tracing.jaeger import init_tracer

from adaptation_planner.planners.scheduling import (
    SimpleFixedSchedulerPlanner,
    MaxEnergyForQueueLimitSchedulerPlanner,
    WeightedRandomMaxEnergyForQueueLimitSchedulerPlanner,
    RandomSchedulerPlanner,
    RoundRobinSchedulerPlanner
)

from adaptation_planner.planners.qos_based_scheduling import (
    SingleBestForQoSSinglePolicySchedulerPlanner
)

from adaptation_planner.conf import MOCKED_OD_STREAM_KEY


class AdaptationPlanner(BaseTracerService):

    PLAN_STAGE_PREPARATION_START = 'preparation-start'
    PLAN_STAGE_WAITING_KNOWLEDGE_QUERIES = 'waiting-knowledge-queries'
    PLAN_STAGE_IN_EXECUTION = 'in-execution'

    def __init__(self,
                 service_stream_key, service_cmd_key,
                 scheduler_planner_type,
                 stream_factory,
                 logging_level,
                 tracer_configs):
        tracer = init_tracer(self.__class__.__name__, **tracer_configs)
        super(AdaptationPlanner, self).__init__(
            name=self.__class__.__name__,
            service_stream_key=service_stream_key,
            service_cmd_key=service_cmd_key,
            stream_factory=stream_factory,
            logging_level=logging_level,
            tracer=tracer,
        )
        self.service_cmd_key = service_cmd_key
        self.cmd_validation_fields = ['id', 'action']
        self.data_validation_fields = ['id']
        self.knowledge_cmd_stream_key = 'adpk-cmd'
        self.knowledge_cmd_stream = self.stream_factory.create(key=self.knowledge_cmd_stream_key, stype='streamOnly')

        self.plans_being_planned = {}
        self.last_executed = {}

        self.scheduler_cmd_stream_key = 'sc-cmd'
        self.ce_endpoint_stream_key = 'wm-data'
        self.scheduler_planner_type = scheduler_planner_type
        self.setup_scheduler_planner()

    def setup_scheduler_planner(self):
        self.available_scheduler_planners = {
            'simple_fixed': SimpleFixedSchedulerPlanner(
                self, self.scheduler_cmd_stream_key, self.ce_endpoint_stream_key,
                mocked_od_stream_key=MOCKED_OD_STREAM_KEY
            ),
            'single_best': MaxEnergyForQueueLimitSchedulerPlanner(
                self, self.scheduler_cmd_stream_key, self.ce_endpoint_stream_key,
            ),
            'weighted_random': WeightedRandomMaxEnergyForQueueLimitSchedulerPlanner(
                self, self.scheduler_cmd_stream_key, self.ce_endpoint_stream_key,
            ),
            'random': RandomSchedulerPlanner(
                self, self.scheduler_cmd_stream_key, self.ce_endpoint_stream_key,
            ),
            'round_robin': RoundRobinSchedulerPlanner(
                self, self.scheduler_cmd_stream_key, self.ce_endpoint_stream_key,
            ),
            'qos_single_best': SingleBestForQoSSinglePolicySchedulerPlanner(
                self, self.scheduler_cmd_stream_key, self.ce_endpoint_stream_key
            ),

        }

        self.scheduler_planner = self.available_scheduler_planners[self.scheduler_planner_type]

    @timer_logger
    def process_data_event(self, event_data, json_msg):
        if not super(AdaptationPlanner, self).process_data_event(event_data, json_msg):
            return False

    def update_plan_on_knoledge(self, plan):
        pass

    def check_ongoing_knowledge_queries_are_done(self, knowledge_queries):
        for query_ref, query in knowledge_queries.items():
            if query.get('data') is None:
                return False
        return True

    def query_knowledge(self, query):
        new_event_data = {
            'id': self.service_based_random_event_id(),
            'action': 'queryKnowledge',
            'query': query,
            'answer_to_stream': self.service_cmd_key
        }
        self.write_event_with_trace(new_event_data, self.knowledge_cmd_stream)

    def update_plan(self, change_request=None, plan=None):
        if change_request is None:
            change_request = plan['change_request']

        changed = False
        if change_request['type'] == 'incorrectSchedulerPlan':
            self.scheduler_planner.plan(change_request, plan=plan)
            changed = True

        if change_request['type'] in ['serviceWorkerOverloaded', 'serviceWorkerBestIdle']:
            self.scheduler_planner.plan(change_request, plan=plan)
            changed = True

        if changed:
            self.update_plan_on_knoledge(plan)

    def plan_for_change_request(self, event_data):
        change_request = event_data['change']
        self.update_plan(change_request=change_request, plan=None)

    def update_plans_from_queries_result(self, query, query_data):
        query_ref = query['query_ref']
        for plan_id, plan in self.plans_being_planned.items():
            plan_ongoing_queries = plan.get('ongoing_knowledge_queries', {})
            if query_ref not in plan_ongoing_queries.keys():
                continue
            else:
                ongoing_query = plan_ongoing_queries[query_ref]
                ongoing_query['data'] = query_data
                self.update_plan(plan=plan)

    def process_action(self, action, event_data, json_msg):
        if not super(AdaptationPlanner, self).process_action(action, event_data, json_msg):
            return False
        if action == 'changePlanRequest':
            self.plan_for_change_request(event_data)
        elif action == 'answerQueryKnowledge':
            query = event_data['query']
            query_data = event_data['data']
            self.update_plans_from_queries_result(query, query_data)

    def prepare_plan(self, plan=None):
        if plan is None:
            plan = {}
        plan_id = self.service_based_random_event_id()
        plan['id'] = plan_id
        plan['stage'] = self.PLAN_STAGE_PREPARATION_START
        plan['ongoing_knowledge_queries'] = {}
        self.plans_being_planned[plan_id] = plan
        return plan

    def get_destination_streams(self, destination):
        return self.stream_factory.create(destination, stype='streamOnly')

    def log_state(self):
        super(AdaptationPlanner, self).log_state()
        # self._log_dict('Plans being Planned:', self.plans_being_planned)
        self._log_dict('Last plan executed:', self.last_executed)
        self.logger.info(f'Last execution_plan: {self.last_executed.get("execution_plan", {})}')
        self._log_dict('All queries', self.scheduler_planner.all_queries)

    def run(self):
        super(AdaptationPlanner, self).run()
        self.log_state()
        self.cmd_thread = threading.Thread(target=self.run_forever, args=(self.process_cmd,))
        self.data_thread = threading.Thread(target=self.run_forever, args=(self.process_data,))
        self.cmd_thread.start()
        self.data_thread.start()
        self.cmd_thread.join()
        self.data_thread.join()
