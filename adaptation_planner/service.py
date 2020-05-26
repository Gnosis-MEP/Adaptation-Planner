import threading

from event_service_utils.logging.decorators import timer_logger
from event_service_utils.services.tracer import BaseTracerService
from event_service_utils.tracing.jaeger import init_tracer


class AdaptationPlanner(BaseTracerService):

    PLAN_STAGE_PREPARATION_START = 'preparation-start'
    PLAN_STAGE_WAITING_KNOWLEDGE_QUERIES = 'waiting-knowledge-queries'
    PLAN_STAGE_IN_EXECUTION = 'in-execution'

    def __init__(self,
                 service_stream_key, service_cmd_key,
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

    @timer_logger
    def process_data_event(self, event_data, json_msg):
        if not super(AdaptationPlanner, self).process_data_event(event_data, json_msg):
            return False

    def get_queries_required_services(self, query_text_list):
        required_services = []
        if any(['object_detection' in query_text.lower() for query_text in query_text_list]):
            required_services.append('object_detection')
        return required_services

    def build_buffer_stream_plan_from_required_services(self, buffer_stream_key, required_services):
        buffer_stream_plan = []
        for service in required_services:
            service_stream = self.services_to_streams[service]
            buffer_stream_plan.append([service_stream])

        window_manager_stream = 'wm-data'
        buffer_stream_plan.extend([window_manager_stream])

        return {
            'dataflow': {
                # buffer_stream_key: [['object-detection-data'], ['wm-data']]
                buffer_stream_key: buffer_stream_plan,
            }
        }

    def update_plan_on_knoledge(self, plan):
        pass

    def check_ongoing_knowledge_queries_are_done(knowledge_queries):
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

    def ask_knowledge_for_queries_text_from_ids(self, query_ids):
        # only get first query for now, since there's no diff in geting more than one for the same buffer
        k_query_text = "SELECT DISTINCT ?o WHERE {?s gnosis-mep:query_text ?o.}"
        query = {
            'query_text': k_query_text,
            'bindings': {
                's': query_ids[0]
            },
            'query_ref': self.service_based_random_event_id(),  #for simple internal reference of this query.
        }
        self.query_knowledge(query)
        return query

    def plan_for_incorrect_scheduler_plan(self, change_request=None, plan=None):
        if plan is None:
            plan = {}
            plan.update({
                'type': 'correctSchedulerPlan',
                'execution_plan': None,
                'executor': 'sc-cmd',
                'change_request': change_request
            })
            plan = self.prepare_plan(plan)

        if change_request is None:
            change_request = plan['change_request']

        cause = change_request['cause']
        buffer_stream_entity = cause
        if plan['stage'] == self.PLAN_STAGE_PREPARATION_START:
            query_ids = buffer_stream_entity['gnosis-mep:buffer_stream#query_ids']
            knowledge_query = self.ask_knowledge_for_queries_text_from_ids(query_ids)

            plan['stage'] = self.PLAN_STAGE_WAITING_KNOWLEDGE_QUERIES
            knowledge_queries = plan.getdefault('ongoing_knowledge_queries', {})
            query_ref = knowledge_query['query_ref']
            knowledge_queries[query_ref] = knowledge_query

        elif plan['stage'] == self.PLAN_STAGE_WAITING_KNOWLEDGE_QUERIES:
            if self.check_ongoing_knowledge_queries_are_done(plan.get('ongoing_knowledge_queries', {})):
                k_response = self.plan['ongoing_knowledge_queries'][0]['response']
                query_text_list = [t[0] for t in k_response]

                required_services = self.get_queries_required_services(query_text_list)
                buffer_stream_key = buffer_stream_entity['gnosis-mep:buffer_stream#buffer_stream_key']
                buffer_stream_plan = self.build_buffer_stream_plan_from_required_services(
                    buffer_stream_key, required_services)

                plan['execution_plan'] = buffer_stream_plan
                plan['stage'] = self.PLAN_STAGE_IN_EXECUTION
                self.send_plan_to_scheduler(buffer_stream_plan)
            else:
                pass
        self.update_plan_on_knoledge(plan)
        return plan

    def update_plan(self, change_request=None, plan=None):
        if change_request is None:
            change_request = plan['change_request']

        if change_request['type'] == 'incorrectSchedulerPlan':
            self.plan_for_incorrect_scheduler_plan(change_request, plan=plan)

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
                ongoing_query['data'] = query['data']
                self.update_plan(plan=plan)

    def process_action(self, action, event_data, json_msg):
        if not super(AdaptationPlanner, self).process_action(action, event_data, json_msg):
            return False
        if action == 'changePlanRequest':
            self.plan_for_change_request(event_data)
        elif action == 'answerQueryKnowledge':
            query = event_data['query']
            query_data = event_data['data']
            self.update_plans_queries(query, query_data)

    def prepare_plan(self, plan=None):
        if plan is None:
            plan = {}
        plan_id = '123'
        plan['id'] = plan_id
        plan['stage'] = self.PLAN_STAGE_PREPARATION_START
        plan['ongoing_knowledge_queries'] = {}
        self.plans_being_planned[plan_id] = plan
        return plan

    def get_destination_streams(self, destination):
        return self.stream_factory.create(destination, stype='streamOnly')

    def send_plan_to_scheduler(self, adaptive_plan):
        new_event_data = {
            'id': self.service_based_random_event_id(),
            'action': 'executeAdaptivePlan',
        }
        new_event_data.update(adaptive_plan)

        # another hacky hack. hardcoding the stream key for the scheduler
        self.write_event_with_trace(new_event_data, self.get_destination_streams('sc-cmd'))

    def log_state(self):
        super(AdaptationPlanner, self).log_state()
        self.logger.info(f'My service name is: {self.name}')

    def run(self):
        super(AdaptationPlanner, self).run()
        self.cmd_thread = threading.Thread(target=self.run_forever, args=(self.process_cmd,))
        self.data_thread = threading.Thread(target=self.run_forever, args=(self.process_data,))
        self.cmd_thread.start()
        self.data_thread.start()
        self.cmd_thread.join()
        self.data_thread.join()
