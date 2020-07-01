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
