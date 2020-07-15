from unittest.mock import patch

from event_service_utils.tests.base_test_case import MockedServiceStreamTestCase
from event_service_utils.tests.json_msg_helper import prepare_event_msg_tuple

from adaptation_planner.service import AdaptationPlanner

from adaptation_planner.planners.scheduling import MaxEnergyForQueueLimitSchedulerPlanner

from adaptation_planner.conf import (
    SERVICE_STREAM_KEY,
    SERVICE_CMD_KEY,
)


class TestMaxEnergyForQueueLimitSchedulerPlanner(MockedServiceStreamTestCase):
    GLOBAL_SERVICE_CONFIG = {
        'service_stream_key': SERVICE_STREAM_KEY,
        'service_cmd_key': SERVICE_CMD_KEY,
        'logging_level': 'ERROR',
        'tracer_configs': {'reporting_host': None, 'reporting_port': None},
    }
    SERVICE_CLS = AdaptationPlanner
    MOCKED_STREAMS_DICT = {
        SERVICE_STREAM_KEY: [],
        SERVICE_CMD_KEY: [],
    }

    def instantiate_service(self):
        super(TestMaxEnergyForQueueLimitSchedulerPlanner, self).instantiate_service()
        self.service.scheduler_planner = MaxEnergyForQueueLimitSchedulerPlanner(
            self.service, self.service.scheduler_cmd_stream_key, self.service.ce_endpoint_stream_key,
        )
        return self.service

    @patch(
        'adaptation_planner.planners.scheduling.MaxEnergyForQueueLimitSchedulerPlanner'
        '.plan_stage_waiting_knowledge_queries'
    )
    @patch('adaptation_planner.planners.scheduling.MaxEnergyForQueueLimitSchedulerPlanner.plan_stage_preparation_start')
    @patch('adaptation_planner.service.AdaptationPlanner.prepare_plan')
    def test_plan_should_init_plan_if_none_only(self, prepare_plan, stage_preparation, stage_waiting_kq):
        change_request = {'cause': None}
        self.service.scheduler_planner.plan(change_request=change_request, plan=None)
        prepare_plan.assert_called_once()

    @patch(
        'adaptation_planner.planners.scheduling.MaxEnergyForQueueLimitSchedulerPlanner'
        '.plan_stage_waiting_knowledge_queries'
    )
    @patch('adaptation_planner.planners.scheduling.MaxEnergyForQueueLimitSchedulerPlanner.plan_stage_preparation_start')
    @patch('adaptation_planner.service.AdaptationPlanner.prepare_plan')
    def test_plan_shouldnot_init_plan_if_not_none(self, prepare_plan, stage_preparation, stage_waiting_kq):
        change_request = {'cause': None}
        plan = {'change_request': change_request, 'stage': 'whatever'}
        self.service.scheduler_planner.plan(change_request=None, plan=plan)
        prepare_plan.assert_not_called()

    @patch(
        'adaptation_planner.planners.scheduling.MaxEnergyForQueueLimitSchedulerPlanner'
        '.plan_stage_waiting_knowledge_queries'
    )
    @patch('adaptation_planner.planners.scheduling.MaxEnergyForQueueLimitSchedulerPlanner.plan_stage_preparation_start')
    @patch('adaptation_planner.service.AdaptationPlanner.prepare_plan')
    def test_plan_should_call_plan_stage_preparation_start_correcly(self, prepare_plan, stage_preparation, stage_waiting_kq):
        cause = 'cause'
        change_request = {'cause': cause}
        plan = None
        prepared_plan_value = {'stage': self.service.PLAN_STAGE_PREPARATION_START}
        prepare_plan.return_value = prepared_plan_value
        self.service.scheduler_planner.plan(change_request=change_request, plan=plan)
        stage_preparation.assert_called_once_with(cause, prepared_plan_value)
        stage_waiting_kq.assert_not_called()

    @patch(
        'adaptation_planner.planners.scheduling.MaxEnergyForQueueLimitSchedulerPlanner'
        '.plan_stage_waiting_knowledge_queries'
    )
    @patch('adaptation_planner.planners.scheduling.MaxEnergyForQueueLimitSchedulerPlanner.plan_stage_preparation_start')
    def test_plan_should_call_plan_stage_waiting_knowledge_queries_correcly(self, stage_preparation, stage_waiting_kq):
        cause = 'cause'
        change_request = {'cause': cause}
        plan = {'stage': self.service.PLAN_STAGE_WAITING_KNOWLEDGE_QUERIES, 'change_request': change_request}
        self.service.scheduler_planner.plan(change_request=None, plan=plan)
        stage_waiting_kq.assert_called_once_with(cause, plan)
        stage_preparation.assert_not_called()

    @patch(
        'adaptation_planner.planners.scheduling.MaxEnergyForQueueLimitSchedulerPlanner'
        '.ask_knowledge_for_all_entities_of_namespace'
    )
    def test_plan_stage_preparation_start_call_correct_method_and_sets_correct_stage(
        self, ask_k
    ):
        cause = 'cause'
        change_request = {'cause': cause}
        plan = {
            'stage': self.service.PLAN_STAGE_PREPARATION_START, 'change_request': change_request
        }

        ongoing_queries = [{'query_ref': i} for i in range(3)]
        ask_k.side_effect = ongoing_queries

        ret_plan = self.service.scheduler_planner.plan_stage_preparation_start(cause=cause, plan=plan)
        for args in [('gnosis-mep:subscriber_query',), ('gnosis-mep:buffer_stream',), ('gnosis-mep:service_worker',)]:
            assert args in [c[0] for c in ask_k.call_args_list]
        self.assertEqual(ask_k.call_count, 3)

        self.assertEqual(ret_plan['stage'], self.service.PLAN_STAGE_WAITING_KNOWLEDGE_QUERIES)
        expected_ongoing_queries = {q['query_ref']: q for q in ongoing_queries}
        self.assertDictEqual(ret_plan['ongoing_knowledge_queries'], expected_ongoing_queries)

    @patch(
        'adaptation_planner.planners.scheduling.MaxEnergyForQueueLimitSchedulerPlanner'
        '.send_plan_to_scheduler'
    )
    @patch(
        'adaptation_planner.planners.scheduling.MaxEnergyForQueueLimitSchedulerPlanner'
        '.create_scheduling_plan'
    )
    @patch(
        'adaptation_planner.planners.scheduling.MaxEnergyForQueueLimitSchedulerPlanner'
        '.prepare_data_structures_from_knowledge_queries_data'
    )
    @patch('adaptation_planner.service.AdaptationPlanner.check_ongoing_knowledge_queries_are_done')
    def test_plan_stage_waiting_knowledge_queries_should_not_call_anything_if_not_ready(
        self, queries_done, prepare_struct, create_sc_plan, send_plan
    ):
        cause = 'cause'
        change_request = {'cause': cause}
        plan = {
            'stage': self.service.PLAN_STAGE_WAITING_KNOWLEDGE_QUERIES,
            'change_request': change_request,
            'ongoing_knowledge_queries': {
                'q1': {'data': []},
                'q1': {'data': []},
                'q1': {},
            }
        }
        queries_done.return_value = False

        self.service.scheduler_planner.plan_stage_waiting_knowledge_queries(cause=cause, plan=plan)

        queries_done.assert_called()
        prepare_struct.assert_not_called()
        create_sc_plan.assert_not_called()
        send_plan.assert_not_called()


    @patch(
        'adaptation_planner.planners.scheduling.MaxEnergyForQueueLimitSchedulerPlanner'
        '.send_plan_to_scheduler'
    )
    @patch(
        'adaptation_planner.planners.scheduling.MaxEnergyForQueueLimitSchedulerPlanner'
        '.create_scheduling_plan'
    )
    @patch(
        'adaptation_planner.planners.scheduling.MaxEnergyForQueueLimitSchedulerPlanner'
        '.prepare_data_structures_from_knowledge_queries_data'
    )
    @patch('adaptation_planner.service.AdaptationPlanner.check_ongoing_knowledge_queries_are_done')
    def test_plan_stage_waiting_knowledge_queries_should_call_correct_and_set_stage(
        self, queries_done, prepare_struct, create_sc_plan, send_plan
    ):
        cause = 'cause'
        change_request = {'cause': cause}
        plan = {
            'stage': self.service.PLAN_STAGE_WAITING_KNOWLEDGE_QUERIES,
            'change_request': change_request,
            'ongoing_knowledge_queries': {
                'q1': {'data': []},
                'q1': {'data': []},
                'q1': {'data': []},
            }
        }
        queries_done.return_value = True
        execution_plan = {'execution_plan': 'whatever'}
        create_sc_plan.return_value = execution_plan

        ret_plan = self.service.scheduler_planner.plan_stage_waiting_knowledge_queries(cause=cause, plan=plan)

        queries_done.assert_called()
        prepare_struct.assert_called()
        create_sc_plan.assert_called()
        send_plan.assert_called()

        self.assertEqual(ret_plan['stage'], self.service.PLAN_STAGE_IN_EXECUTION)
        self.assertDictEqual(ret_plan['ongoing_knowledge_queries'], {})
        self.assertEqual(ret_plan['execution_plan'], execution_plan)

        # expected_ongoing_queries = {q['query_ref']: q for q in ongoing_queries}
        # self.assertDictEqual(ret_plan['ongoing_knowledge_queries'], expected_ongoing_queries)