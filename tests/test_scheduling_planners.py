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

    def test_prepare_local_services_with_workers(self):
        ongoing_knowledge_queries = {
            'AdaptationPlanner:aecf0760-bf47-4ae0-b0df-08b435954146-gnosis-mep:service_worker': {
                "query": {
                    "query_ref": "AdaptationPlanner:aecf0760-bf47-4ae0-b0df-08b435954146-gnosis-mep:service_worker"
                },
                "data": [
                    [
                        "http://gnosis-mep.org/service_worker/object-detection-ssd-data",
                        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                        "http://gnosis-mep.org/service_worker"
                    ],
                    [
                        "http://gnosis-mep.org/service_worker/object-detection-ssd-data",
                        "http://gnosis-mep.org/service_worker#queue_limit",
                        "100"
                    ],
                    [
                        "http://gnosis-mep.org/service_worker/object-detection-ssd-data",
                        "http://gnosis-mep.org/service_worker#queue_space_percent",
                        "1.0"
                    ],
                    [
                        "http://gnosis-mep.org/service_worker/object-detection-ssd-data",
                        "http://gnosis-mep.org/service_worker#queue_space",
                        "100"
                    ],
                    [
                        "http://gnosis-mep.org/service_worker/object-detection-ssd-data",
                        "http://gnosis-mep.org/service_worker#stream_key",
                        "object-detection-ssd-data"
                    ],
                    [
                        "http://gnosis-mep.org/service_worker/object-detection-ssd-data",
                        "http://gnosis-mep.org/service_worker#service_type",
                        "ObjectDetection"
                    ],
                    [
                        "http://gnosis-mep.org/service_worker/color-detection-data",
                        "http://gnosis-mep.org/service_worker#stream_key",
                        "color-detection-data"
                    ],
                    [
                        "http://gnosis-mep.org/service_worker/color-detection-data",
                        "http://gnosis-mep.org/service_worker#service_type",
                        "ColorDetection"
                    ],
                    [
                        "http://gnosis-mep.org/service_worker/color-detection-data",
                        "http://gnosis-mep.org/service_worker#queue_space",
                        "100"
                    ],
                    [
                        "http://gnosis-mep.org/service_worker/color-detection-data",
                        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                        "http://gnosis-mep.org/service_worker"
                    ],
                    [
                        "http://gnosis-mep.org/service_worker/color-detection-data",
                        "http://gnosis-mep.org/service_worker#queue_limit",
                        "100"
                    ],
                    [
                        "http://gnosis-mep.org/service_worker/color-detection-data",
                        "http://gnosis-mep.org/service_worker#queue_space_percent",
                        "1.0"
                    ],
                    [
                        "http://gnosis-mep.org/service_worker/object-detection-ssd-gpu-data",
                        "http://gnosis-mep.org/service_worker#queue_limit",
                        "100"
                    ],
                    [
                        "http://gnosis-mep.org/service_worker/object-detection-ssd-gpu-data",
                        "http://gnosis-mep.org/service_worker#service_type",
                        "ObjectDetection"
                    ],
                    [
                        "http://gnosis-mep.org/service_worker/object-detection-ssd-gpu-data",
                        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                        "http://gnosis-mep.org/service_worker"
                    ],
                    [
                        "http://gnosis-mep.org/service_worker/object-detection-ssd-gpu-data",
                        "http://gnosis-mep.org/service_worker#stream_key",
                        "object-detection-ssd-gpu-data"
                    ],
                    [
                        "http://gnosis-mep.org/service_worker/object-detection-ssd-gpu-data",
                        "http://gnosis-mep.org/service_worker#queue_space",
                        "90"
                    ],
                    [
                        "http://gnosis-mep.org/service_worker/object-detection-ssd-gpu-data",
                        "http://gnosis-mep.org/service_worker#queue_space_percent",
                        "0.9"
                    ]
                ]
            }

        }
        self.service.scheduler_planner.prepare_local_services_with_workers(ongoing_knowledge_queries)
        expected_all_services_dict = {
            "ObjectDetection": {
                "object-detection-ssd-data": {
                    "monitoring": {
                        "type": "http://gnosis-mep.org/service_worker",
                        "queue_limit": 100,
                        "queue_space_percent": 1.0,
                        "queue_space": 100,
                        "stream_key": "object-detection-ssd-data",
                        "service_type": "ObjectDetection"
                    },
                    "resources": {
                        "planned": {
                            "queue_space": 100
                        },
                        "usage": {
                            "energy_consumption": 188.0,
                        }
                    }
                },
                "object-detection-ssd-gpu-data": {
                    "monitoring": {
                        "queue_limit": 100,
                        "service_type": "ObjectDetection",
                        "type": "http://gnosis-mep.org/service_worker",
                        "stream_key": "object-detection-ssd-gpu-data",
                        "queue_space": 90,
                        "queue_space_percent": 0.9
                    },
                    "resources": {
                        "planned": {
                            "queue_space": 90
                        },
                        "usage": {
                            "energy_consumption": 163.8,
                        }
                    }
                }
            },
            "ColorDetection": {
                "color-detection-data": {
                    "monitoring": {
                        "stream_key": "color-detection-data",
                        "service_type": "ColorDetection",
                        "queue_space": 100,
                        "type": "http://gnosis-mep.org/service_worker",
                        "queue_limit": 100,
                        "queue_space_percent": 1.0
                    },
                    "resources": {
                        "planned": {
                            "queue_space": 100
                        }
                    }
                }
            }
        }
        self.assertDictEqual(expected_all_services_dict, self.service.scheduler_planner.all_services_worker_pool)

    def test_create_buffer_stream_plan_should_return_best_available_with_min_energy(self):
        self.service.scheduler_planner.all_buffer_streams = {
            'some-buffer-stream-key': {
                'query_ids': ['query_id1'],
                'queries': {
                    'query_id1': {
                        'query_text': 'etc ... object_detection etc..'
                    }
                }
            }
        }
        self.service.scheduler_planner.all_services_worker_pool = {
            "ObjectDetection": {
                "object-detection-ssd-data": {
                    "monitoring": {
                        "type": "http://gnosis-mep.org/service_worker",
                        "queue_limit": 100,
                        "queue_space_percent": 1.0,
                        "queue_space": 100,
                        "stream_key": "object-detection-ssd-data",
                        "service_type": "ObjectDetection"
                    },
                    "resources": {
                        "planned": {
                            "queue_space": 100
                        },
                        "usage": {
                            "energy_consumption": 10,
                            "time": 1
                        }
                    }
                },
                "object-detection-ssd-gpu-data": {
                    "monitoring": {
                        "queue_limit": 100,
                        "service_type": "ObjectDetection",
                        "type": "http://gnosis-mep.org/service_worker",
                        "stream_key": "object-detection-ssd-gpu-data",
                        "queue_space": 90,
                        "queue_space_percent": 0.9
                    },
                    "resources": {
                        "planned": {
                            "queue_space": 90
                        },
                        "usage": {
                            "energy_consumption": 6,
                            "time": 1
                        }
                    }
                }
            }
        }

        ret_plan = self.service.scheduler_planner.create_buffer_stream_plan(
            self.service.scheduler_planner.all_buffer_streams['some-buffer-stream-key']
        )
        expected_plan = [['object-detection-ssd-gpu-data'], ['wm-data']]
        self.assertListEqual(expected_plan, ret_plan)

        obj_detections = self.service.scheduler_planner.all_services_worker_pool['ObjectDetection']
        ssd_gpu = obj_detections['object-detection-ssd-gpu-data']
        ssd_gpu_planned_res = ssd_gpu['resources']['planned']
        self.assertEqual(ssd_gpu_planned_res['queue_space'], 60)

    def test_create_buffer_stream_plan_should_return_best_available_with_min_energy2(self):
        self.service.scheduler_planner.all_buffer_streams = {
            'some-buffer-stream-key': {
                'query_ids': ['query_id1'],
                'queries': {
                    'query_id1': {
                        'query_text': 'etc ... object_detection etc..'
                    }
                }
            }
        }
        self.service.scheduler_planner.all_services_worker_pool = {
            "ObjectDetection": {
                "object-detection-ssd-data": {
                    "monitoring": {
                        "type": "http://gnosis-mep.org/service_worker",
                        "queue_limit": 100,
                        "queue_space_percent": 1.0,
                        "queue_space": 100,
                        "stream_key": "object-detection-ssd-data",
                        "service_type": "ObjectDetection"
                    },
                    "resources": {
                        "planned": {
                            "queue_space": 100
                        },
                        "usage": {
                            "energy_consumption": 5,
                            "time": 1
                        }
                    }
                },
                "object-detection-ssd-gpu-data": {
                    "monitoring": {
                        "queue_limit": 100,
                        "service_type": "ObjectDetection",
                        "type": "http://gnosis-mep.org/service_worker",
                        "stream_key": "object-detection-ssd-gpu-data",
                        "queue_space": 90,
                        "queue_space_percent": 0.9
                    },
                    "resources": {
                        "planned": {
                            "queue_space": 90
                        },
                        "usage": {
                            "energy_consumption": 6,
                            "time": 1
                        }
                    }
                }
            }
        }

        ret_plan = self.service.scheduler_planner.create_buffer_stream_plan(
            self.service.scheduler_planner.all_buffer_streams['some-buffer-stream-key']
        )
        expected_plan = [['object-detection-ssd-data'], ['wm-data']]
        self.assertListEqual(expected_plan, ret_plan)

        obj_detections = self.service.scheduler_planner.all_services_worker_pool['ObjectDetection']
        ssd_gpu = obj_detections['object-detection-ssd-data']
        ssd_gpu_planned_res = ssd_gpu['resources']['planned']
        self.assertEqual(ssd_gpu_planned_res['queue_space'], 70)
