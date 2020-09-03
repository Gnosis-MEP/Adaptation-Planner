from unittest.mock import patch

from event_service_utils.tests.base_test_case import MockedServiceStreamTestCase
from event_service_utils.tests.json_msg_helper import prepare_event_msg_tuple

from adaptation_planner.service import AdaptationPlanner

from adaptation_planner.planners.scheduling import (
    MaxEnergyForQueueLimitSchedulerPlanner,
    WeightedRandomMaxEnergyForQueueLimitSchedulerPlanner
)

from adaptation_planner.conf import (
    SERVICE_STREAM_KEY,
    SERVICE_CMD_KEY,
    SCHEDULER_PLANNER_TYPE,
)


class TestMaxEnergyForQueueLimitSchedulerPlanner(MockedServiceStreamTestCase):
    GLOBAL_SERVICE_CONFIG = {
        'service_stream_key': SERVICE_STREAM_KEY,
        'service_cmd_key': SERVICE_CMD_KEY,
        'scheduler_planner_type': SCHEDULER_PLANNER_TYPE,
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
                        "http://gnosis-mep.org/service_worker#energy_consumption",
                        "10.0"
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
                        "http://gnosis-mep.org/service_worker#energy_consumption",
                        "20.0"
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
                        "energy_consumption": '10.0',
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
                            "energy_consumption": 10.0
                        }
                    }
                },
                "object-detection-ssd-gpu-data": {
                    "monitoring": {
                        "energy_consumption": '20.0',
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
                            "energy_consumption": 20.0
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
                        },
                        "usage": {}
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


class TestWeightedRandomMaxEnergyForQueueLimitSchedulerPlanner(MockedServiceStreamTestCase):
    GLOBAL_SERVICE_CONFIG = {
        'service_stream_key': SERVICE_STREAM_KEY,
        'service_cmd_key': SERVICE_CMD_KEY,
        'scheduler_planner_type': SCHEDULER_PLANNER_TYPE,
        'logging_level': 'ERROR',
        'tracer_configs': {'reporting_host': None, 'reporting_port': None},
    }
    SERVICE_CLS = AdaptationPlanner
    MOCKED_STREAMS_DICT = {
        SERVICE_STREAM_KEY: [],
        SERVICE_CMD_KEY: [],
    }

    def instantiate_service(self):
        super(TestWeightedRandomMaxEnergyForQueueLimitSchedulerPlanner, self).instantiate_service()
        self.service.scheduler_planner = WeightedRandomMaxEnergyForQueueLimitSchedulerPlanner(
            self.service, self.service.scheduler_cmd_stream_key, self.service.ce_endpoint_stream_key,
        )
        return self.service

    @patch(
        'adaptation_planner.planners.scheduling.WeightedRandomMaxEnergyForQueueLimitSchedulerPlanner'
        '.get_worker_choice_weight'
    )
    def test_get_service_nonfloaded_workers(self, worker_weight_mock):
        service = 'ObjectDetection'
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
                        "queue_space": 10,
                        "queue_space_percent": 0.1
                    },
                    "resources": {
                        "planned": {
                            "queue_space": 10
                        },
                        "usage": {
                            "energy_consumption": 6,
                            "time": 1
                        }
                    }
                },
                "object-detection-ssd-gpu2-data": {
                    "monitoring": {
                        "queue_limit": 100,
                        "service_type": "ObjectDetection",
                        "type": "http://gnosis-mep.org/service_worker",
                        "stream_key": "object-detection-ssd-gpu2-data",
                        "queue_space": 30,
                        "queue_space_percent": 0.3
                    },
                    "resources": {
                        "planned": {
                            "queue_space": 30
                        },
                        "usage": {
                            "energy_consumption": 6,
                            "time": 1
                        }
                    }
                }
            }
        }
        worker_weight_mock.side_effect = [1, 3]
        min_queue_space_percent = 0.3
        non_floaded_worker = list(self.service.scheduler_planner.get_service_nonfloaded_workers(service, min_queue_space_percent))
        expected_res = [
            ('ObjectDetection', 'object-detection-ssd-data', 1),
            ('ObjectDetection', 'object-detection-ssd-gpu2-data', 3)
        ]
        self.assertListEqual(non_floaded_worker, expected_res)

    @patch(
        'adaptation_planner.planners.scheduling.WeightedRandomMaxEnergyForQueueLimitSchedulerPlanner'
        '.get_worker_choice_weight'
    )
    def test_get_per_service_nonfloaded_workers(self, worker_weight_mock):
        service = 'ObjectDetection'
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
                        "queue_space": 10,
                        "queue_space_percent": 0.1
                    },
                    "resources": {
                        "planned": {
                            "queue_space": 10
                        },
                        "usage": {
                            "energy_consumption": 6,
                            "time": 1
                        }
                    }
                },
                "object-detection-ssd-gpu2-data": {
                    "monitoring": {
                        "queue_limit": 100,
                        "service_type": "ObjectDetection",
                        "type": "http://gnosis-mep.org/service_worker",
                        "stream_key": "object-detection-ssd-gpu2-data",
                        "queue_space": 30,
                        "queue_space_percent": 0.3
                    },
                    "resources": {
                        "planned": {
                            "queue_space": 30
                        },
                        "usage": {
                            "energy_consumption": 6,
                            "time": 1
                        }
                    }
                }
            },
            "ColorDetection": {
                "color-detection-data": {
                    "monitoring": {
                        "type": "http://gnosis-mep.org/service_worker",
                        "queue_limit": 100,
                        "queue_space_percent": 1.0,
                        "queue_space": 100,
                        "stream_key": "color-detection-data",
                        "service_type": "ColorDetection"
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
            }
        }
        worker_weight_mock.side_effect = [1, 3, 4]
        min_queue_space_percent = 0.3
        required_services = ['ObjectDetection', 'ColorDetection']
        per_service_non_floaded_workers = self.service.scheduler_planner.get_per_service_nonfloaded_workers(required_services, min_queue_space_percent)
        expected_res = {
            'ObjectDetection': [
                ('ObjectDetection', 'object-detection-ssd-data', 1),
                ('ObjectDetection', 'object-detection-ssd-gpu2-data', 3)
            ],

            'ColorDetection': [
                ('ColorDetection', 'color-detection-data', 4)
            ],
        }
        self.assertDictEqual(per_service_non_floaded_workers, expected_res)

    @patch(
        'adaptation_planner.planners.scheduling.WeightedRandomMaxEnergyForQueueLimitSchedulerPlanner'
        '.calculate_worker_queue_space_percentage'
    )
    def test_get_worker_choice_weight(self, perc_mock):
        perc_mock.return_value = 0.5
        worker = {
            'resources': {
                'usage': {
                    'energy_consumption': 10
                }
            }
        }
        weight = self.service.scheduler_planner.get_worker_choice_weight(worker)
        expected = 0.005
        self.assertEqual(expected, weight)

    def test_get_dataflow_choice_weight(self):
        dataflow_choice = (
            ('ObjectDetection', 'object-detection-ssd-data', 3),
            ('ColorDetection', 'color-detection-data', 2)
        )
        weight = self.service.scheduler_planner.get_dataflow_choice_weight(dataflow_choice)
        expected = 2
        self.assertEqual(expected, weight)

    def test_get_dataflow_choice_weight_with_length_one(self):
        dataflow_choice = (
            ('ObjectDetection', 'object-detection-ssd-data', 3),
        )
        weight = self.service.scheduler_planner.get_dataflow_choice_weight(dataflow_choice)
        expected = 3
        self.assertEqual(expected, weight)

    def test_calculate_cumsum_for_dataflow_choice_initial_value(self):
        dataflow_choice = [
            [
                "ObjectDetection",
                "object-detection-ssd-gpu2-data",
                3
            ],
            [
                "ColorDetection",
                "color-detection-data",
                4
            ]
        ]
        dataflow_index = 0
        prev_cum_weight = 0
        best_weight_and_index = None
        dataflow_weighted_choice, prev_cum_weight, best_weight_and_index = self.service.scheduler_planner.calculate_cumsum_for_dataflow_choice(
            dataflow_index, dataflow_choice, prev_cum_weight, best_weight_and_index)

        expected_dataflow_choice = [
            3,
            [['ObjectDetection', 'object-detection-ssd-gpu2-data', 3], ['ColorDetection', 'color-detection-data', 4]]
        ]
        self.assertEqual(prev_cum_weight, 3)
        self.assertListEqual(list(best_weight_and_index), [3, 0])
        self.assertListEqual(dataflow_weighted_choice, expected_dataflow_choice)

    def test_calculate_cumsum_for_dataflow_choice_second_value(self):
        dataflow_choice = [
            [
                "ObjectDetection",
                "object-detection-ssd-gpu2-data",
                3
            ],
            [
                "ColorDetection",
                "color-detection-data",
                4
            ]
        ]
        dataflow_index = 1
        prev_cum_weight = 1
        best_weight_and_index = (1, 0)
        dataflow_weighted_choice, prev_cum_weight, best_weight_and_index = self.service.scheduler_planner.calculate_cumsum_for_dataflow_choice(
            dataflow_index, dataflow_choice, prev_cum_weight, best_weight_and_index)

        expected_dataflow_choice = [
            4,
            [['ObjectDetection', 'object-detection-ssd-gpu2-data', 3], ['ColorDetection', 'color-detection-data', 4]]
        ]
        self.assertEqual(prev_cum_weight, 4)
        self.assertListEqual(list(best_weight_and_index), [3, 1])
        self.assertListEqual(dataflow_weighted_choice, expected_dataflow_choice)

    def test_create_dataflow_choices_with_cum_weights_and_best_dataflow(self):
        per_service_nonfloaded_workers = {
            'ObjectDetection': [
                ('ObjectDetection', 'object-detection-ssd-data', 1),
                ('ObjectDetection', 'object-detection-ssd-gpu2-data', 3)
            ],

            'ColorDetection': [
                ('ColorDetection', 'color-detection-data', 5)
            ],
        }
        dataflow_choices, best_weight_and_index = self.service.scheduler_planner.create_dataflow_choices_with_cum_weights_and_best_dataflow(
            per_service_nonfloaded_workers)

        expected_dataflow_choices = [
            [
                1,
                (('ObjectDetection', 'object-detection-ssd-data', 1), ('ColorDetection', 'color-detection-data', 5))
            ],
            [
                4,
                (('ObjectDetection', 'object-detection-ssd-gpu2-data', 3), ('ColorDetection', 'color-detection-data', 5))
            ]
        ]
        self.assertListEqual(dataflow_choices, expected_dataflow_choices)
        self.assertListEqual(list(best_weight_and_index), [3, 1])

    @patch(
        'adaptation_planner.planners.scheduling.WeightedRandomMaxEnergyForQueueLimitSchedulerPlanner'
        '.update_worker_planned_resource'
    )
    def test_update_workers_planned_resources(self, mocked_update_worker):
        obj_worker = {
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
        }

        color_worker = {
            "monitoring": {
                "type": "http://gnosis-mep.org/service_worker",
                "queue_limit": 100,
                "queue_space_percent": 1.0,
                "queue_space": 100,
                "stream_key": "color-detection-data",
                "service_type": "ColorDetection"
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
        }
        self.service.scheduler_planner.all_services_worker_pool = {
            "ObjectDetection": {
                "object-detection-ssd-data": obj_worker
            },
            "ColorDetection": {
                "color-detection-data": color_worker
            }
        }
        dataflow_choice = [
            5,
            (('ObjectDetection', 'object-detection-ssd-data', 1), ('ColorDetection', 'color-detection-data', 5))
        ]

        dataflow_weight = 1
        total_cum_weight = 10
        min_queue_space_percent = 0.1
        self.service.scheduler_planner.update_workers_planned_resources(
            dataflow_choice,
            dataflow_weight,
            total_cum_weight,
            min_queue_space_percent
        )
        self.assertEqual(mocked_update_worker.call_count, 2)
        first_call = mocked_update_worker.call_args_list[0]
        self.assertDictEqual(first_call[0][0], obj_worker)
        self.assertAlmostEqual(first_call[0][1], 0.01)
        second_call = mocked_update_worker.call_args_list[1]
        self.assertDictEqual(second_call[0][0], color_worker)
        self.assertAlmostEqual(second_call[0][1], 0.01)

    def test_clean_up_dataflow_choices_for_correct_format(self):
        ce_endpoint_stream_key = 'endpoint-stream-key'
        self.service.scheduler_planner.ce_endpoint_stream_key = ce_endpoint_stream_key
        dataflow_choices_with_cum_weights = [
            [
                1,
                (('ObjectDetection', 'object-detection-ssd-data', 1), ('ColorDetection', 'color-detection-data', 5))
            ],
            [
                4,
                (('ObjectDetection', 'object-detection-ssd-gpu2-data', 3), ('ColorDetection', 'color-detection-data', 5))
            ]
        ]
        ret = self.service.scheduler_planner.clean_up_dataflow_choices_for_correct_format(
            dataflow_choices_with_cum_weights)
        expected_ret = [
            [1, (['object-detection-ssd-data'], ['color-detection-data'], [ce_endpoint_stream_key])],
            [4, (['object-detection-ssd-gpu2-data'], ['color-detection-data'], [ce_endpoint_stream_key])]
        ]
        self.assertListEqual(ret, expected_ret)

    @patch(
        'adaptation_planner.planners.scheduling.WeightedRandomMaxEnergyForQueueLimitSchedulerPlanner'
        '.clean_up_dataflow_choices_for_correct_format'
    )
    @patch(
        'adaptation_planner.planners.scheduling.WeightedRandomMaxEnergyForQueueLimitSchedulerPlanner'
        '.update_workers_planned_resources'
    )
    @patch(
        'adaptation_planner.planners.scheduling.WeightedRandomMaxEnergyForQueueLimitSchedulerPlanner'
        '.create_dataflow_choices_with_cum_weights_and_best_dataflow'
    )
    @patch(
        'adaptation_planner.planners.scheduling.WeightedRandomMaxEnergyForQueueLimitSchedulerPlanner'
        '.get_per_service_nonfloaded_workers'
    )
    @patch(
        'adaptation_planner.planners.scheduling.WeightedRandomMaxEnergyForQueueLimitSchedulerPlanner'
        '.get_buffer_stream_required_services'
    )
    def test_create_buffer_stream_choices_plan_calls_necessary_functions(self, required_services_mocked, non_floaded_mocked, create_choices_mocked, update_mocked, clean_mocked):
        buffer_stream_entity = {}
        best_weight_and_index = [3, 0]
        best_dataflow = ['fake_dataflow']
        create_choices_mocked.return_value = [best_dataflow, best_weight_and_index]
        self.service.scheduler_planner.create_buffer_stream_choices_plan(buffer_stream_entity)
        required_services_mocked.assert_called_once()
        non_floaded_mocked.assert_called_once()
        create_choices_mocked.assert_called_once()
        update_mocked.assert_called_once()
        clean_mocked.assert_called_once()

    @patch(
        'adaptation_planner.planners.scheduling.WeightedRandomMaxEnergyForQueueLimitSchedulerPlanner'
        '.create_buffer_stream_choices_plan'
    )
    def test_create_scheduling_plan(self, create_choices_plan_mocked):
        self.service.scheduler_planner.all_buffer_streams = {
            'bf-1': {},
            'bf-2': {},
            'bf-3': {},
        }
        dataflows = {
            'bf-1': 'w-choices1',
            'bf-2': 'w-choices2',
            'bf-3': 'w-choices3',
        }
        create_choices_plan_mocked.side_effect = ['w-choices1', 'w-choices2', 'w-choices3']
        expected_scheduling_strategy_plan = {
            'strategy': {
                'name': 'weighted_random',
                'dataflows': dataflows
            }
        }
        sc_plan = self.service.scheduler_planner.create_scheduling_plan()
        self.assertDictEqual(sc_plan, expected_scheduling_strategy_plan)

