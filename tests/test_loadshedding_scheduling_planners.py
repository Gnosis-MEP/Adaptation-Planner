from unittest import TestCase
from unittest.mock import patch, MagicMock

from adaptation_planner.planners.load_shedding_based_scheduling import (
    SingleBestForQoSSinglePolicyLSSchedulerPlanner,
    WeightedRandomQoSSinglePolicyLSSchedulerPlanner
)


class TestSingleBestForQoSSinglePolicyLSSchedulerPlanner(TestCase):

    def setUp(self):
        scheduler_cmd_stream_key = 'sc-cmd'
        ce_endpoint_stream_key = 'wm-data'
        parent_service = MagicMock()
        self.planner = SingleBestForQoSSinglePolicyLSSchedulerPlanner(
            parent_service, scheduler_cmd_stream_key, ce_endpoint_stream_key)
        self.all_queries_dict = {
            '3940d2cad2926150093a9a786163ee14': {
                'action': 'updateControlFlow',
                'data_flow': ['ObjectDetection'],
                'publisher_id': 'publisher1',
                'qos_policies': {
                    'accuracy': 'min'
                },
                'query_id': '3940d2cad2926150093a9a786163ee14',
                'type': 'http://gnosis-mep.org/subscriber_query'
            }
        }
        self.all_buffer_streams = {
            'b41eeb0408847b28474f362f5642635e': {
                'action': 'startPreprocessing',
                'buffer_stream_key': 'b41eeb0408847b28474f362f5642635e',
                'fps': '30',
                'publisher_id': 'publisher1',
                'queries': self.all_queries_dict,
                'resolution': '640x480',
                'source': 'rtmp://172.17.0.1/vod2/cars.mp4',
                'type': 'http://gnosis-mep.org/buffer_stream'
            }
        }

        self.worker_a = {
            'monitoring': {
                'accuracy': '0.6',
                # 'content_types': 'node:person',
                'energy_consumption': '100.0',
                'queue_size': 0,
                'queue_limit': 100,
                'queue_space': 100,
                'queue_space_percent': 1.0,
                'service_type': 'ObjectDetection',
                'stream_key': 'object-detection-ssd-data',
                'throughput': '15',
                'type': 'http://gnosis-mep.org/service_worker'
            },
            'resources': {
                'planned': {
                    'queue_space': 100
                },
                'usage': {
                    'energy_consumption': 108.0
                }
            }
        }
        self.worker_b = {
            'monitoring': {
                'accuracy': '0.9',
                # 'content_types': 'node_attribute:label',
                'energy_consumption': '200.0',
                'queue_size': 4,
                'queue_limit': 100,
                'queue_space': 100,
                'queue_space_percent': 1.0,
                'service_type': 'ObjectDetection',
                'stream_key': 'object-detection-ssd-gpu-data',
                'throughput': '30',
                'type': 'http://gnosis-mep.org/service_worker'
            },
            'resources': {
                'planned': {
                    'queue_space': 100
                },
                'usage': {
                    'energy_consumption': 108.0
                }
            }
        }
        self.all_services_worker_pool = {
            'ObjectDetection': {
                'object-detection-ssd-data': self.worker_a,
                'object-detection-ssd-gpu-data': self.worker_b,
            }
        }

    def test_get_bufferstream_required_loadshedding_rate_for_worker_when_overloaded(self):
        self.planner.adaptation_delta = 10
        self.planner.all_services_worker_pool = self.all_services_worker_pool

        required_events = 200
        self.worker_a['resources']['planned']['events_capacity'] = -50
        load_shedding_rate = self.planner.get_bufferstream_required_loadshedding_rate_for_worker(
            self.worker_a, required_events)
        self.assertAlmostEqual(load_shedding_rate, 0.25)

    def test_get_bufferstream_required_loadshedding_rate_for_worker_when_underloaded(self):
        self.planner.adaptation_delta = 10
        self.planner.all_services_worker_pool = self.all_services_worker_pool

        required_events = 145
        self.worker_a['resources']['planned']['events_capacity'] = 0
        load_shedding_rate = self.planner.get_bufferstream_required_loadshedding_rate_for_worker(
            self.worker_a, required_events)
        self.assertAlmostEqual(load_shedding_rate, 0)

    def test_get_bufferstream_required_loadshedding_rate_for_worker_when_extremely_overloaded(self):
        self.planner.adaptation_delta = 10
        self.planner.all_services_worker_pool = self.all_services_worker_pool

        required_events = 200
        self.worker_a['resources']['planned']['events_capacity'] = -600  # wont happent, but in any case
        load_shedding_rate = self.planner.get_bufferstream_required_loadshedding_rate_for_worker(
            self.worker_a, required_events)
        self.assertAlmostEqual(load_shedding_rate, 1)

    def test_update_workers_planned_resources_returns_load_shedding_with_single_worker(self):
        required_events = 100
        required_services = ['ObjectDetection']
        buffer_stream_plan = [['object-detection-ssd-data']]

        self.planner.all_services_worker_pool = self.all_services_worker_pool
        self.worker_a['resources']['planned']['events_capacity'] = 90

        load_shedding_rate = self.planner.update_workers_planned_resources(
            required_services, buffer_stream_plan, required_events)
        self.assertAlmostEqual(load_shedding_rate, 0.1)

    def test_update_workers_planned_resources_returns_load_shedding_with_single_service(self):
        required_events = 100
        required_services = ['ObjectDetection']
        buffer_stream_plan = [['object-detection-ssd-data']]

        self.planner.all_services_worker_pool = self.all_services_worker_pool
        self.worker_a['resources']['planned']['events_capacity'] = 90

        load_shedding_rate = self.planner.update_workers_planned_resources(
            required_services, buffer_stream_plan, required_events)
        self.assertAlmostEqual(load_shedding_rate, 0.1)

    def test_update_workers_planned_resources_returns_load_shedding_with_multiple_service(self):
        required_events = 100
        required_services = ['ObjectDetection', 'ObjectDetection']
        buffer_stream_plan = [['object-detection-ssd-data'], ['object-detection-ssd-gpu-data']]

        self.planner.all_services_worker_pool = self.all_services_worker_pool
        self.worker_a['resources']['planned']['events_capacity'] = 50
        self.worker_b['resources']['planned']['events_capacity'] = 90

        load_shedding_rate = self.planner.update_workers_planned_resources(
            required_services, buffer_stream_plan, required_events)
        self.assertAlmostEqual(load_shedding_rate, 0.5)

    @patch('adaptation_planner.planners.load_shedding_based_scheduling.SingleBestForQoSSinglePolicyLSSchedulerPlanner.get_buffer_stream_required_services')
    @patch('adaptation_planner.planners.load_shedding_based_scheduling.SingleBestForQoSSinglePolicyLSSchedulerPlanner.get_bufferstream_planned_event_count')
    @patch('adaptation_planner.planners.load_shedding_based_scheduling.SingleBestForQoSSinglePolicyLSSchedulerPlanner.initialize_service_workers_planned_capacity')
    @patch('adaptation_planner.planners.load_shedding_based_scheduling.SingleBestForQoSSinglePolicyLSSchedulerPlanner.filter_overloaded_service_worker_pool_or_all_if_empty')
    @patch('adaptation_planner.planners.load_shedding_based_scheduling.SingleBestForQoSSinglePolicyLSSchedulerPlanner.workers_key_sorted_by_qos')
    @patch('adaptation_planner.planners.load_shedding_based_scheduling.SingleBestForQoSSinglePolicyLSSchedulerPlanner.update_workers_planned_resources')
    def test_create_buffer_stream_plan_calls_proper_methods_and_returns_load_shedding(self, updated_res, w_sort, w_filter, w_init, event_count, req_serv):
        self.all_queries_dict['3940d2cad2926150093a9a786163ee14']['qos_policies'] = {
            'energy_consumption': 'min'
        }
        bufferstream_entity = self.all_buffer_streams['b41eeb0408847b28474f362f5642635e']
        req_serv.return_value = ['ObjectDetection']
        load_shedding = 0.5
        updated_res.return_value = load_shedding
        self.planner.all_services_worker_pool = self.all_services_worker_pool
        ret = self.planner.create_buffer_stream_plan(bufferstream_entity)
        self.assertTrue(updated_res.called)
        self.assertTrue(w_sort.called)
        self.assertTrue(w_filter.called)
        self.assertTrue(w_init.called)
        self.assertTrue(event_count.called)
        self.assertTrue(req_serv.called)
        self.assertEquals(len(ret), 2)
        self.assertEquals(ret[1], 0.5)

    @patch('adaptation_planner.planners.load_shedding_based_scheduling.SingleBestForQoSSinglePolicyLSSchedulerPlanner.create_buffer_stream_plan')
    def test_create_buffer_stream_choices_plan_creates_plan_choices_with_load_shedding(self, create_bsp):
        self.planner.all_buffer_streams = self.all_buffer_streams
        self.planner.all_queries_dict = self.all_queries_dict
        self.planner.all_services_worker_pool = self.all_services_worker_pool
        bufferstream_entity = self.all_buffer_streams['b41eeb0408847b28474f362f5642635e']

        create_bsp.return_value = ['plan', 'load_shedding']
        ret = self.planner.create_buffer_stream_choices_plan(bufferstream_entity)
        self.assertEqual(len(ret), 1)
        best_plan = ret[0]
        self.assertEqual(len(best_plan), 3)
        self.assertEqual(best_plan[0], 'load_shedding')
        self.assertEqual(best_plan[1], None)
        self.assertEqual(best_plan[2], 'plan')


class TestWeightedRandomQoSSinglePolicyLSSchedulerPlanner(TestCase):

    def setUp(self):
        scheduler_cmd_stream_key = 'sc-cmd'
        ce_endpoint_stream_key = 'wm-data'
        parent_service = MagicMock()
        self.planner = WeightedRandomQoSSinglePolicyLSSchedulerPlanner(
            parent_service, scheduler_cmd_stream_key, ce_endpoint_stream_key)
        self.all_queries_dict = {
            '3940d2cad2926150093a9a786163ee14': {
                'action': 'updateControlFlow',
                'data_flow': ['ObjectDetection', 'ColorDetection'],
                'publisher_id': 'publisher1',
                'qos_policies': {
                    'accuracy': 'max'
                },
                'query_id': '3940d2cad2926150093a9a786163ee14',
                'type': 'http://gnosis-mep.org/subscriber_query'
            }
        }
        self.all_buffer_streams = {
            'b41eeb0408847b28474f362f5642635e': {
                'action': 'startPreprocessing',
                'buffer_stream_key': 'b41eeb0408847b28474f362f5642635e',
                'fps': '30',
                'publisher_id': 'publisher1',
                'queries': self.all_queries_dict,
                'resolution': '640x480',
                'source': 'rtmp://172.17.0.1/vod2/cars.mp4',
                'type': 'http://gnosis-mep.org/buffer_stream'
            }
        }

        self.worker_s1a = {
            'monitoring': {
                'accuracy': '0.6',
                'energy_consumption': '100.0',
                'queue_size': 2,
                'queue_limit': 100,
                'queue_space': 100,
                'queue_space_percent': 1.0,
                'service_type': 'ObjectDetection',
                'stream_key': 'object-detection-ssd-data',
                'throughput': '15',
                'type': 'http://gnosis-mep.org/service_worker'
            },
            'resources': {
                'planned': {
                }
            }
        }
        self.worker_s1b = {
            'monitoring': {
                'accuracy': '0.9',
                'energy_consumption': '150.0',
                'queue_size': 5,
                'queue_limit': 100,
                'queue_space': 100,
                'queue_space_percent': 1.0,
                'service_type': 'ObjectDetection',
                'stream_key': 'object-detection-ssd-gpu-data',
                'throughput': '30',
                'type': 'http://gnosis-mep.org/service_worker'
            },
            'resources': {
                'planned': {
                }
            }
        }
        self.worker_s2a = {
            'monitoring': {
                'accuracy': '0.8',
                'energy_consumption': '50.0',
                'queue_size': 10,
                'service_type': 'ColorDetection',
                'stream_key': 'color-detection-data',
                'throughput': '15',
                'type': 'http://gnosis-mep.org/service_worker'
            },
            'resources': {
                'planned': {
                },
            }
        }

        self.worker_s2b = {
            'monitoring': {
                'accuracy': '0.7',
                'energy_consumption': '30.0',
                'queue_size': 10,
                'service_type': 'ColorDetection',
                'stream_key': 'color-detection2-data',
                'throughput': '60',
                'type': 'http://gnosis-mep.org/service_worker'
            },
            'resources': {
                'planned': {
                },
            }
        }

        self.all_services_worker_pool = {
            'ObjectDetection': {
                'object-detection-ssd-data': self.worker_s1a,
                'object-detection-ssd-gpu-data': self.worker_s1b,
            },
            'ColorDetection': {
                'color-detection-data': self.worker_s2a,
                'color-detection2-data': self.worker_s2b,
            }
        }

    def test_update_workers_planned_resources(self):
        self.all_services_worker_pool = {}
        self.all_services_worker_pool['ObjectDetection'] = {
            'object-detection-data': {
                'resources': {
                    'planned': {
                        'events_capacity': 100,
                    }
                }
            },
            'object-detection-data2': {
                'resources': {
                    'planned': {
                        'events_capacity': 200,
                    }
                }
            }
        }
        self.all_services_worker_pool['ColorDetection'] = {
            'color-detection-data': {
                'resources': {
                    'planned': {
                        'events_capacity': 300,
                    }
                }
            }
        }

        dataflow_choices_with_cum_weights = [
            (25.0,
             ([25, 'ObjectDetection', 'object-detection-data'],
              [85, 'ColorDetection', 'color-detection-data'])),
            (100.0,
             ([75, 'ObjectDetection', 'object-detection-data2'],
              [85, 'ColorDetection', 'color-detection-data'],)),
        ]
        dataflow_choices_weights = [25, 75]
        planned_event_count = 404
        # first one is from obj
        # second from color which is actually the worst
        expected_load_shedding_rate_choices = [0.00990099, 0.343234323]
        self.planner.all_services_worker_pool = self.all_services_worker_pool
        load_shedding_rate_choices = self.planner.update_workers_planned_resources(
            dataflow_choices_with_cum_weights, dataflow_choices_weights, planned_event_count
        )

        self.assertEqual(len(load_shedding_rate_choices), 2)
        self.assertAlmostEqual(load_shedding_rate_choices[0], expected_load_shedding_rate_choices[0])
        self.assertAlmostEqual(load_shedding_rate_choices[1], expected_load_shedding_rate_choices[1])

    def test_format_dataflow_choices_to_buffer_stream_choices_plan(self):
        dataflow_choices = [
            (25.0,
             ([25, 'ObjectDetection', 'object-detection-data'],
              [85, 'ColorDetection', 'color-detection-data'])),
            (100.0,
             ([75, 'ObjectDetection', 'object-detection-data2'],
              [85, 'ColorDetection', 'color-detection-data'],)),
        ]
        load_shedding_rate_choices = [0.00990099, 0.343234323]
        ret = self.planner.format_dataflow_choices_to_buffer_stream_choices_plan(
            dataflow_choices, load_shedding_rate_choices)

        expected = [
            (0.00990099, 25.0, [['object-detection-data'], ['color-detection-data'], ['wm-data']]),
            (0.343234323, 100.0, [['object-detection-data2'], ['color-detection-data'], ['wm-data']])
        ]

        self.assertListEqual(ret, expected)

    @patch('adaptation_planner.planners.load_shedding_based_scheduling.WeightedRandomQoSSinglePolicyLSSchedulerPlanner.get_buffer_stream_required_services')
    @patch('adaptation_planner.planners.load_shedding_based_scheduling.WeightedRandomQoSSinglePolicyLSSchedulerPlanner.get_bufferstream_planned_event_count')
    @patch('adaptation_planner.planners.load_shedding_based_scheduling.WeightedRandomQoSSinglePolicyLSSchedulerPlanner.create_filtered_and_weighted_workers_pool')
    @patch('adaptation_planner.planners.load_shedding_based_scheduling.WeightedRandomQoSSinglePolicyLSSchedulerPlanner.create_dataflow_choices_with_cum_weights_and_relative_weights')
    @patch('adaptation_planner.planners.load_shedding_based_scheduling.WeightedRandomQoSSinglePolicyLSSchedulerPlanner.update_workers_planned_resources')
    @patch('adaptation_planner.planners.load_shedding_based_scheduling.WeightedRandomQoSSinglePolicyLSSchedulerPlanner.format_dataflow_choices_to_buffer_stream_choices_plan')
    def test_create_buffer_stream_choices_plan_when_non_acc_max_qos(self, format_df, updated_res, create_choices, filtered, get_event_count, get_req_serv):
        self.all_queries_dict['3940d2cad2926150093a9a786163ee14']['qos_policies'] = {
            'latency': 'min'
        }

        self.planner.all_buffer_streams = self.all_buffer_streams
        self.planner.all_queries_dict = self.all_queries_dict
        self.planner.all_services_worker_pool = self.all_services_worker_pool
        bufferstream_entity = self.all_buffer_streams['b41eeb0408847b28474f362f5642635e']

        get_req_serv.return_value = ['ObjectDetection', 'ColorDetection']
        get_event_count.return_value = 10
        filtered.return_value = 'mocked_per_service_worker_keys_with_weights'
        create_choices.return_value = ['dataflow_choices', 'weights']
        format_df.return_value = 'mocked_buffer_stream_choices_plan'
        mocked_load_shedding = [0.5, 0.6]
        updated_res.return_value = mocked_load_shedding

        ret = self.planner.create_buffer_stream_choices_plan(bufferstream_entity)
        self.assertEqual(ret, 'mocked_buffer_stream_choices_plan')

        self.assertTrue(get_event_count.called)
        self.assertTrue(get_req_serv.called)

        self.assertTrue(filtered.called)
        filtered.assert_called_once_with(
            ['ObjectDetection', 'ColorDetection'], 10, 'latency', 'min')

        self.assertTrue(create_choices.called)
        create_choices.assert_called_once_with('mocked_per_service_worker_keys_with_weights')

        self.assertTrue(updated_res.called)
        updated_res.assert_called_once_with('dataflow_choices', 'weights', 10)

        self.assertTrue(format_df.called)
        format_df.assert_called_once_with('dataflow_choices', mocked_load_shedding)

    @patch('adaptation_planner.planners.load_shedding_based_scheduling.WeightedRandomQoSSinglePolicyLSSchedulerPlanner.get_buffer_stream_required_services')
    @patch('adaptation_planner.planners.load_shedding_based_scheduling.WeightedRandomQoSSinglePolicyLSSchedulerPlanner.get_bufferstream_planned_event_count')
    @patch('adaptation_planner.planners.load_shedding_based_scheduling.WeightedRandomQoSSinglePolicyLSSchedulerPlanner.create_filtered_and_weighted_workers_pool')
    @patch('adaptation_planner.planners.load_shedding_based_scheduling.WeightedRandomQoSSinglePolicyLSSchedulerPlanner.create_dataflow_choices_with_cum_weights_and_relative_weights')
    @patch('adaptation_planner.planners.load_shedding_based_scheduling.WeightedRandomQoSSinglePolicyLSSchedulerPlanner.update_workers_planned_resources')
    @patch('adaptation_planner.planners.load_shedding_based_scheduling.WeightedRandomQoSSinglePolicyLSSchedulerPlanner.format_dataflow_choices_to_buffer_stream_choices_plan')
    def test_create_buffer_stream_choices_plan_when_acc_max_qos(self, format_df, updated_res, create_choices, filtered, get_event_count, get_req_serv):
        self.planner.all_buffer_streams = self.all_buffer_streams
        self.planner.all_queries_dict = self.all_queries_dict
        self.planner.all_services_worker_pool = self.all_services_worker_pool
        bufferstream_entity = self.all_buffer_streams['b41eeb0408847b28474f362f5642635e']

        get_req_serv.return_value = ['ObjectDetection', 'ColorDetection']
        get_event_count.return_value = 10
        filtered.return_value = 'mocked_per_service_worker_keys_with_weights'
        create_choices.return_value = ['dataflow_choices', 'weights']
        format_df.return_value = 'mocked_buffer_stream_choices_plan'
        mocked_load_shedding = [0.5, 0.6]
        updated_res.return_value = mocked_load_shedding

        ret = self.planner.create_buffer_stream_choices_plan(bufferstream_entity)
        self.assertEqual(ret, 'mocked_buffer_stream_choices_plan')

        self.assertTrue(get_event_count.called)
        self.assertTrue(get_req_serv.called)

        self.assertTrue(filtered.called)
        filtered.assert_called_once_with(
            ['ObjectDetection', 'ColorDetection'], 10, 'accuracy', 'max')

        self.assertTrue(create_choices.called)
        create_choices.assert_called_once_with('mocked_per_service_worker_keys_with_weights')

        self.assertTrue(updated_res.called)
        updated_res.assert_called_once_with('dataflow_choices', 'weights', 10)

        self.assertTrue(format_df.called)
        format_df.assert_called_once_with('dataflow_choices', [0, 0])
