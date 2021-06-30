from unittest import TestCase
from unittest.mock import patch, MagicMock

from adaptation_planner.planners.load_shedding_based_scheduling import (
    SingleBestForQoSSinglePolicyLSSchedulerPlanner,
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
