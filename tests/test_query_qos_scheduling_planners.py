from unittest import TestCase
from unittest.mock import patch, MagicMock

from adaptation_planner.planners.qos_based_scheduling import (
    SingleBestForQoSSinglePolicySchedulerPlanner,
)


class TestSingleBestForQoSSinglePolicySchedulerPlanner(TestCase):

    def setUp(self):
        scheduler_cmd_stream_key = 'sc-cmd'
        ce_endpoint_stream_key = 'wm-cmd'
        parent_service = MagicMock()
        self.planner = SingleBestForQoSSinglePolicySchedulerPlanner(
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

    def test_workers_sorted_by_qos_accuracy_max(self):
        worker_pool = self.all_services_worker_pool['ObjectDetection']
        qos_policy_name = 'accuracy'
        qos_policy_value = 'max'
        ret = self.planner.workers_key_sorted_by_qos(
            worker_pool, qos_policy_name=qos_policy_name, qos_policy_value=qos_policy_value)
        self.assertListEqual(ret, ['object-detection-ssd-gpu-data', 'object-detection-ssd-data'])

    def test_workers_sorted_by_qos_latency_min(self):
        worker_pool = self.all_services_worker_pool['ObjectDetection']
        qos_policy_name = 'latency'
        qos_policy_value = 'min'
        ret = self.planner.workers_key_sorted_by_qos(
            worker_pool, qos_policy_name=qos_policy_name, qos_policy_value=qos_policy_value)
        self.assertListEqual(ret, ['object-detection-ssd-gpu-data', 'object-detection-ssd-data'])

    def test_workers_sorted_by_qos_energy_min(self):
        worker_pool = self.all_services_worker_pool['ObjectDetection']
        qos_policy_name = 'energy_consumption'
        qos_policy_value = 'min'
        ret = self.planner.workers_key_sorted_by_qos(
            worker_pool, qos_policy_name=qos_policy_name, qos_policy_value=qos_policy_value)
        self.assertListEqual(ret, ['object-detection-ssd-data', 'object-detection-ssd-gpu-data'])

    def test_create_buffer_stream_plan_accuracy_max(self):
        self.all_queries_dict['3940d2cad2926150093a9a786163ee14']['qos_policies'] = {
            'accuracy': 'max'
        }
        bufferstream_entity = self.all_buffer_streams['b41eeb0408847b28474f362f5642635e']
        self.planner.all_services_worker_pool = self.all_services_worker_pool
        ret = self.planner.create_buffer_stream_plan(bufferstream_entity)
        expected_bf_plan = [['object-detection-ssd-gpu-data'], ['wm-cmd']]
        self.assertListEqual(ret, expected_bf_plan)

    def test_create_buffer_stream_plan_latency_min(self):
        self.all_queries_dict['3940d2cad2926150093a9a786163ee14']['qos_policies'] = {
            'latency': 'min'
        }
        bufferstream_entity = self.all_buffer_streams['b41eeb0408847b28474f362f5642635e']
        self.planner.all_services_worker_pool = self.all_services_worker_pool
        ret = self.planner.create_buffer_stream_plan(bufferstream_entity)
        expected_bf_plan = [['object-detection-ssd-gpu-data'], ['wm-cmd']]
        self.assertListEqual(ret, expected_bf_plan)

    def test_create_buffer_stream_plan_energy_min(self):
        self.all_queries_dict['3940d2cad2926150093a9a786163ee14']['qos_policies'] = {
            'energy_consumption': 'min'
        }
        bufferstream_entity = self.all_buffer_streams['b41eeb0408847b28474f362f5642635e']
        self.planner.all_services_worker_pool = self.all_services_worker_pool
        ret = self.planner.create_buffer_stream_plan(bufferstream_entity)
        expected_bf_plan = [['object-detection-ssd-data'], ['wm-cmd']]
        self.assertListEqual(ret, expected_bf_plan)

    @patch('adaptation_planner.planners.qos_based_scheduling.SingleBestForQoSSinglePolicySchedulerPlanner.get_buffer_stream_required_services')
    @patch('adaptation_planner.planners.qos_based_scheduling.SingleBestForQoSSinglePolicySchedulerPlanner.get_bufferstream_planned_event_count')
    @patch('adaptation_planner.planners.qos_based_scheduling.SingleBestForQoSSinglePolicySchedulerPlanner.initialize_service_workers_planned_capacity')
    @patch('adaptation_planner.planners.qos_based_scheduling.SingleBestForQoSSinglePolicySchedulerPlanner.filter_overloaded_service_worker_pool_or_all_if_empty')
    @patch('adaptation_planner.planners.qos_based_scheduling.SingleBestForQoSSinglePolicySchedulerPlanner.workers_key_sorted_by_qos')
    @patch('adaptation_planner.planners.qos_based_scheduling.SingleBestForQoSSinglePolicySchedulerPlanner.update_workers_planned_resources')
    def test_create_buffer_stream_plan_calls_proper_methods(self, updated_res, w_sort, w_filter, w_init, event_count, req_serv):
        self.all_queries_dict['3940d2cad2926150093a9a786163ee14']['qos_policies'] = {
            'energy_consumption': 'min'
        }
        bufferstream_entity = self.all_buffer_streams['b41eeb0408847b28474f362f5642635e']
        req_serv.return_value = ['ObjectDetection']
        self.planner.all_services_worker_pool = self.all_services_worker_pool
        ret = self.planner.create_buffer_stream_plan(bufferstream_entity)
        self.assertTrue(updated_res.called)
        self.assertTrue(w_sort.called)
        self.assertTrue(w_filter.called)
        self.assertTrue(w_init.called)
        self.assertTrue(event_count.called)
        self.assertTrue(req_serv.called)

    def test_initialize_service_workers_planned_capacity(self):
        self.planner.adaptation_delta = 10
        self.planner.all_services_worker_pool = self.all_services_worker_pool
        worker_pool = self.planner.initialize_service_workers_planned_capacity(
            self.planner.all_services_worker_pool['ObjectDetection'])
        self.assertDictEqual(worker_pool, self.planner.all_services_worker_pool['ObjectDetection'])
        self.assertEqual(
            worker_pool['object-detection-ssd-data']['resources']['planned']['events_capacity'],
            150)
        self.assertEqual(
            worker_pool['object-detection-ssd-gpu-data']['resources']['planned']['events_capacity'],
            296)

    def test_update_workers_planned_resources_when_capacity_is_present(self):
        required_events = 101
        required_services = ['ObjectDetection']
        buffer_stream_plan = [['object-detection-ssd-data']]

        self.planner.all_services_worker_pool = self.all_services_worker_pool
        self.worker_a['resources']['planned']['events_capacity'] = 200

        self.planner.update_workers_planned_resources(required_services, buffer_stream_plan, required_events)
        self.assertEqual(
            self.worker_a['resources']['planned']['events_capacity'],
            99)

    def test_update_workers_planned_resources_when_capacity_is_not_present(self):
        required_events = 101
        required_services = ['ObjectDetection']
        buffer_stream_plan = [['object-detection-ssd-data']]

        self.planner.all_services_worker_pool = self.all_services_worker_pool
        self.worker_a['resources']['planned'].pop('events_capacity', None)

        self.planner.update_workers_planned_resources(required_services, buffer_stream_plan, required_events)
        self.assertEqual(
            self.worker_a['resources']['planned']['events_capacity'],
            -101)

    def test_update_workers_planned_resources_when_more_than_one_required_service(self):
        required_events = 10
        required_services = ['ObjectDetection', 'ObjectDetection']
        buffer_stream_plan = [['object-detection-ssd-data'], ['object-detection-ssd-gpu-data']]

        self.planner.all_services_worker_pool = self.all_services_worker_pool
        self.worker_a['resources']['planned']['events_capacity'] = 200
        self.worker_b['resources']['planned']['events_capacity'] = 5

        self.planner.update_workers_planned_resources(required_services, buffer_stream_plan, required_events)
        self.assertEqual(
            self.worker_a['resources']['planned']['events_capacity'],
            190)

        self.assertEqual(
            self.worker_b['resources']['planned']['events_capacity'],
            -5)

    def test_filter_overloaded_service_worker_pool_or_all_if_empty_with_at_least_one(self):
        self.planner.all_services_worker_pool = self.all_services_worker_pool
        self.worker_a['resources']['planned']['events_capacity'] = 10
        self.worker_b['resources']['planned']['events_capacity'] = -1

        worker_pool = self.planner.filter_overloaded_service_worker_pool_or_all_if_empty(
            self.planner.all_services_worker_pool['ObjectDetection'])
        self.assertIn('object-detection-ssd-data', worker_pool.keys())
        self.assertNotIn('object-detection-ssd-gpu-data', worker_pool.keys())

    def test_filter_overloaded_service_worker_pool_or_all_if_empty_with_none(self):
        self.planner.all_services_worker_pool = self.all_services_worker_pool
        self.worker_a['resources']['planned']['events_capacity'] = 0
        self.worker_b['resources']['planned']['events_capacity'] = -1

        worker_pool = self.planner.filter_overloaded_service_worker_pool_or_all_if_empty(
            self.planner.all_services_worker_pool['ObjectDetection'])
        self.assertIn('object-detection-ssd-data', worker_pool.keys())
        self.assertIn('object-detection-ssd-gpu-data', worker_pool.keys())

