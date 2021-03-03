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
