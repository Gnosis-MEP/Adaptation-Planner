from unittest import TestCase
from unittest.mock import patch, MagicMock

from adaptation_planner.planners.event_driven.qqos_based import (
    QQoS_TK_LP_SchedulerPlanner,
    QQoS_W_HP_SchedulerPlanner
)


class BaseSchedulerPlannerTestCase(TestCase):

    def initialize_planner(self):
        self.planner = None

    def setUp(self):
        self.ce_endpoint_stream_key = 'wm-data'
        self.parent_service = MagicMock()
        self.initialize_planner()

        self.all_queries = {
            "48321bfc85870426a28298662d458b10": {
                "subscriber_id": "sid",
                "query_id": "48321bfc85870426a28298662d458b10",
                "parsed_query": {
                    "name": "my_first_query",
                    "from": [
                        "pid"
                    ],
                    "content": [
                        "ObjectDetection",
                        "ColorDetection"
                    ],
                    "match": "MATCH (c1:Car {color:'blue'}), (c2:Car {color:'white'})",
                    "optional_match": "",
                    "where": "",
                    "window": {
                        "window_type": "TUMBLING_COUNT_WINDOW",
                        "args": [
                            2
                        ]
                    },
                    "ret": "RETURN *",
                    "qos_policies": {
                        "latency": "min"
                    }
                },
                "buffer_stream": {
                    "publisher_id": "pid",
                    "buffer_stream_key": "f79681aaa510938aca8c60506171d9d8",
                    "source": "psource",
                    "resolution": "300x300",
                    "fps": "30"
                },
                "service_chain": [
                    "ObjectDetection",
                    "ColorDetection"
                ],
                "id": "ClientManager:321aa0c7-171c-4d91-a312-dc00e2b7e6ba"
            }
        }

        self.all_buffer_streams = {
            "f79681aaa510938aca8c60506171d9d8": {
                "publisher_id": "pid",
                "buffer_stream_key": "f79681aaa510938aca8c60506171d9d8",
                "source": "psource",
                "resolution": "300x300",
                "fps": "30",
                "queries": {
                    "48321bfc85870426a28298662d458b10": self.all_queries["48321bfc85870426a28298662d458b10"]
                }
            }
        }

        self.worker_a = {
            'monitoring': {
                'accuracy': 0.6,
                'energy_consumption': 100.0,
                'queue_size': 0,
                'queue_limit': 100,
                'queue_space': 100,
                'queue_space_percent': 1.0,
                'service_type': 'ObjectDetection',
                'stream_key': 'object-detection-1',
                'throughput': 15,
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
                'accuracy': 0.9,
                'energy_consumption': 200.0,
                'queue_size': 4,
                'queue_limit': 100,
                'queue_space': 100,
                'queue_space_percent': 1.0,
                'service_type': 'ObjectDetection',
                'stream_key': 'object-detection-2',
                'throughput': 30,
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

        self.worker_c = {
            'monitoring': {
                'accuracy': 0.5,
                'energy_consumption': 100.0,
                'queue_size': 0,
                'queue_limit': 100,
                'queue_space': 100,
                'queue_space_percent': 1.0,
                'service_type': 'ColorDetection',
                'stream_key': 'color-detection',
                'throughput': 30,
            },
            'resources': {
                'planned': {
                    'queue_space': 100
                },
                'usage': {
                    'energy_consumption': 100.0
                }
            }
        }
        self.all_services_worker_pool = {
            'ObjectDetection': {
                'object-detection-1': self.worker_a,
                'object-detection-2': self.worker_b,
            },
            'ColorDetection': {
                self.worker_c['monitoring']['stream_key']: self.worker_c,
            }
        }


class TestQQoS_TK_LP_SchedulerPlanner(BaseSchedulerPlannerTestCase):

    def initialize_planner(self):
        self.planner = QQoS_TK_LP_SchedulerPlanner(
            self.parent_service, self.ce_endpoint_stream_key)

    def test_workers_sorted_by_qos_accuracy_max(self):
        worker_pool = self.all_services_worker_pool['ObjectDetection']
        qos_policy_name = 'accuracy'
        qos_policy_value = 'max'
        ret = self.planner.workers_key_sorted_by_qos(
            worker_pool, qos_policy_name=qos_policy_name, qos_policy_value=qos_policy_value)
        self.assertListEqual(ret, ['object-detection-2', 'object-detection-1'])

    def test_workers_sorted_by_qos_latency_min(self):
        worker_pool = self.all_services_worker_pool['ObjectDetection']
        qos_policy_name = 'latency'
        qos_policy_value = 'min'
        ret = self.planner.workers_key_sorted_by_qos(
            worker_pool, qos_policy_name=qos_policy_name, qos_policy_value=qos_policy_value)
        self.assertListEqual(ret, ['object-detection-2', 'object-detection-1'])

    def test_workers_sorted_by_qos_energy_min(self):
        worker_pool = self.all_services_worker_pool['ObjectDetection']
        qos_policy_name = 'energy_consumption'
        qos_policy_value = 'min'
        ret = self.planner.workers_key_sorted_by_qos(
            worker_pool, qos_policy_name=qos_policy_name, qos_policy_value=qos_policy_value)
        self.assertListEqual(ret, ['object-detection-1', 'object-detection-2'])

    def test_update_workers_planned_resources_when_capacity_is_present(self):
        required_events = 101
        required_services = ['ObjectDetection']
        buffer_stream_plan = [['object-detection-1']]

        self.planner.all_services_worker_pool = self.all_services_worker_pool
        self.worker_a['resources']['planned']['events_capacity'] = 200

        self.planner.update_workers_planned_resources(required_services, buffer_stream_plan, required_events)
        self.assertEqual(
            self.worker_a['resources']['planned']['events_capacity'],
            99)

    def test_update_workers_planned_resources_when_capacity_is_not_present(self):
        required_events = 101
        required_services = ['ObjectDetection']
        buffer_stream_plan = [['object-detection-1']]

        self.planner.all_services_worker_pool = self.all_services_worker_pool
        self.worker_a['resources']['planned'].pop('events_capacity', None)

        self.planner.update_workers_planned_resources(required_services, buffer_stream_plan, required_events)
        self.assertEqual(
            self.worker_a['resources']['planned']['events_capacity'],
            -101)

    def test_update_workers_planned_resources_when_more_than_one_required_service(self):
        required_events = 10
        required_services = ['ObjectDetection', 'ObjectDetection']
        buffer_stream_plan = [['object-detection-1'], ['object-detection-2']]

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

    def test_create_buffer_stream_plan_accuracy_max(self):
        self.all_queries['48321bfc85870426a28298662d458b10']['parsed_query']['qos_policies'] = {
            'accuracy': 'max'
        }
        bufferstream_entity = self.all_buffer_streams['f79681aaa510938aca8c60506171d9d8']
        self.planner.all_services_worker_pool = self.all_services_worker_pool
        ret = self.planner.create_buffer_stream_plan(bufferstream_entity)
        expected_bf_plan = [['object-detection-2'], ['color-detection'], ['wm-data']]
        self.assertListEqual(ret, expected_bf_plan)

    def test_create_buffer_stream_plan_latency_min(self):
        self.all_queries['48321bfc85870426a28298662d458b10']['parsed_query']['qos_policies'] = {
            'latency': 'min'
        }
        bufferstream_entity = self.all_buffer_streams['f79681aaa510938aca8c60506171d9d8']
        self.planner.all_services_worker_pool = self.all_services_worker_pool
        ret = self.planner.create_buffer_stream_plan(bufferstream_entity)
        expected_bf_plan = [['object-detection-2'], ['color-detection'], ['wm-data']]
        self.assertListEqual(ret, expected_bf_plan)

    def test_create_buffer_stream_plan_energy_min(self):
        self.all_queries['48321bfc85870426a28298662d458b10']['parsed_query']['qos_policies'] = {
            'energy_consumption': 'min'
        }
        bufferstream_entity = self.all_buffer_streams['f79681aaa510938aca8c60506171d9d8']
        self.planner.all_services_worker_pool = self.all_services_worker_pool
        ret = self.planner.create_buffer_stream_plan(bufferstream_entity)
        expected_bf_plan = [['object-detection-1'], ['color-detection'], ['wm-data']]
        self.assertListEqual(ret, expected_bf_plan)

    @patch('adaptation_planner.planners.event_driven.qqos_based.QQoS_TK_LP_SchedulerPlanner.create_buffer_stream_plan')
    def test_create_buffer_stream_choices_plan_has_correct_return_format(self, mocked_create_plan):
        self.all_queries['48321bfc85870426a28298662d458b10']['parsed_query']['qos_policies'] = {
            'energy_consumption': 'min'
        }
        bufferstream_entity = self.all_buffer_streams['f79681aaa510938aca8c60506171d9d8']
        mocked_create_plan.return_value = 'buffer_stream_plan'

        ret = self.planner.create_buffer_stream_choices_plan(bufferstream_entity)

        expected_bf_plan = [(None, 'buffer_stream_plan')]
        self.assertListEqual(expected_bf_plan, ret)


class TestQQoS_W_HP_SchedulerPlanner(BaseSchedulerPlannerTestCase):

    def initialize_planner(self):
        self.planner = QQoS_W_HP_SchedulerPlanner(
            self.parent_service, self.ce_endpoint_stream_key)

    def test_create_filtered_and_weighted_workers_pool(self):
        self.all_services_worker_pool['ObjectDetection']['object-detection-1']['monitoring']['accuracy'] = 0.9
        self.all_services_worker_pool['ObjectDetection']['object-detection-1']['resources']['planned']['events_capacity'] = 0

        self.all_services_worker_pool['ObjectDetection']['object-detection-2']['monitoring']['accuracy'] = 0.6
        self.all_services_worker_pool['ObjectDetection']['object-detection-2']['resources']['planned']['events_capacity'] = 50

        self.all_services_worker_pool['ColorDetection']['color-detection']['monitoring']['accuracy'] = 0.3
        self.all_services_worker_pool['ColorDetection']['color-detection']['resources']['planned']['events_capacity'] = 100

        self.planner.all_services_worker_pool = self.all_services_worker_pool
        qos_policy_name = 'accuracy'
        qos_policy_value = 'max'
        required_services = ['ObjectDetection', 'ColorDetection']
        planned_event_count = 100
        ret = self.planner.create_filtered_and_weighted_workers_pool(
            required_services, planned_event_count, qos_policy_name, qos_policy_value
        )
        self.assertIn('ObjectDetection', ret.keys())
        self.assertListEqual([(0.306, 'ObjectDetection', 'object-detection-2')], ret['ObjectDetection'])
        self.assertIn('ColorDetection', ret.keys())
        self.assertEqual([(0.3, 'ColorDetection', 'color-detection')], ret['ColorDetection'])

    def test_get_worker_congestion_impact_rate_when_events_cap_greater_than_0(self):
        self.worker_a['resources']['planned']['events_capacity'] = 2

        planned_event_count = 10
        ret = self.planner.get_worker_congestion_impact_rate(self.worker_a, planned_event_count)
        self.assertAlmostEqual(ret, 0.3)

    def test_get_worker_congestion_impact_rate_when_events_cap_is_0(self):
        self.worker_a['resources']['planned']['events_capacity'] = 0
        planned_event_count = 10
        ret = self.planner.get_worker_congestion_impact_rate(self.worker_a, planned_event_count)
        self.assertAlmostEqual(ret, 0.1)

    def test_get_worker_congestion_impact_rate_when_events_cap_is_negative(self):
        self.worker_a['resources']['planned']['events_capacity'] = -2
        planned_event_count = 10
        ret = self.planner.get_worker_congestion_impact_rate(self.worker_a, planned_event_count)
        self.assertAlmostEqual(ret, 0.05)

    def test_get_worker_congestion_impact_rate_shouldnt_be_more_than_one(self):
        self.worker_a['resources']['planned']['events_capacity'] = 20
        planned_event_count = 10
        ret = self.planner.get_worker_congestion_impact_rate(self.worker_a, planned_event_count)
        self.assertEqual(ret, 1)

    @patch('adaptation_planner.planners.event_driven.qqos_based.QQoS_W_HP_SchedulerPlanner.get_worker_congestion_impact_rate')
    def test_get_worker_choice_weight_for_qos_policy_accuracy_max_when_no_congestion(self, get_con_rate):
        qos_policy_name = 'accuracy'
        qos_policy_value = 'max'
        planned_event_count = 10
        get_con_rate.return_value = 1
        self.worker_a['monitoring'][qos_policy_name] = 0.9

        ret = self.planner.get_worker_choice_weight_for_qos_policy(
            self.worker_a, planned_event_count, qos_policy_name, qos_policy_value)
        self.assertAlmostEqual(ret, 0.9)

    @patch('adaptation_planner.planners.event_driven.qqos_based.QQoS_W_HP_SchedulerPlanner.get_worker_congestion_impact_rate')
    def test_get_worker_choice_weight_for_qos_policy_accuracy_max_with_congestion(self, get_con_rate):
        qos_policy_name = 'accuracy'
        qos_policy_value = 'max'
        planned_event_count = 10
        get_con_rate.return_value = 0.25
        self.worker_a['monitoring'][qos_policy_name] = 0.9

        ret = self.planner.get_worker_choice_weight_for_qos_policy(
            self.worker_a, planned_event_count, qos_policy_name, qos_policy_value)
        self.assertAlmostEqual(ret, 0.225)

    @patch('adaptation_planner.planners.event_driven.qqos_based.QQoS_W_HP_SchedulerPlanner.get_worker_congestion_impact_rate')
    def test_get_worker_choice_weight_for_qos_policy_energy_min_with_congestion(self, get_con_rate):
        qos_policy_name = 'energy_consumption'
        qos_policy_value = 'min'
        planned_event_count = 10
        get_con_rate.return_value = 0.5
        self.worker_a['monitoring'][qos_policy_name] = 10

        ret = self.planner.get_worker_choice_weight_for_qos_policy(
            self.worker_a, planned_event_count, qos_policy_name, qos_policy_value)
        self.assertAlmostEqual(ret, 0.05)

    @patch('adaptation_planner.planners.event_driven.qqos_based.QQoS_W_HP_SchedulerPlanner.get_worker_congestion_impact_rate')
    def test_get_worker_choice_weight_for_qos_policy_energy_min_with_congestion_even_worst_energy(self, get_con_rate):
        qos_policy_name = 'energy_consumption'
        qos_policy_value = 'min'
        planned_event_count = 10
        get_con_rate.return_value = 0.5
        self.worker_a['monitoring'][qos_policy_name] = 20

        ret = self.planner.get_worker_choice_weight_for_qos_policy(
            self.worker_a, planned_event_count, qos_policy_name, qos_policy_value)
        self.assertAlmostEqual(ret, 0.025)

    @patch('adaptation_planner.planners.event_driven.qqos_based.QQoS_W_HP_SchedulerPlanner.get_worker_congestion_impact_rate')
    def test_get_worker_choice_weight_for_qos_policy_latency_min_with_congestion(self, get_con_rate):
        qos_policy_name = 'latency'
        qos_policy_value = 'min'
        planned_event_count = 10
        get_con_rate.return_value = 0.5
        self.worker_a['monitoring']['throughput'] = 10

        ret = self.planner.get_worker_choice_weight_for_qos_policy(
            self.worker_a, planned_event_count, qos_policy_name, qos_policy_value)
        self.assertAlmostEqual(ret, 5.0)

    @patch('adaptation_planner.planners.event_driven.qqos_based.QQoS_W_HP_SchedulerPlanner.get_worker_congestion_impact_rate')
    def test_get_worker_choice_weight_for_qos_policy_latency_min_with_congestion_even_better_throughput(self, get_con_rate):
        qos_policy_name = 'latency'
        qos_policy_value = 'min'
        planned_event_count = 10
        get_con_rate.return_value = 0.5
        self.worker_a['monitoring']['throughput'] = 15

        ret = self.planner.get_worker_choice_weight_for_qos_policy(
            self.worker_a, planned_event_count, qos_policy_name, qos_policy_value)
        self.assertAlmostEqual(ret, 7.5)

    def test_get_dataflow_choice_min_weight(self):
        service_worker_w_tuple1 = [10, 'ObjectDetection', 'object-detection-data']
        service_worker_w_tuple2 = [5, 'ColorDetection', 'color-detection-data2']
        service_worker_w_tuple3 = [15, 'RainDetection', 'rain-detection-data3']
        dataflow_choice = [service_worker_w_tuple1, service_worker_w_tuple2, service_worker_w_tuple3]
        ret = self.planner.get_dataflow_choice_min_weight(dataflow_choice)

        self.assertEqual(ret, 5)

    def test_create_cartesian_product_dataflow_choices(self):
        service_worker_w_tuple_a1 = [1, 'ObjectDetection', 'object-detection-data']
        service_worker_w_tuple_a2 = [2, 'ColorDetection', 'color-detection-data']
        service_worker_w_tuple_a3 = [3, 'RainDetection', 'rain-detection-data']

        service_worker_w_tuple_b1 = [4, 'ObjectDetection', 'object-detection-data2']
        service_worker_w_tuple_b2 = [5, 'ColorDetection', 'color-detection-data2']
        service_worker_w_tuple_b3 = [6, 'RainDetection', 'rain-detection-data2']

        per_service_worker_keys_with_weights = {
            'ObjectDetection': [service_worker_w_tuple_a1, service_worker_w_tuple_b1],
            'ColorDetection': [service_worker_w_tuple_a2, service_worker_w_tuple_b2],
            'RainDetection': [service_worker_w_tuple_a3, service_worker_w_tuple_b3],

        }
        ret = self.planner.create_cartesian_product_dataflow_choices(per_service_worker_keys_with_weights)
        expected = [
            ([1, 'ObjectDetection', 'object-detection-data'],
             [2, 'ColorDetection', 'color-detection-data'],
             [3, 'RainDetection', 'rain-detection-data']),
            ([1, 'ObjectDetection', 'object-detection-data'],
             [2, 'ColorDetection', 'color-detection-data'],
             [6, 'RainDetection', 'rain-detection-data2']),
            ([1, 'ObjectDetection', 'object-detection-data'],
             [5, 'ColorDetection', 'color-detection-data2'],
             [3, 'RainDetection', 'rain-detection-data']),
            ([1, 'ObjectDetection', 'object-detection-data'],
             [5, 'ColorDetection', 'color-detection-data2'],
             [6, 'RainDetection', 'rain-detection-data2']),
            ([4, 'ObjectDetection', 'object-detection-data2'],
             [2, 'ColorDetection', 'color-detection-data'],
             [3, 'RainDetection', 'rain-detection-data']),
            ([4, 'ObjectDetection', 'object-detection-data2'],
             [2, 'ColorDetection', 'color-detection-data'],
             [6, 'RainDetection', 'rain-detection-data2']),
            ([4, 'ObjectDetection', 'object-detection-data2'],
             [5, 'ColorDetection', 'color-detection-data2'],
             [3, 'RainDetection', 'rain-detection-data']),
            ([4, 'ObjectDetection', 'object-detection-data2'],
             [5, 'ColorDetection', 'color-detection-data2'],
             [6, 'RainDetection', 'rain-detection-data2'])
        ]

        self.assertEqual(len(ret), 8)
        self.assertListEqual(ret, expected)


    def test_create_dataflow_choices_with_cum_weights(self):
        dataflow_choices = [
            ([1, 'ObjectDetection', 'object-detection-data'],
             [2, 'ColorDetection', 'color-detection-data'],
             [3, 'RainDetection', 'rain-detection-data']),
            ([1, 'ObjectDetection', 'object-detection-data'],
             [2, 'ColorDetection', 'color-detection-data'],
             [6, 'RainDetection', 'rain-detection-data2']),
            ([1, 'ObjectDetection', 'object-detection-data'],
             [5, 'ColorDetection', 'color-detection-data2'],
             [3, 'RainDetection', 'rain-detection-data']),
            ([1, 'ObjectDetection', 'object-detection-data'],
             [5, 'ColorDetection', 'color-detection-data2'],
             [6, 'RainDetection', 'rain-detection-data2']),
            ([4, 'ObjectDetection', 'object-detection-data2'],
             [2, 'ColorDetection', 'color-detection-data'],
             [3, 'RainDetection', 'rain-detection-data']),
            ([4, 'ObjectDetection', 'object-detection-data2'],
             [2, 'ColorDetection', 'color-detection-data'],
             [6, 'RainDetection', 'rain-detection-data2']),
            ([4, 'ObjectDetection', 'object-detection-data2'],
             [5, 'ColorDetection', 'color-detection-data2'],
             [3, 'RainDetection', 'rain-detection-data']),
            ([4, 'ObjectDetection', 'object-detection-data2'],
             [5, 'ColorDetection', 'color-detection-data2'],
             [6, 'RainDetection', 'rain-detection-data2'])
        ]
        dataflow_relative_weights = [1, 1, 1, 1, 2, 2, 3, 4]
        ret = self.planner.create_dataflow_choices_with_cum_weights(
            dataflow_choices, dataflow_relative_weights
        )
        expected = [
            (1.0,
             ([1, 'ObjectDetection', 'object-detection-data'],
              [2, 'ColorDetection', 'color-detection-data'],
                 [3, 'RainDetection', 'rain-detection-data'])),
            (2.0,
             ([1, 'ObjectDetection', 'object-detection-data'],
              [2, 'ColorDetection', 'color-detection-data'],
              [6, 'RainDetection', 'rain-detection-data2'])),
            (3.0,
             ([1, 'ObjectDetection', 'object-detection-data'],
              [5, 'ColorDetection', 'color-detection-data2'],
              [3, 'RainDetection', 'rain-detection-data'])),
            (4.0,
             ([1, 'ObjectDetection', 'object-detection-data'],
              [5, 'ColorDetection', 'color-detection-data2'],
              [6, 'RainDetection', 'rain-detection-data2'])),
            (6.0,
             ([4, 'ObjectDetection', 'object-detection-data2'],
              [2, 'ColorDetection', 'color-detection-data'],
              [3, 'RainDetection', 'rain-detection-data'])),
            (8.0,
             ([4, 'ObjectDetection', 'object-detection-data2'],
              [2, 'ColorDetection', 'color-detection-data'],
              [6, 'RainDetection', 'rain-detection-data2'])),
            (11.0,
             ([4, 'ObjectDetection', 'object-detection-data2'],
              [5, 'ColorDetection', 'color-detection-data2'],
              [3, 'RainDetection', 'rain-detection-data'])),
            (15.0,
             ([4, 'ObjectDetection', 'object-detection-data2'],
              [5, 'ColorDetection', 'color-detection-data2'],
              [6, 'RainDetection', 'rain-detection-data2']))
        ]
        self.assertEqual(len(ret), 8)
        self.assertListEqual(ret, expected)

    def test_create_dataflow_choices_with_cum_weights_and_relative_weights(self):
        service_worker_w_tuple_a1 = [10, 'ObjectDetection', 'object-detection-data']
        service_worker_w_tuple_a2 = [5, 'ColorDetection', 'color-detection-data']
        service_worker_w_tuple_a3 = [15, 'RainDetection', 'rain-detection-data']

        service_worker_w_tuple_b1 = [9, 'ObjectDetection', 'object-detection-data2']
        service_worker_w_tuple_b2 = [8, 'ColorDetection', 'color-detection-data2']
        service_worker_w_tuple_b3 = [13, 'RainDetection', 'rain-detection-data2']

        per_service_worker_keys_with_weights = {
            'ObjectDetection': [service_worker_w_tuple_a1, service_worker_w_tuple_b1],
            'ColorDetection': [service_worker_w_tuple_a2, service_worker_w_tuple_b2],
            'RainDetection': [service_worker_w_tuple_a3, service_worker_w_tuple_b3],

        }
        ret = self.planner.create_dataflow_choices_with_cum_weights_and_relative_weights(
            per_service_worker_keys_with_weights
        )
        dataflow_choices = ret[0]
        dataflow_relative_weights = ret[1]
        expected_ret = [(5.0,
                         ([10, 'ObjectDetection', 'object-detection-data'],
                          [5, 'ColorDetection', 'color-detection-data'],
                             [15, 'RainDetection', 'rain-detection-data'])),
                        (10.0,
                         ([10, 'ObjectDetection', 'object-detection-data'],
                          [5, 'ColorDetection', 'color-detection-data'],
                             [13, 'RainDetection', 'rain-detection-data2'])),
                        (18.0,
                         ([10, 'ObjectDetection', 'object-detection-data'],
                          [8, 'ColorDetection', 'color-detection-data2'],
                             [15, 'RainDetection', 'rain-detection-data'])),
                        (26.0,
                         ([10, 'ObjectDetection', 'object-detection-data'],
                          [8, 'ColorDetection', 'color-detection-data2'],
                             [13, 'RainDetection', 'rain-detection-data2'])),
                        (31.0,
                         ([9, 'ObjectDetection', 'object-detection-data2'],
                          [5, 'ColorDetection', 'color-detection-data'],
                             [15, 'RainDetection', 'rain-detection-data'])),
                        (36.0,
                         ([9, 'ObjectDetection', 'object-detection-data2'],
                          [5, 'ColorDetection', 'color-detection-data'],
                             [13, 'RainDetection', 'rain-detection-data2'])),
                        (44.0,
                         ([9, 'ObjectDetection', 'object-detection-data2'],
                          [8, 'ColorDetection', 'color-detection-data2'],
                             [15, 'RainDetection', 'rain-detection-data'])),
                        (52.0,
                         ([9, 'ObjectDetection', 'object-detection-data2'],
                          [8, 'ColorDetection', 'color-detection-data2'],
                             [13, 'RainDetection', 'rain-detection-data2']))
                        ]

        self.assertListEqual(dataflow_choices, expected_ret)
        self.assertListEqual(dataflow_relative_weights, [5, 5, 8, 8, 5, 5, 8, 8])

    def test_update_workers_planned_resources(self):

        self.all_services_worker_pool['ObjectDetection']['object-detection-1']['resources']['planned']['events_capacity'] = 100

        self.all_services_worker_pool['ObjectDetection']['object-detection-2']['resources']['planned']['events_capacity'] = 200

        self.all_services_worker_pool['ColorDetection']['color-detection']['resources']['planned']['events_capacity'] = 300

        dataflow_choices_with_cum_weights = [
            (25.0,
             ([25, 'ObjectDetection', 'object-detection-1'],
              [85, 'ColorDetection', 'color-detection'])),
            (100.0,
             ([75, 'ObjectDetection', 'object-detection-2'],
              [85, 'ColorDetection', 'color-detection'],)),
        ]
        dataflow_choices_weights = [25, 75]
        planned_event_count = 100
        self.planner.all_services_worker_pool = self.all_services_worker_pool

        self.planner.update_workers_planned_resources(
            dataflow_choices_with_cum_weights, dataflow_choices_weights, planned_event_count
        )

        expected_obj1_detection_events_cap = 75
        expected_obj2_detection_events_cap = 125
        expected_color_detection_events_cap = 200
        obj1_detection_worker = self.planner.all_services_worker_pool['ObjectDetection']['object-detection-1']
        obj2_detection_worker = self.planner.all_services_worker_pool['ObjectDetection']['object-detection-2']
        color_detection_worker = self.planner.all_services_worker_pool['ColorDetection']['color-detection']

        self.assertAlmostEqual(
            obj1_detection_worker['resources']['planned']['events_capacity'],
            expected_obj1_detection_events_cap
        )
        self.assertAlmostEqual(
            obj2_detection_worker['resources']['planned']['events_capacity'],
            expected_obj2_detection_events_cap
        )

        self.assertAlmostEqual(
            color_detection_worker['resources']['planned']['events_capacity'],
            expected_color_detection_events_cap
        )

    def test_format_dataflow_choices_to_buffer_stream_choices_plan(self):
        dataflow_choices = [
            (25.0,
             ([25, 'ObjectDetection', 'object-detection-data'],
              [85, 'ColorDetection', 'color-detection-data'])),
            (100.0,
             ([75, 'ObjectDetection', 'object-detection-data2'],
              [85, 'ColorDetection', 'color-detection-data'],)),
        ]
        ret = self.planner.format_dataflow_choices_to_buffer_stream_choices_plan(dataflow_choices)

        expected = [
            (25.0, [['object-detection-data'], ['color-detection-data'], ['wm-data']]),
            (100.0, [['object-detection-data2'], ['color-detection-data'], ['wm-data']])
        ]

        self.assertListEqual(ret, expected)


    @patch('adaptation_planner.planners.event_driven.qqos_based.QQoS_W_HP_SchedulerPlanner.get_buffer_stream_required_services')
    @patch('adaptation_planner.planners.event_driven.qqos_based.QQoS_W_HP_SchedulerPlanner.get_bufferstream_planned_event_count')
    @patch('adaptation_planner.planners.event_driven.qqos_based.QQoS_W_HP_SchedulerPlanner.create_filtered_and_weighted_workers_pool')
    @patch('adaptation_planner.planners.event_driven.qqos_based.QQoS_W_HP_SchedulerPlanner.create_dataflow_choices_with_cum_weights_and_relative_weights')
    @patch('adaptation_planner.planners.event_driven.qqos_based.QQoS_W_HP_SchedulerPlanner.update_workers_planned_resources')
    @patch('adaptation_planner.planners.event_driven.qqos_based.QQoS_W_HP_SchedulerPlanner.format_dataflow_choices_to_buffer_stream_choices_plan')
    def test_create_buffer_stream_choices_plan(self, format_df, updated_res, create_choices, filtered, get_event_count, get_req_serv):

        self.planner.all_buffer_streams = self.all_buffer_streams
        self.planner.all_queries = self.all_queries
        self.planner.all_services_worker_pool = self.all_services_worker_pool
        bufferstream_entity = self.all_buffer_streams['f79681aaa510938aca8c60506171d9d8']

        get_req_serv.return_value = ['ObjectDetection', 'ColorDetection']
        get_event_count.return_value = 10
        filtered.return_value = 'mocked_per_service_worker_keys_with_weights'
        create_choices.return_value = ['dataflow_choices', 'weights']
        format_df.return_value = 'mocked_buffer_stream_choices_plan'

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
        format_df.assert_called_once_with('dataflow_choices')

# class TestSingleBestForQoSSinglePolicySchedulerPlanner(TestCase):

#     def setUp(self):
#         scheduler_cmd_stream_key = 'sc-cmd'
#         ce_endpoint_stream_key = 'wm-data'
#         parent_service = MagicMock()
#         self.planner = SingleBestForQoSSinglePolicySchedulerPlanner(
#             parent_service, scheduler_cmd_stream_key, ce_endpoint_stream_key)
#         self.all_queries_dict = {
#             '3940d2cad2926150093a9a786163ee14': {
#                 'action': 'updateControlFlow',
#                 'data_flow': ['ObjectDetection'],
#                 'publisher_id': 'publisher1',
#                 'qos_policies': {
#                     'accuracy': 'min'
#                 },
#                 'query_id': '3940d2cad2926150093a9a786163ee14',
#                 'type': 'http://gnosis-mep.org/subscriber_query'
#             }
#         }
#         self.all_buffer_streams = {
#             'b41eeb0408847b28474f362f5642635e': {
#                 'action': 'startPreprocessing',
#                 'buffer_stream_key': 'b41eeb0408847b28474f362f5642635e',
#                 'fps': '30',
#                 'publisher_id': 'publisher1',
#                 'queries': self.all_queries_dict,
#                 'resolution': '640x480',
#                 'source': 'rtmp://172.17.0.1/vod2/cars.mp4',
#                 'type': 'http://gnosis-mep.org/buffer_stream'
#             }
#         }

#         self.worker_a = {
#             'monitoring': {
#                 'accuracy': '0.6',
#                 # 'content_types': 'node:person',
#                 'energy_consumption': '100.0',
#                 'queue_size': 0,
#                 'queue_limit': 100,
#                 'queue_space': 100,
#                 'queue_space_percent': 1.0,
#                 'service_type': 'ObjectDetection',
#                 'stream_key': 'object-detection-ssd-data',
#                 'throughput': '15',
#                 'type': 'http://gnosis-mep.org/service_worker'
#             },
#             'resources': {
#                 'planned': {
#                     'queue_space': 100
#                 },
#                 'usage': {
#                     'energy_consumption': 108.0
#                 }
#             }
#         }
#         self.worker_b = {
#             'monitoring': {
#                 'accuracy': '0.9',
#                 # 'content_types': 'node_attribute:label',
#                 'energy_consumption': '200.0',
#                 'queue_size': 4,
#                 'queue_limit': 100,
#                 'queue_space': 100,
#                 'queue_space_percent': 1.0,
#                 'service_type': 'ObjectDetection',
#                 'stream_key': 'object-detection-ssd-gpu-data',
#                 'throughput': '30',
#                 'type': 'http://gnosis-mep.org/service_worker'
#             },
#             'resources': {
#                 'planned': {
#                     'queue_space': 100
#                 },
#                 'usage': {
#                     'energy_consumption': 108.0
#                 }
#             }
#         }
#         self.all_services_worker_pool = {
#             'ObjectDetection': {
#                 'object-detection-ssd-data': self.worker_a,
#                 'object-detection-ssd-gpu-data': self.worker_b,
#             }
#         }

#     def test_workers_sorted_by_qos_accuracy_max(self):
#         worker_pool = self.all_services_worker_pool['ObjectDetection']
#         qos_policy_name = 'accuracy'
#         qos_policy_value = 'max'
#         ret = self.planner.workers_key_sorted_by_qos(
#             worker_pool, qos_policy_name=qos_policy_name, qos_policy_value=qos_policy_value)
#         self.assertListEqual(ret, ['object-detection-ssd-gpu-data', 'object-detection-ssd-data'])

#     def test_workers_sorted_by_qos_latency_min(self):
#         worker_pool = self.all_services_worker_pool['ObjectDetection']
#         qos_policy_name = 'latency'
#         qos_policy_value = 'min'
#         ret = self.planner.workers_key_sorted_by_qos(
#             worker_pool, qos_policy_name=qos_policy_name, qos_policy_value=qos_policy_value)
#         self.assertListEqual(ret, ['object-detection-ssd-gpu-data', 'object-detection-ssd-data'])

#     def test_workers_sorted_by_qos_energy_min(self):
#         worker_pool = self.all_services_worker_pool['ObjectDetection']
#         qos_policy_name = 'energy_consumption'
#         qos_policy_value = 'min'
#         ret = self.planner.workers_key_sorted_by_qos(
#             worker_pool, qos_policy_name=qos_policy_name, qos_policy_value=qos_policy_value)
#         self.assertListEqual(ret, ['object-detection-ssd-data', 'object-detection-ssd-gpu-data'])

#     def test_create_buffer_stream_plan_accuracy_max(self):
#         self.all_queries_dict['3940d2cad2926150093a9a786163ee14']['qos_policies'] = {
#             'accuracy': 'max'
#         }
#         bufferstream_entity = self.all_buffer_streams['b41eeb0408847b28474f362f5642635e']
#         self.planner.all_services_worker_pool = self.all_services_worker_pool
#         ret = self.planner.create_buffer_stream_plan(bufferstream_entity)
#         expected_bf_plan = [['object-detection-ssd-gpu-data'], ['wm-data']]
#         self.assertListEqual(ret, expected_bf_plan)

#     def test_create_buffer_stream_plan_latency_min(self):
#         self.all_queries_dict['3940d2cad2926150093a9a786163ee14']['qos_policies'] = {
#             'latency': 'min'
#         }
#         bufferstream_entity = self.all_buffer_streams['b41eeb0408847b28474f362f5642635e']
#         self.planner.all_services_worker_pool = self.all_services_worker_pool
#         ret = self.planner.create_buffer_stream_plan(bufferstream_entity)
#         expected_bf_plan = [['object-detection-ssd-gpu-data'], ['wm-data']]
#         self.assertListEqual(ret, expected_bf_plan)

#     def test_create_buffer_stream_plan_energy_min(self):
#         self.all_queries_dict['3940d2cad2926150093a9a786163ee14']['qos_policies'] = {
#             'energy_consumption': 'min'
#         }
#         bufferstream_entity = self.all_buffer_streams['b41eeb0408847b28474f362f5642635e']
#         self.planner.all_services_worker_pool = self.all_services_worker_pool
#         ret = self.planner.create_buffer_stream_plan(bufferstream_entity)
#         expected_bf_plan = [['object-detection-ssd-data'], ['wm-data']]
#         self.assertListEqual(ret, expected_bf_plan)

#     @patch('adaptation_planner.planners.qos_based_scheduling.SingleBestForQoSSinglePolicySchedulerPlanner.get_buffer_stream_required_services')
#     @patch('adaptation_planner.planners.qos_based_scheduling.SingleBestForQoSSinglePolicySchedulerPlanner.get_bufferstream_planned_event_count')
#     @patch('adaptation_planner.planners.qos_based_scheduling.SingleBestForQoSSinglePolicySchedulerPlanner.initialize_service_workers_planned_capacity')
#     @patch('adaptation_planner.planners.qos_based_scheduling.SingleBestForQoSSinglePolicySchedulerPlanner.filter_best_than_avg_and_overloaded_service_worker_pool_or_all')
#     @patch('adaptation_planner.planners.qos_based_scheduling.SingleBestForQoSSinglePolicySchedulerPlanner.workers_key_sorted_by_qos')
#     @patch('adaptation_planner.planners.qos_based_scheduling.SingleBestForQoSSinglePolicySchedulerPlanner.update_workers_planned_resources')
#     def test_create_buffer_stream_plan_calls_proper_methods_if_energy_reduct(self, updated_res, w_sort, w_filter, w_init, event_count, req_serv):
#         self.all_queries_dict['3940d2cad2926150093a9a786163ee14']['qos_policies'] = {
#             'energy_consumption': 'min'
#         }
#         bufferstream_entity = self.all_buffer_streams['b41eeb0408847b28474f362f5642635e']
#         req_serv.return_value = ['ObjectDetection']
#         self.planner.all_services_worker_pool = self.all_services_worker_pool
#         ret = self.planner.create_buffer_stream_plan(bufferstream_entity)
#         self.assertTrue(updated_res.called)
#         self.assertTrue(w_sort.called)
#         self.assertTrue(w_filter.called)
#         self.assertTrue(w_init.called)
#         self.assertTrue(event_count.called)
#         self.assertTrue(req_serv.called)

#     @patch('adaptation_planner.planners.qos_based_scheduling.SingleBestForQoSSinglePolicySchedulerPlanner.get_buffer_stream_required_services')
#     @patch('adaptation_planner.planners.qos_based_scheduling.SingleBestForQoSSinglePolicySchedulerPlanner.get_bufferstream_planned_event_count')
#     @patch('adaptation_planner.planners.qos_based_scheduling.SingleBestForQoSSinglePolicySchedulerPlanner.initialize_service_workers_planned_capacity')
#     @patch('adaptation_planner.planners.qos_based_scheduling.SingleBestForQoSSinglePolicySchedulerPlanner.filter_overloaded_service_worker_pool_or_all_if_empty')
#     @patch('adaptation_planner.planners.qos_based_scheduling.SingleBestForQoSSinglePolicySchedulerPlanner.workers_key_sorted_by_qos')
#     @patch('adaptation_planner.planners.qos_based_scheduling.SingleBestForQoSSinglePolicySchedulerPlanner.update_workers_planned_resources')
#     def test_create_buffer_stream_plan_calls_proper_methods_if_not_energy_reduct(self, updated_res, w_sort, w_filter, w_init, event_count, req_serv):
#         self.all_queries_dict['3940d2cad2926150093a9a786163ee14']['qos_policies'] = {
#             'latency': 'min'
#         }
#         bufferstream_entity = self.all_buffer_streams['b41eeb0408847b28474f362f5642635e']
#         req_serv.return_value = ['ObjectDetection']
#         self.planner.all_services_worker_pool = self.all_services_worker_pool
#         ret = self.planner.create_buffer_stream_plan(bufferstream_entity)
#         self.assertTrue(updated_res.called)
#         self.assertTrue(w_sort.called)
#         self.assertTrue(w_filter.called)
#         self.assertTrue(w_init.called)
#         self.assertTrue(event_count.called)
#         self.assertTrue(req_serv.called)

#     def test_initialize_service_workers_planned_capacity(self):
#         self.planner.adaptation_delta = 10
#         self.planner.all_services_worker_pool = self.all_services_worker_pool
#         worker_pool = self.planner.initialize_service_workers_planned_capacity(
#             self.planner.all_services_worker_pool['ObjectDetection'])
#         self.assertDictEqual(worker_pool, self.planner.all_services_worker_pool['ObjectDetection'])
#         self.assertEqual(
#             worker_pool['object-detection-ssd-data']['resources']['planned']['events_capacity'],
#             150)
#         self.assertEqual(
#             worker_pool['object-detection-ssd-gpu-data']['resources']['planned']['events_capacity'],
#             296)

#     def test_update_workers_planned_resources_when_capacity_is_present(self):
#         required_events = 101
#         required_services = ['ObjectDetection']
#         buffer_stream_plan = [['object-detection-ssd-data']]

#         self.planner.all_services_worker_pool = self.all_services_worker_pool
#         self.worker_a['resources']['planned']['events_capacity'] = 200

#         self.planner.update_workers_planned_resources(required_services, buffer_stream_plan, required_events)
#         self.assertEqual(
#             self.worker_a['resources']['planned']['events_capacity'],
#             99)

#     def test_update_workers_planned_resources_when_capacity_is_not_present(self):
#         required_events = 101
#         required_services = ['ObjectDetection']
#         buffer_stream_plan = [['object-detection-ssd-data']]

#         self.planner.all_services_worker_pool = self.all_services_worker_pool
#         self.worker_a['resources']['planned'].pop('events_capacity', None)

#         self.planner.update_workers_planned_resources(required_services, buffer_stream_plan, required_events)
#         self.assertEqual(
#             self.worker_a['resources']['planned']['events_capacity'],
#             -101)

#     def test_update_workers_planned_resources_when_more_than_one_required_service(self):
#         required_events = 10
#         required_services = ['ObjectDetection', 'ObjectDetection']
#         buffer_stream_plan = [['object-detection-ssd-data'], ['object-detection-ssd-gpu-data']]

#         self.planner.all_services_worker_pool = self.all_services_worker_pool
#         self.worker_a['resources']['planned']['events_capacity'] = 200
#         self.worker_b['resources']['planned']['events_capacity'] = 5

#         self.planner.update_workers_planned_resources(required_services, buffer_stream_plan, required_events)
#         self.assertEqual(
#             self.worker_a['resources']['planned']['events_capacity'],
#             190)

#         self.assertEqual(
#             self.worker_b['resources']['planned']['events_capacity'],
#             -5)

#     def test_filter_overloaded_service_worker_pool_or_all_if_empty_with_at_least_one(self):
#         self.planner.all_services_worker_pool = self.all_services_worker_pool
#         self.worker_a['resources']['planned']['events_capacity'] = 10
#         self.worker_b['resources']['planned']['events_capacity'] = -1

#         worker_pool = self.planner.filter_overloaded_service_worker_pool_or_all_if_empty(
#             self.planner.all_services_worker_pool['ObjectDetection'])
#         self.assertIn('object-detection-ssd-data', worker_pool.keys())
#         self.assertNotIn('object-detection-ssd-gpu-data', worker_pool.keys())

#     def test_filter_overloaded_service_worker_pool_or_all_if_empty_with_none(self):
#         self.planner.all_services_worker_pool = self.all_services_worker_pool
#         self.worker_a['resources']['planned']['events_capacity'] = 0
#         self.worker_b['resources']['planned']['events_capacity'] = -1

#         worker_pool = self.planner.filter_overloaded_service_worker_pool_or_all_if_empty(
#             self.planner.all_services_worker_pool['ObjectDetection'])
#         self.assertIn('object-detection-ssd-data', worker_pool.keys())
#         self.assertIn('object-detection-ssd-gpu-data', worker_pool.keys())


# class TestWeightedRandomQoSSinglePolicySchedulerPlanner(TestCase):

#     def setUp(self):
#         scheduler_cmd_stream_key = 'sc-cmd'
#         ce_endpoint_stream_key = 'wm-data'
#         parent_service = MagicMock()
#         self.planner = WeightedRandomQoSSinglePolicySchedulerPlanner(
#             parent_service, scheduler_cmd_stream_key, ce_endpoint_stream_key)
#         self.all_queries_dict = {
#             '3940d2cad2926150093a9a786163ee14': {
#                 'action': 'updateControlFlow',
#                 'data_flow': ['ObjectDetection', 'ColorDetection'],
#                 'publisher_id': 'publisher1',
#                 'qos_policies': {
#                     'accuracy': 'min'
#                 },
#                 'query_id': '3940d2cad2926150093a9a786163ee14',
#                 'type': 'http://gnosis-mep.org/subscriber_query'
#             }
#         }
#         self.all_buffer_streams = {
#             'b41eeb0408847b28474f362f5642635e': {
#                 'action': 'startPreprocessing',
#                 'buffer_stream_key': 'b41eeb0408847b28474f362f5642635e',
#                 'fps': '30',
#                 'publisher_id': 'publisher1',
#                 'queries': self.all_queries_dict,
#                 'resolution': '640x480',
#                 'source': 'rtmp://172.17.0.1/vod2/cars.mp4',
#                 'type': 'http://gnosis-mep.org/buffer_stream'
#             }
#         }

#         self.worker_s1a = {
#             'monitoring': {
#                 'accuracy': '0.6',
#                 'energy_consumption': '100.0',
#                 'queue_size': 2,
#                 'queue_limit': 100,
#                 'queue_space': 100,
#                 'queue_space_percent': 1.0,
#                 'service_type': 'ObjectDetection',
#                 'stream_key': 'object-detection-ssd-data',
#                 'throughput': '15',
#                 'type': 'http://gnosis-mep.org/service_worker'
#             },
#             'resources': {
#                 'planned': {
#                 }
#             }
#         }
#         self.worker_s1b = {
#             'monitoring': {
#                 'accuracy': '0.9',
#                 'energy_consumption': '150.0',
#                 'queue_size': 5,
#                 'queue_limit': 100,
#                 'queue_space': 100,
#                 'queue_space_percent': 1.0,
#                 'service_type': 'ObjectDetection',
#                 'stream_key': 'object-detection-ssd-gpu-data',
#                 'throughput': '30',
#                 'type': 'http://gnosis-mep.org/service_worker'
#             },
#             'resources': {
#                 'planned': {
#                 }
#             }
#         }
#         self.worker_s2a = {
#             'monitoring': {
#                 'accuracy': '0.8',
#                 'energy_consumption': '50.0',
#                 'queue_size': 10,
#                 'service_type': 'ColorDetection',
#                 'stream_key': 'color-detection-data',
#                 'throughput': '15',
#                 'type': 'http://gnosis-mep.org/service_worker'
#             },
#             'resources': {
#                 'planned': {
#                 },
#             }
#         }

#         self.worker_s2b = {
#             'monitoring': {
#                 'accuracy': '0.7',
#                 'energy_consumption': '30.0',
#                 'queue_size': 10,
#                 'service_type': 'ColorDetection',
#                 'stream_key': 'color-detection2-data',
#                 'throughput': '60',
#                 'type': 'http://gnosis-mep.org/service_worker'
#             },
#             'resources': {
#                 'planned': {
#                 },
#             }
#         }

#         self.all_services_worker_pool = {
#             'ObjectDetection': {
#                 'object-detection-ssd-data': self.worker_s1a,
#                 'object-detection-ssd-gpu-data': self.worker_s1b,
#             },
#             'ColorDetection': {
#                 'color-detection-data': self.worker_s2a,
#                 'color-detection2-data': self.worker_s2b,
#             }
#         }

#     def test_get_worker_congestion_impact_rate_when_events_cap_greater_than_0(self):
#         self.worker_s1a['resources']['planned']['events_capacity'] = 2
#         planned_event_count = 10
#         ret = self.planner.get_worker_congestion_impact_rate(self.worker_s1a, planned_event_count)
#         self.assertAlmostEqual(ret, 0.3)

#     def test_get_worker_congestion_impact_rate_when_events_cap_is_0(self):
#         self.worker_s1a['resources']['planned']['events_capacity'] = 0
#         planned_event_count = 10
#         ret = self.planner.get_worker_congestion_impact_rate(self.worker_s1a, planned_event_count)
#         self.assertAlmostEqual(ret, 0.1)

#     def test_get_worker_congestion_impact_rate_when_events_cap_is_negative(self):
#         self.worker_s1a['resources']['planned']['events_capacity'] = -2
#         planned_event_count = 10
#         ret = self.planner.get_worker_congestion_impact_rate(self.worker_s1a, planned_event_count)
#         self.assertAlmostEqual(ret, 0.05)

#     def test_get_worker_congestion_impact_rate_shouldnt_be_more_than_one(self):
#         self.worker_s1a['resources']['planned']['events_capacity'] = 20
#         planned_event_count = 10
#         ret = self.planner.get_worker_congestion_impact_rate(self.worker_s1a, planned_event_count)
#         self.assertEqual(ret, 1)

#     @patch('adaptation_planner.planners.qos_based_scheduling.WeightedRandomQoSSinglePolicySchedulerPlanner.get_worker_congestion_impact_rate')
#     def test_get_worker_choice_weight_for_qos_policy_accuracy_max_when_no_congestion(self, get_con_rate):
#         qos_policy_name = 'accuracy'
#         qos_policy_value = 'max'
#         planned_event_count = 10
#         get_con_rate.return_value = 1
#         self.worker_s1a['monitoring'][qos_policy_name] = 0.9

#         ret = self.planner.get_worker_choice_weight_for_qos_policy(
#             self.worker_s1a, planned_event_count, qos_policy_name, qos_policy_value)
#         self.assertAlmostEqual(ret, 0.9)

#     @patch('adaptation_planner.planners.qos_based_scheduling.WeightedRandomQoSSinglePolicySchedulerPlanner.get_worker_congestion_impact_rate')
#     def test_get_worker_choice_weight_for_qos_policy_accuracy_max_with_congestion(self, get_con_rate):
#         qos_policy_name = 'accuracy'
#         qos_policy_value = 'max'
#         planned_event_count = 10
#         get_con_rate.return_value = 0.25
#         self.worker_s1a['monitoring'][qos_policy_name] = 0.9

#         ret = self.planner.get_worker_choice_weight_for_qos_policy(
#             self.worker_s1a, planned_event_count, qos_policy_name, qos_policy_value)
#         self.assertAlmostEqual(ret, 0.225)

#     @patch('adaptation_planner.planners.qos_based_scheduling.WeightedRandomQoSSinglePolicySchedulerPlanner.get_worker_congestion_impact_rate')
#     def test_get_worker_choice_weight_for_qos_policy_energy_min_with_congestion(self, get_con_rate):
#         qos_policy_name = 'energy_consumption'
#         qos_policy_value = 'min'
#         planned_event_count = 10
#         get_con_rate.return_value = 0.5
#         self.worker_s1a['monitoring'][qos_policy_name] = 10

#         ret = self.planner.get_worker_choice_weight_for_qos_policy(
#             self.worker_s1a, planned_event_count, qos_policy_name, qos_policy_value)
#         self.assertAlmostEqual(ret, 0.05)

#     @patch('adaptation_planner.planners.qos_based_scheduling.WeightedRandomQoSSinglePolicySchedulerPlanner.get_worker_congestion_impact_rate')
#     def test_get_worker_choice_weight_for_qos_policy_energy_min_with_congestion_even_worst_energy(self, get_con_rate):
#         qos_policy_name = 'energy_consumption'
#         qos_policy_value = 'min'
#         planned_event_count = 10
#         get_con_rate.return_value = 0.5
#         self.worker_s1a['monitoring'][qos_policy_name] = 20

#         ret = self.planner.get_worker_choice_weight_for_qos_policy(
#             self.worker_s1a, planned_event_count, qos_policy_name, qos_policy_value)
#         self.assertAlmostEqual(ret, 0.025)

#     @patch('adaptation_planner.planners.qos_based_scheduling.WeightedRandomQoSSinglePolicySchedulerPlanner.get_worker_congestion_impact_rate')
#     def test_get_worker_choice_weight_for_qos_policy_latency_min_with_congestion(self, get_con_rate):
#         qos_policy_name = 'latency'
#         qos_policy_value = 'min'
#         planned_event_count = 10
#         get_con_rate.return_value = 0.5
#         self.worker_s1a['monitoring']['throughput'] = 10

#         ret = self.planner.get_worker_choice_weight_for_qos_policy(
#             self.worker_s1a, planned_event_count, qos_policy_name, qos_policy_value)
#         self.assertAlmostEqual(ret, 5.0)

#     @patch('adaptation_planner.planners.qos_based_scheduling.WeightedRandomQoSSinglePolicySchedulerPlanner.get_worker_congestion_impact_rate')
#     def test_get_worker_choice_weight_for_qos_policy_latency_min_with_congestion_even_better_throughput(self, get_con_rate):
#         qos_policy_name = 'latency'
#         qos_policy_value = 'min'
#         planned_event_count = 10
#         get_con_rate.return_value = 0.5
#         self.worker_s1a['monitoring']['throughput'] = 15

#         ret = self.planner.get_worker_choice_weight_for_qos_policy(
#             self.worker_s1a, planned_event_count, qos_policy_name, qos_policy_value)
#         self.assertAlmostEqual(ret, 7.5)

#     def test_get_dataflow_choice_min_weight(self):
#         service_worker_w_tuple1 = [10, 'ObjectDetection', 'object-detection-data']
#         service_worker_w_tuple2 = [5, 'ColorDetection', 'color-detection-data2']
#         service_worker_w_tuple3 = [15, 'RainDetection', 'rain-detection-data3']
#         dataflow_choice = [service_worker_w_tuple1, service_worker_w_tuple2, service_worker_w_tuple3]
#         ret = self.planner.get_dataflow_choice_min_weight(dataflow_choice)

#         self.assertEqual(ret, 5)

#     def test_create_dataflow_choices_with_cum_weights_and_relative_weights(self):
#         service_worker_w_tuple_a1 = [10, 'ObjectDetection', 'object-detection-data']
#         service_worker_w_tuple_a2 = [5, 'ColorDetection', 'color-detection-data']
#         service_worker_w_tuple_a3 = [15, 'RainDetection', 'rain-detection-data']

#         service_worker_w_tuple_b1 = [9, 'ObjectDetection', 'object-detection-data2']
#         service_worker_w_tuple_b2 = [8, 'ColorDetection', 'color-detection-data2']
#         service_worker_w_tuple_b3 = [13, 'RainDetection', 'rain-detection-data2']

#         per_service_worker_keys_with_weights = {
#             'ObjectDetection': [service_worker_w_tuple_a1, service_worker_w_tuple_b1],
#             'ColorDetection': [service_worker_w_tuple_a2, service_worker_w_tuple_b2],
#             'RainDetection': [service_worker_w_tuple_a3, service_worker_w_tuple_b3],

#         }
#         ret = self.planner.create_dataflow_choices_with_cum_weights_and_relative_weights(
#             per_service_worker_keys_with_weights
#         )
#         dataflow_choices = ret[0]
#         dataflow_relative_weights = ret[1]
#         expected_ret = [(5.0,
#                          ([10, 'ObjectDetection', 'object-detection-data'],
#                           [5, 'ColorDetection', 'color-detection-data'],
#                              [15, 'RainDetection', 'rain-detection-data'])),
#                         (10.0,
#                          ([10, 'ObjectDetection', 'object-detection-data'],
#                           [5, 'ColorDetection', 'color-detection-data'],
#                              [13, 'RainDetection', 'rain-detection-data2'])),
#                         (18.0,
#                          ([10, 'ObjectDetection', 'object-detection-data'],
#                           [8, 'ColorDetection', 'color-detection-data2'],
#                              [15, 'RainDetection', 'rain-detection-data'])),
#                         (26.0,
#                          ([10, 'ObjectDetection', 'object-detection-data'],
#                           [8, 'ColorDetection', 'color-detection-data2'],
#                              [13, 'RainDetection', 'rain-detection-data2'])),
#                         (31.0,
#                          ([9, 'ObjectDetection', 'object-detection-data2'],
#                           [5, 'ColorDetection', 'color-detection-data'],
#                              [15, 'RainDetection', 'rain-detection-data'])),
#                         (36.0,
#                          ([9, 'ObjectDetection', 'object-detection-data2'],
#                           [5, 'ColorDetection', 'color-detection-data'],
#                              [13, 'RainDetection', 'rain-detection-data2'])),
#                         (44.0,
#                          ([9, 'ObjectDetection', 'object-detection-data2'],
#                           [8, 'ColorDetection', 'color-detection-data2'],
#                              [15, 'RainDetection', 'rain-detection-data'])),
#                         (52.0,
#                          ([9, 'ObjectDetection', 'object-detection-data2'],
#                           [8, 'ColorDetection', 'color-detection-data2'],
#                              [13, 'RainDetection', 'rain-detection-data2']))
#                         ]

#         self.assertListEqual(dataflow_choices, expected_ret)
#         self.assertListEqual(dataflow_relative_weights, [5, 5, 8, 8, 5, 5, 8, 8])

#     def test_create_cartesian_product_dataflow_choices(self):
#         service_worker_w_tuple_a1 = [1, 'ObjectDetection', 'object-detection-data']
#         service_worker_w_tuple_a2 = [2, 'ColorDetection', 'color-detection-data']
#         service_worker_w_tuple_a3 = [3, 'RainDetection', 'rain-detection-data']

#         service_worker_w_tuple_b1 = [4, 'ObjectDetection', 'object-detection-data2']
#         service_worker_w_tuple_b2 = [5, 'ColorDetection', 'color-detection-data2']
#         service_worker_w_tuple_b3 = [6, 'RainDetection', 'rain-detection-data2']

#         per_service_worker_keys_with_weights = {
#             'ObjectDetection': [service_worker_w_tuple_a1, service_worker_w_tuple_b1],
#             'ColorDetection': [service_worker_w_tuple_a2, service_worker_w_tuple_b2],
#             'RainDetection': [service_worker_w_tuple_a3, service_worker_w_tuple_b3],

#         }
#         ret = self.planner.create_cartesian_product_dataflow_choices(per_service_worker_keys_with_weights)
#         expected = [
#             ([1, 'ObjectDetection', 'object-detection-data'],
#              [2, 'ColorDetection', 'color-detection-data'],
#              [3, 'RainDetection', 'rain-detection-data']),
#             ([1, 'ObjectDetection', 'object-detection-data'],
#              [2, 'ColorDetection', 'color-detection-data'],
#              [6, 'RainDetection', 'rain-detection-data2']),
#             ([1, 'ObjectDetection', 'object-detection-data'],
#              [5, 'ColorDetection', 'color-detection-data2'],
#              [3, 'RainDetection', 'rain-detection-data']),
#             ([1, 'ObjectDetection', 'object-detection-data'],
#              [5, 'ColorDetection', 'color-detection-data2'],
#              [6, 'RainDetection', 'rain-detection-data2']),
#             ([4, 'ObjectDetection', 'object-detection-data2'],
#              [2, 'ColorDetection', 'color-detection-data'],
#              [3, 'RainDetection', 'rain-detection-data']),
#             ([4, 'ObjectDetection', 'object-detection-data2'],
#              [2, 'ColorDetection', 'color-detection-data'],
#              [6, 'RainDetection', 'rain-detection-data2']),
#             ([4, 'ObjectDetection', 'object-detection-data2'],
#              [5, 'ColorDetection', 'color-detection-data2'],
#              [3, 'RainDetection', 'rain-detection-data']),
#             ([4, 'ObjectDetection', 'object-detection-data2'],
#              [5, 'ColorDetection', 'color-detection-data2'],
#              [6, 'RainDetection', 'rain-detection-data2'])
#         ]

#         self.assertEqual(len(ret), 8)
#         self.assertListEqual(ret, expected)

#     def test_create_dataflow_choices_with_cum_weights(self):
#         dataflow_choices = [
#             ([1, 'ObjectDetection', 'object-detection-data'],
#              [2, 'ColorDetection', 'color-detection-data'],
#              [3, 'RainDetection', 'rain-detection-data']),
#             ([1, 'ObjectDetection', 'object-detection-data'],
#              [2, 'ColorDetection', 'color-detection-data'],
#              [6, 'RainDetection', 'rain-detection-data2']),
#             ([1, 'ObjectDetection', 'object-detection-data'],
#              [5, 'ColorDetection', 'color-detection-data2'],
#              [3, 'RainDetection', 'rain-detection-data']),
#             ([1, 'ObjectDetection', 'object-detection-data'],
#              [5, 'ColorDetection', 'color-detection-data2'],
#              [6, 'RainDetection', 'rain-detection-data2']),
#             ([4, 'ObjectDetection', 'object-detection-data2'],
#              [2, 'ColorDetection', 'color-detection-data'],
#              [3, 'RainDetection', 'rain-detection-data']),
#             ([4, 'ObjectDetection', 'object-detection-data2'],
#              [2, 'ColorDetection', 'color-detection-data'],
#              [6, 'RainDetection', 'rain-detection-data2']),
#             ([4, 'ObjectDetection', 'object-detection-data2'],
#              [5, 'ColorDetection', 'color-detection-data2'],
#              [3, 'RainDetection', 'rain-detection-data']),
#             ([4, 'ObjectDetection', 'object-detection-data2'],
#              [5, 'ColorDetection', 'color-detection-data2'],
#              [6, 'RainDetection', 'rain-detection-data2'])
#         ]
#         dataflow_relative_weights = [1, 1, 1, 1, 2, 2, 3, 4]
#         ret = self.planner.create_dataflow_choices_with_cum_weights(
#             dataflow_choices, dataflow_relative_weights
#         )
#         expected = [
#             (1.0,
#              ([1, 'ObjectDetection', 'object-detection-data'],
#               [2, 'ColorDetection', 'color-detection-data'],
#                  [3, 'RainDetection', 'rain-detection-data'])),
#             (2.0,
#              ([1, 'ObjectDetection', 'object-detection-data'],
#               [2, 'ColorDetection', 'color-detection-data'],
#               [6, 'RainDetection', 'rain-detection-data2'])),
#             (3.0,
#              ([1, 'ObjectDetection', 'object-detection-data'],
#               [5, 'ColorDetection', 'color-detection-data2'],
#               [3, 'RainDetection', 'rain-detection-data'])),
#             (4.0,
#              ([1, 'ObjectDetection', 'object-detection-data'],
#               [5, 'ColorDetection', 'color-detection-data2'],
#               [6, 'RainDetection', 'rain-detection-data2'])),
#             (6.0,
#              ([4, 'ObjectDetection', 'object-detection-data2'],
#               [2, 'ColorDetection', 'color-detection-data'],
#               [3, 'RainDetection', 'rain-detection-data'])),
#             (8.0,
#              ([4, 'ObjectDetection', 'object-detection-data2'],
#               [2, 'ColorDetection', 'color-detection-data'],
#               [6, 'RainDetection', 'rain-detection-data2'])),
#             (11.0,
#              ([4, 'ObjectDetection', 'object-detection-data2'],
#               [5, 'ColorDetection', 'color-detection-data2'],
#               [3, 'RainDetection', 'rain-detection-data'])),
#             (15.0,
#              ([4, 'ObjectDetection', 'object-detection-data2'],
#               [5, 'ColorDetection', 'color-detection-data2'],
#               [6, 'RainDetection', 'rain-detection-data2']))
#         ]
#         self.assertEqual(len(ret), 8)
#         self.assertListEqual(ret, expected)

#     def test_update_workers_planned_resources(self):
#         self.all_services_worker_pool = {}
#         self.all_services_worker_pool['ObjectDetection'] = {
#             'object-detection-data': {
#                 'resources': {
#                     'planned': {
#                         'events_capacity': 100,
#                     }
#                 }
#             },
#             'object-detection-data2': {
#                 'resources': {
#                     'planned': {
#                         'events_capacity': 200,
#                     }
#                 }
#             }
#         }
#         self.all_services_worker_pool['ColorDetection'] = {
#             'color-detection-data': {
#                 'resources': {
#                     'planned': {
#                         'events_capacity': 300,
#                     }
#                 }
#             }
#         }

#         dataflow_choices_with_cum_weights = [
#             (25.0,
#              ([25, 'ObjectDetection', 'object-detection-data'],
#               [85, 'ColorDetection', 'color-detection-data'])),
#             (100.0,
#              ([75, 'ObjectDetection', 'object-detection-data2'],
#               [85, 'ColorDetection', 'color-detection-data'],)),
#         ]
#         dataflow_choices_weights = [25, 75]
#         planned_event_count = 100
#         self.planner.all_services_worker_pool = self.all_services_worker_pool

#         self.planner.update_workers_planned_resources(
#             dataflow_choices_with_cum_weights, dataflow_choices_weights, planned_event_count
#         )

#         expected_obj1_detection_events_cap = 75
#         expected_obj2_detection_events_cap = 125
#         expected_color_detection_events_cap = 200
#         obj1_detection_worker = self.planner.all_services_worker_pool['ObjectDetection']['object-detection-data']
#         obj2_detection_worker = self.planner.all_services_worker_pool['ObjectDetection']['object-detection-data2']
#         color_detection_worker = self.planner.all_services_worker_pool['ColorDetection']['color-detection-data']

#         self.assertAlmostEqual(
#             obj1_detection_worker['resources']['planned']['events_capacity'],
#             expected_obj1_detection_events_cap
#         )
#         self.assertAlmostEqual(
#             obj2_detection_worker['resources']['planned']['events_capacity'],
#             expected_obj2_detection_events_cap
#         )

#         self.assertAlmostEqual(
#             color_detection_worker['resources']['planned']['events_capacity'],
#             expected_color_detection_events_cap
#         )

#     def test_create_filtered_and_weighted_workers_pool(self):
#         self.all_services_worker_pool = {}
#         self.all_services_worker_pool['ObjectDetection'] = {
#             'object-detection-data': {
#                 'monitoring': {
#                     'accuracy': '0.9',
#                 },
#                 'resources': {
#                     'planned': {
#                         'events_capacity': 0,
#                     }
#                 }
#             },
#             'object-detection-data2': {
#                 'monitoring': {
#                     'accuracy': '0.6',
#                 },
#                 'resources': {
#                     'planned': {
#                         'events_capacity': 50,
#                     }
#                 }
#             }
#         }
#         self.all_services_worker_pool['ColorDetection'] = {
#             'color-detection-data': {
#                 'monitoring': {
#                     'accuracy': '0.3',
#                 },
#                 'resources': {
#                     'planned': {
#                         'events_capacity': 100,
#                     }
#                 }
#             }
#         }
#         self.planner.all_services_worker_pool = self.all_services_worker_pool
#         qos_policy_name = 'accuracy'
#         qos_policy_value = 'max'
#         required_services = ['ObjectDetection', 'ColorDetection']
#         planned_event_count = 100
#         ret = self.planner.create_filtered_and_weighted_workers_pool(
#             required_services, planned_event_count, qos_policy_name, qos_policy_value
#         )
#         self.assertIn('ObjectDetection', ret.keys())
#         self.assertListEqual([(0.306, 'ObjectDetection', 'object-detection-data2')], ret['ObjectDetection'])
#         self.assertIn('ColorDetection', ret.keys())
#         self.assertEqual([(0.3, 'ColorDetection', 'color-detection-data')], ret['ColorDetection'])

#     @patch('adaptation_planner.planners.qos_based_scheduling.WeightedRandomQoSSinglePolicySchedulerPlanner.initialize_service_workers_planned_capacity')
#     def test_create_filtered_and_weighted_workers_pool_initialize_worker_capacity(self, init_cap):
#         self.all_services_worker_pool = {}
#         self.all_services_worker_pool['ObjectDetection'] = {
#             'object-detection-data': {
#                 'monitoring': {
#                     'accuracy': '0.9',
#                     'throughtput': '10',
#                 },
#                 'resources': {
#                     'planned': {
#                     }
#                 }
#             },
#             'object-detection-data2': {
#                 'monitoring': {
#                     'accuracy': '0.6',
#                     'throughtput': '20',
#                 },
#                 'resources': {
#                     'planned': {
#                     }
#                 }
#             }
#         }
#         self.all_services_worker_pool['ColorDetection'] = {
#             'color-detection-data': {
#                 'monitoring': {
#                     'accuracy': '0.3',
#                     'throughtput': '30',
#                 },
#                 'resources': {
#                     'planned': {
#                     }
#                 }
#             }
#         }
#         self.planner.all_services_worker_pool = self.all_services_worker_pool
#         qos_policy_name = 'accuracy'
#         qos_policy_value = 'max'
#         required_services = ['ObjectDetection', 'ColorDetection']
#         planned_event_count = 100
#         ret = self.planner.create_filtered_and_weighted_workers_pool(
#             required_services, planned_event_count, qos_policy_name, qos_policy_value
#         )
#         self.assertTrue(init_cap.called)

#     def test_format_dataflow_choices_to_buffer_stream_choices_plan(self):
#         dataflow_choices = [
#             (25.0,
#              ([25, 'ObjectDetection', 'object-detection-data'],
#               [85, 'ColorDetection', 'color-detection-data'])),
#             (100.0,
#              ([75, 'ObjectDetection', 'object-detection-data2'],
#               [85, 'ColorDetection', 'color-detection-data'],)),
#         ]
#         ret = self.planner.format_dataflow_choices_to_buffer_stream_choices_plan(dataflow_choices)

#         expected = [
#             (25.0, [['object-detection-data'], ['color-detection-data'], ['wm-data']]),
#             (100.0, [['object-detection-data2'], ['color-detection-data'], ['wm-data']])
#         ]

#         self.assertListEqual(ret, expected)
#         pass

#     @patch('adaptation_planner.planners.qos_based_scheduling.WeightedRandomQoSSinglePolicySchedulerPlanner.get_buffer_stream_required_services')
#     @patch('adaptation_planner.planners.qos_based_scheduling.WeightedRandomQoSSinglePolicySchedulerPlanner.get_bufferstream_planned_event_count')
#     @patch('adaptation_planner.planners.qos_based_scheduling.WeightedRandomQoSSinglePolicySchedulerPlanner.create_filtered_and_weighted_workers_pool')
#     @patch('adaptation_planner.planners.qos_based_scheduling.WeightedRandomQoSSinglePolicySchedulerPlanner.create_dataflow_choices_with_cum_weights_and_relative_weights')
#     @patch('adaptation_planner.planners.qos_based_scheduling.WeightedRandomQoSSinglePolicySchedulerPlanner.update_workers_planned_resources')
#     @patch('adaptation_planner.planners.qos_based_scheduling.WeightedRandomQoSSinglePolicySchedulerPlanner.format_dataflow_choices_to_buffer_stream_choices_plan')
#     def test_create_buffer_stream_choices_plan(self, format_df, updated_res, create_choices, filtered, get_event_count, get_req_serv):

#         self.planner.all_buffer_streams = self.all_buffer_streams
#         self.planner.all_queries_dict = self.all_queries_dict
#         self.planner.all_services_worker_pool = self.all_services_worker_pool
#         bufferstream_entity = self.all_buffer_streams['b41eeb0408847b28474f362f5642635e']

#         get_req_serv.return_value = ['ObjectDetection', 'ColorDetection']
#         get_event_count.return_value = 10
#         filtered.return_value = 'mocked_per_service_worker_keys_with_weights'
#         create_choices.return_value = ['dataflow_choices', 'weights']
#         format_df.return_value = 'mocked_buffer_stream_choices_plan'

#         ret = self.planner.create_buffer_stream_choices_plan(bufferstream_entity)
#         self.assertEqual(ret, 'mocked_buffer_stream_choices_plan')

#         self.assertTrue(get_event_count.called)
#         self.assertTrue(get_req_serv.called)

#         self.assertTrue(filtered.called)
#         filtered.assert_called_once_with(
#             ['ObjectDetection', 'ColorDetection'], 10, 'accuracy', 'min')

#         self.assertTrue(create_choices.called)
#         create_choices.assert_called_once_with('mocked_per_service_worker_keys_with_weights')

#         self.assertTrue(updated_res.called)
#         updated_res.assert_called_once_with('dataflow_choices', 'weights', 10)

#         self.assertTrue(format_df.called)
#         format_df.assert_called_once_with('dataflow_choices')
