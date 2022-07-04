from unittest.mock import patch, MagicMock

from base_test_case import BaseSchedulerPlannerTestCase
from adaptation_planner.planners.event_driven.load_shedding import (
    QQoS_W_HP_LS_SchedulerPlanner,
    QQoS_TK_LP_LS_SchedulerPlanner
)

class TestQQoS_TK_LP_LS_SchedulerPlanner(BaseSchedulerPlannerTestCase):

    def initialize_planner(self):
        self.planner = QQoS_TK_LP_LS_SchedulerPlanner(
            self.parent_service, self.ce_endpoint_stream_key)

    def test_update_workers_planned_resources_returns_load_shedding_with_single_worker(self):
        required_events = 100
        required_services = ['ObjectDetection']
        buffer_stream_plan = [['object-detection-1']]

        self.planner.all_services_worker_pool = self.all_services_worker_pool
        self.worker_a['resources']['planned']['events_capacity'] = 90
        service_type = self.worker_a['monitoring']['service_type']
        self.planner.required_services_workload_status = {
            service_type: {'is_overloaded': True}
        }

        load_shedding_rate = self.planner.update_workers_planned_resources(
            required_services, buffer_stream_plan, required_events)
        self.assertAlmostEqual(load_shedding_rate, 0.1)

    def test_update_workers_planned_resources_returns_load_shedding_with_shouldnt_load_shed_if_not_overloaded(self):
        required_events = 100
        required_services = ['ObjectDetection']
        buffer_stream_plan = [['object-detection-1']]

        self.planner.all_services_worker_pool = self.all_services_worker_pool
        self.worker_a['resources']['planned']['events_capacity'] = 90
        service_type = self.worker_a['monitoring']['service_type']
        self.planner.required_services_workload_status = {
            service_type: {'is_overloaded': False}
        }

        load_shedding_rate = self.planner.update_workers_planned_resources(
            required_services, buffer_stream_plan, required_events)
        self.assertEqual(load_shedding_rate, 0)

    def test_update_workers_planned_resources_returns_load_shedding_with_multiple_service(self):
        required_events = 100
        required_services = ['ObjectDetection', 'ColorDetection']
        buffer_stream_plan = [['object-detection-1'], ['color-detection']]
        service_type = self.worker_a['monitoring']['service_type']
        self.planner.required_services_workload_status = {
            service_type: {'is_overloaded': True}
        }

        self.planner.all_services_worker_pool = self.all_services_worker_pool
        self.worker_a['resources']['planned']['events_capacity'] = 50
        self.worker_c['resources']['planned']['events_capacity'] = 90

        load_shedding_rate = self.planner.update_workers_planned_resources(
            required_services, buffer_stream_plan, required_events)
        self.assertAlmostEqual(load_shedding_rate, 0.5)

    @patch('adaptation_planner.planners.event_driven.load_shedding.QQoS_TK_LP_LS_SchedulerPlanner.get_buffer_stream_required_services')
    @patch('adaptation_planner.planners.event_driven.load_shedding.QQoS_TK_LP_LS_SchedulerPlanner.get_bufferstream_planned_event_count')
    @patch('adaptation_planner.planners.event_driven.load_shedding.QQoS_TK_LP_LS_SchedulerPlanner.get_init_workers_filter_based_on_qos_policy')
    @patch('adaptation_planner.planners.event_driven.load_shedding.QQoS_TK_LP_LS_SchedulerPlanner.workers_key_sorted_by_qos')
    @patch('adaptation_planner.planners.event_driven.load_shedding.QQoS_TK_LP_LS_SchedulerPlanner.update_workers_planned_resources')
    def test_create_buffer_stream_plan_calls_proper_methods_and_returns_load_shedding_zero_for_not_latency(self, updated_res, w_sort, w_init, event_count, req_serv):
        self.all_queries['48321bfc85870426a28298662d458b10']['parsed_query']['qos_policies'] = {
            'energy_consumption': 'min'
        }
        bufferstream_entity = self.all_buffer_streams['f79681aaa510938aca8c60506171d9d8']
        req_serv.return_value = ['ObjectDetection']
        load_shedding = 1
        updated_res.return_value = load_shedding
        self.planner.all_services_worker_pool = self.all_services_worker_pool
        ret = self.planner.create_buffer_stream_plan(bufferstream_entity)
        self.assertTrue(updated_res.called)
        self.assertTrue(w_sort.called)
        self.assertTrue(w_init.called)
        self.assertTrue(event_count.called)
        self.assertTrue(req_serv.called)
        self.assertEquals(len(ret), 2)
        self.assertEquals(ret[1], 0)

    @patch('adaptation_planner.planners.event_driven.load_shedding.QQoS_TK_LP_LS_SchedulerPlanner.get_buffer_stream_required_services')
    @patch('adaptation_planner.planners.event_driven.load_shedding.QQoS_TK_LP_LS_SchedulerPlanner.get_bufferstream_planned_event_count')
    @patch('adaptation_planner.planners.event_driven.load_shedding.QQoS_TK_LP_LS_SchedulerPlanner.get_init_workers_filter_based_on_qos_policy')
    @patch('adaptation_planner.planners.event_driven.load_shedding.QQoS_TK_LP_LS_SchedulerPlanner.workers_key_sorted_by_qos')
    @patch('adaptation_planner.planners.event_driven.load_shedding.QQoS_TK_LP_LS_SchedulerPlanner.update_workers_planned_resources')
    def test_create_buffer_stream_plan_calls_proper_methods_and_returns_load_shedding_for_latency(self, updated_res, w_sort, w_init, event_count, req_serv):
        self.all_queries['48321bfc85870426a28298662d458b10']['parsed_query']['qos_policies'] = {
            'latency': 'min'
        }
        bufferstream_entity = self.all_buffer_streams['f79681aaa510938aca8c60506171d9d8']
        req_serv.return_value = ['ObjectDetection']
        load_shedding = 0.5
        updated_res.return_value = load_shedding
        self.planner.all_services_worker_pool = self.all_services_worker_pool
        ret = self.planner.create_buffer_stream_plan(bufferstream_entity)
        self.assertTrue(updated_res.called)
        self.assertTrue(w_sort.called)
        self.assertTrue(w_init.called)
        self.assertTrue(event_count.called)
        self.assertTrue(req_serv.called)
        self.assertEquals(len(ret), 2)
        self.assertEquals(ret[1], load_shedding)

    @patch('adaptation_planner.planners.event_driven.load_shedding.QQoS_TK_LP_LS_SchedulerPlanner.create_buffer_stream_plan')
    def test_create_buffer_stream_choices_plan_creates_plan_choices_with_load_shedding(self, create_bsp):
        self.planner.all_buffer_streams = self.all_buffer_streams
        self.planner.all_queries = self.all_queries
        self.planner.all_services_worker_pool = self.all_services_worker_pool
        bufferstream_entity = self.all_buffer_streams['f79681aaa510938aca8c60506171d9d8']

        create_bsp.return_value = ['plan', 'load_shedding']
        ret = self.planner.create_buffer_stream_choices_plan(bufferstream_entity)
        self.assertEqual(len(ret), 1)
        best_plan = ret[0]
        self.assertEqual(len(best_plan), 3)
        self.assertEqual(best_plan[0], 'load_shedding')
        self.assertEqual(best_plan[1], None)
        self.assertEqual(best_plan[2], 'plan')

    def test_get_bufferstream_required_loadshedding_rate_for_worker_when_overloaded(self):
        self.planner.adaptation_delta = 10
        self.planner.all_services_worker_pool = self.all_services_worker_pool
        service_type = self.worker_a['monitoring']['service_type']
        self.planner.required_services_workload_status = {
            service_type: {'is_overloaded': True}
        }
        required_events = 200
        self.worker_a['resources']['planned']['events_capacity'] = -50
        load_shedding_rate = self.planner.get_bufferstream_required_loadshedding_rate_for_worker(
            self.worker_a, required_events)
        self.assertAlmostEqual(load_shedding_rate, 0.25)

    def test_get_bufferstream_required_loadshedding_rate_for_worker_when_underloaded(self):
        self.planner.adaptation_delta = 10
        self.planner.all_services_worker_pool = self.all_services_worker_pool
        service_type = self.worker_a['monitoring']['service_type']
        self.planner.required_services_workload_status = {
            service_type: {'is_overloaded': True}
        }

        required_events = 145
        self.worker_a['resources']['planned']['events_capacity'] = 0
        load_shedding_rate = self.planner.get_bufferstream_required_loadshedding_rate_for_worker(
            self.worker_a, required_events)
        self.assertAlmostEqual(load_shedding_rate, 0)

    def test_get_bufferstream_required_loadshedding_rate_for_worker_when_extremely_overloaded(self):
        self.planner.adaptation_delta = 10
        self.planner.all_services_worker_pool = self.all_services_worker_pool

        service_type = self.worker_a['monitoring']['service_type']
        self.planner.required_services_workload_status = {
            service_type: {'is_overloaded': True}
        }

        required_events = 200
        self.worker_a['resources']['planned']['events_capacity'] = -600  # wont happent, but in any case
        load_shedding_rate = self.planner.get_bufferstream_required_loadshedding_rate_for_worker(
            self.worker_a, required_events)
        self.assertAlmostEqual(load_shedding_rate, 1)

    def test_get_bufferstream_required_loadshedding_rate_for_worker_when_extremely_overloaded_but_not_system_overloaded(self):
        self.planner.adaptation_delta = 10
        self.planner.all_services_worker_pool = self.all_services_worker_pool
        service_type = self.worker_a['monitoring']['service_type']
        self.planner.required_services_workload_status = {
            service_type: {'is_overloaded': False}
        }

        required_events = 200
        self.worker_a['resources']['planned']['events_capacity'] = -600  # wont happent, but in any case
        load_shedding_rate = self.planner.get_bufferstream_required_loadshedding_rate_for_worker(
            self.worker_a, required_events)
        self.assertEqual(load_shedding_rate, 0)

class TestQQoS_W_HP_LS_SchedulerPlanner(BaseSchedulerPlannerTestCase):

    def initialize_planner(self):
        self.planner = QQoS_W_HP_LS_SchedulerPlanner(
            self.parent_service, self.ce_endpoint_stream_key)

    def test_update_workers_planned_resources(self):

        self.planner.all_services_worker_pool = self.all_services_worker_pool
        self.worker_a['resources']['planned']['events_capacity'] = 100
        self.worker_b['resources']['planned']['events_capacity'] = 200
        self.worker_c['resources']['planned']['events_capacity'] = 300

        self.planner.required_services_workload_status = {
            'ObjectDetection': {'is_overloaded': True},
            'ColorDetection': {'is_overloaded': True},
        }
        dataflow_choices_with_cum_weights = [
            (25.0,
             ([25, 'ObjectDetection', 'object-detection-1'],
              [85, 'ColorDetection', 'color-detection'])),
            (100.0,
             ([75, 'ObjectDetection', 'object-detection-2'],
              [85, 'ColorDetection', 'color-detection'],)),
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
             ([25, 'ObjectDetection', 'object-detection-1'],
              [85, 'ColorDetection', 'color-detection'])),
            (100.0,
             ([75, 'ObjectDetection', 'object-detection-2'],
              [85, 'ColorDetection', 'color-detection'],)),
        ]
        load_shedding_rate_choices = [0.00990099, 0.343234323]
        ret = self.planner.format_dataflow_choices_to_buffer_stream_choices_plan(
            dataflow_choices, load_shedding_rate_choices)

        expected = [
            (0.00990099, 25.0, [['object-detection-1'], ['color-detection'], ['wm-data']]),
            (0.343234323, 100.0, [['object-detection-2'], ['color-detection'], ['wm-data']])
        ]

        self.assertListEqual(ret, expected)

    @patch('adaptation_planner.planners.event_driven.load_shedding.QQoS_W_HP_LS_SchedulerPlanner.get_buffer_stream_required_services')
    @patch('adaptation_planner.planners.event_driven.load_shedding.QQoS_W_HP_LS_SchedulerPlanner.get_bufferstream_planned_event_count')
    @patch('adaptation_planner.planners.event_driven.load_shedding.QQoS_W_HP_LS_SchedulerPlanner.create_filtered_and_weighted_workers_pool')
    @patch('adaptation_planner.planners.event_driven.load_shedding.QQoS_W_HP_LS_SchedulerPlanner.create_dataflow_choices_with_cum_weights_and_relative_weights')
    @patch('adaptation_planner.planners.event_driven.load_shedding.QQoS_W_HP_LS_SchedulerPlanner.update_workers_planned_resources')
    @patch('adaptation_planner.planners.event_driven.load_shedding.QQoS_W_HP_LS_SchedulerPlanner.format_dataflow_choices_to_buffer_stream_choices_plan')
    def test_create_buffer_stream_choices_plan_when_latency_min_qos(self, format_df, updated_res, create_choices, filtered, get_event_count, get_req_serv):
        self.all_queries['48321bfc85870426a28298662d458b10']['parsed_query']['qos_policies'] = {
            'latency': 'min'
        }

        self.planner.all_buffer_streams = self.all_buffer_streams
        self.planner.all_queries = self.all_queries
        self.planner.all_services_worker_pool = self.all_services_worker_pool
        bufferstream_entity = self.all_buffer_streams['f79681aaa510938aca8c60506171d9d8']

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


    @patch('adaptation_planner.planners.event_driven.load_shedding.QQoS_W_HP_LS_SchedulerPlanner.get_buffer_stream_required_services')
    @patch('adaptation_planner.planners.event_driven.load_shedding.QQoS_W_HP_LS_SchedulerPlanner.get_bufferstream_planned_event_count')
    @patch('adaptation_planner.planners.event_driven.load_shedding.QQoS_W_HP_LS_SchedulerPlanner.create_filtered_and_weighted_workers_pool')
    @patch('adaptation_planner.planners.event_driven.load_shedding.QQoS_W_HP_LS_SchedulerPlanner.create_dataflow_choices_with_cum_weights_and_relative_weights')
    @patch('adaptation_planner.planners.event_driven.load_shedding.QQoS_W_HP_LS_SchedulerPlanner.update_workers_planned_resources')
    @patch('adaptation_planner.planners.event_driven.load_shedding.QQoS_W_HP_LS_SchedulerPlanner.format_dataflow_choices_to_buffer_stream_choices_plan')
    def test_create_buffer_stream_choices_plan_when_non_latency_min_qos(self, format_df, updated_res, create_choices, filtered, get_event_count, get_req_serv):
        self.all_queries['48321bfc85870426a28298662d458b10']['parsed_query']['qos_policies'] = {
            'accuracy': 'max'
        }

        self.planner.all_buffer_streams = self.all_buffer_streams
        self.planner.all_queries = self.all_queries
        self.planner.all_services_worker_pool = self.all_services_worker_pool
        bufferstream_entity = self.all_buffer_streams['f79681aaa510938aca8c60506171d9d8']

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



# class TestSingleBestForQoSSinglePolicyLSSchedulerPlanner(TestCase):


#     def test_get_bufferstream_required_loadshedding_rate_for_worker_when_overloaded(self):
#         self.planner.adaptation_delta = 10
#         self.planner.all_services_worker_pool = self.all_services_worker_pool
#         service_type = self.worker_a['monitoring']['service_type']
#         self.planner.required_services_workload_status = {
#             service_type: {'is_overloaded': True}
#         }
#         required_events = 200
#         self.worker_a['resources']['planned']['events_capacity'] = -50
#         load_shedding_rate = self.planner.get_bufferstream_required_loadshedding_rate_for_worker(
#             self.worker_a, required_events)
#         self.assertAlmostEqual(load_shedding_rate, 0.25)

#     def test_get_bufferstream_required_loadshedding_rate_for_worker_when_underloaded(self):
#         self.planner.adaptation_delta = 10
#         self.planner.all_services_worker_pool = self.all_services_worker_pool
#         service_type = self.worker_a['monitoring']['service_type']
#         self.planner.required_services_workload_status = {
#             service_type: {'is_overloaded': True}
#         }

#         required_events = 145
#         self.worker_a['resources']['planned']['events_capacity'] = 0
#         load_shedding_rate = self.planner.get_bufferstream_required_loadshedding_rate_for_worker(
#             self.worker_a, required_events)
#         self.assertAlmostEqual(load_shedding_rate, 0)

#     def test_get_bufferstream_required_loadshedding_rate_for_worker_when_extremely_overloaded(self):
#         self.planner.adaptation_delta = 10
#         self.planner.all_services_worker_pool = self.all_services_worker_pool

#         service_type = self.worker_a['monitoring']['service_type']
#         self.planner.required_services_workload_status = {
#             service_type: {'is_overloaded': True}
#         }

#         required_events = 200
#         self.worker_a['resources']['planned']['events_capacity'] = -600  # wont happent, but in any case
#         load_shedding_rate = self.planner.get_bufferstream_required_loadshedding_rate_for_worker(
#             self.worker_a, required_events)
#         self.assertAlmostEqual(load_shedding_rate, 1)

#     def test_get_bufferstream_required_loadshedding_rate_for_worker_when_extremely_overloaded_but_not_system_overloaded(self):
#         self.planner.adaptation_delta = 10
#         self.planner.all_services_worker_pool = self.all_services_worker_pool
#         service_type = self.worker_a['monitoring']['service_type']
#         self.planner.required_services_workload_status = {
#             service_type: {'is_overloaded': False}
#         }

#         required_events = 200
#         self.worker_a['resources']['planned']['events_capacity'] = -600  # wont happent, but in any case
#         load_shedding_rate = self.planner.get_bufferstream_required_loadshedding_rate_for_worker(
#             self.worker_a, required_events)
#         self.assertEqual(load_shedding_rate, 0)

#     def test_update_workers_planned_resources_returns_load_shedding_with_single_worker(self):
#         required_events = 100
#         required_services = ['ObjectDetection']
#         buffer_stream_plan = [['object-detection-ssd-data']]

#         self.planner.all_services_worker_pool = self.all_services_worker_pool
#         self.worker_a['resources']['planned']['events_capacity'] = 90
#         service_type = self.worker_a['monitoring']['service_type']
#         self.planner.required_services_workload_status = {
#             service_type: {'is_overloaded': True}
#         }

#         load_shedding_rate = self.planner.update_workers_planned_resources(
#             required_services, buffer_stream_plan, required_events)
#         self.assertAlmostEqual(load_shedding_rate, 0.1)

#     def test_update_workers_planned_resources_returns_load_shedding_with_shouldnt_load_shed_if_not_overloaded(self):
#         required_events = 100
#         required_services = ['ObjectDetection']
#         buffer_stream_plan = [['object-detection-ssd-data']]

#         self.planner.all_services_worker_pool = self.all_services_worker_pool
#         self.worker_a['resources']['planned']['events_capacity'] = 90
#         service_type = self.worker_a['monitoring']['service_type']
#         self.planner.required_services_workload_status = {
#             service_type: {'is_overloaded': False}
#         }

#         load_shedding_rate = self.planner.update_workers_planned_resources(
#             required_services, buffer_stream_plan, required_events)
#         self.assertEqual(load_shedding_rate, 0)

#     def test_update_workers_planned_resources_returns_load_shedding_with_multiple_service(self):
#         required_events = 100
#         required_services = ['ObjectDetection', 'ObjectDetection']
#         buffer_stream_plan = [['object-detection-ssd-data'], ['object-detection-ssd-gpu-data']]
#         service_type = self.worker_a['monitoring']['service_type']
#         self.planner.required_services_workload_status = {
#             service_type: {'is_overloaded': True}
#         }

#         self.planner.all_services_worker_pool = self.all_services_worker_pool
#         self.worker_a['resources']['planned']['events_capacity'] = 50
#         self.worker_b['resources']['planned']['events_capacity'] = 90

#         load_shedding_rate = self.planner.update_workers_planned_resources(
#             required_services, buffer_stream_plan, required_events)
#         self.assertAlmostEqual(load_shedding_rate, 0.5)

#     @patch('adaptation_planner.planners.load_shedding_based_scheduling.SingleBestForQoSSinglePolicyLSSchedulerPlanner.get_buffer_stream_required_services')
#     @patch('adaptation_planner.planners.load_shedding_based_scheduling.SingleBestForQoSSinglePolicyLSSchedulerPlanner.get_bufferstream_planned_event_count')
#     @patch('adaptation_planner.planners.load_shedding_based_scheduling.SingleBestForQoSSinglePolicyLSSchedulerPlanner.initialize_service_workers_planned_capacity')
#     @patch('adaptation_planner.planners.load_shedding_based_scheduling.SingleBestForQoSSinglePolicyLSSchedulerPlanner.filter_best_than_avg_and_overloaded_service_worker_pool_or_all')
#     @patch('adaptation_planner.planners.load_shedding_based_scheduling.SingleBestForQoSSinglePolicyLSSchedulerPlanner.workers_key_sorted_by_qos')
#     @patch('adaptation_planner.planners.load_shedding_based_scheduling.SingleBestForQoSSinglePolicyLSSchedulerPlanner.update_workers_planned_resources')
#     def test_create_buffer_stream_plan_calls_proper_methods_and_returns_load_shedding_zero_for_not_latency(self, updated_res, w_sort, w_filter, w_init, event_count, req_serv):
#         self.all_queries_dict['3940d2cad2926150093a9a786163ee14']['qos_policies'] = {
#             'energy_consumption': 'min'
#         }
#         bufferstream_entity = self.all_buffer_streams['b41eeb0408847b28474f362f5642635e']
#         req_serv.return_value = ['ObjectDetection']
#         load_shedding = 1
#         updated_res.return_value = load_shedding
#         self.planner.all_services_worker_pool = self.all_services_worker_pool
#         ret = self.planner.create_buffer_stream_plan(bufferstream_entity)
#         self.assertTrue(updated_res.called)
#         self.assertTrue(w_sort.called)
#         self.assertTrue(w_filter.called)
#         self.assertTrue(w_init.called)
#         self.assertTrue(event_count.called)
#         self.assertTrue(req_serv.called)
#         self.assertEquals(len(ret), 2)
#         self.assertEquals(ret[1], 0)

#     @patch('adaptation_planner.planners.load_shedding_based_scheduling.SingleBestForQoSSinglePolicyLSSchedulerPlanner.get_buffer_stream_required_services')
#     @patch('adaptation_planner.planners.load_shedding_based_scheduling.SingleBestForQoSSinglePolicyLSSchedulerPlanner.get_bufferstream_planned_event_count')
#     @patch('adaptation_planner.planners.load_shedding_based_scheduling.SingleBestForQoSSinglePolicyLSSchedulerPlanner.initialize_service_workers_planned_capacity')
#     @patch('adaptation_planner.planners.load_shedding_based_scheduling.SingleBestForQoSSinglePolicyLSSchedulerPlanner.filter_overloaded_service_worker_pool_or_all_if_empty')
#     @patch('adaptation_planner.planners.load_shedding_based_scheduling.SingleBestForQoSSinglePolicyLSSchedulerPlanner.workers_key_sorted_by_qos')
#     @patch('adaptation_planner.planners.load_shedding_based_scheduling.SingleBestForQoSSinglePolicyLSSchedulerPlanner.update_workers_planned_resources')
#     def test_create_buffer_stream_plan_calls_proper_methods_and_returns_load_shedding_for_latency(self, updated_res, w_sort, w_filter, w_init, event_count, req_serv):
#         self.all_queries_dict['3940d2cad2926150093a9a786163ee14']['qos_policies'] = {
#             'latency': 'min'
#         }
#         bufferstream_entity = self.all_buffer_streams['b41eeb0408847b28474f362f5642635e']
#         req_serv.return_value = ['ObjectDetection']
#         load_shedding = 0.5
#         updated_res.return_value = load_shedding
#         self.planner.all_services_worker_pool = self.all_services_worker_pool
#         ret = self.planner.create_buffer_stream_plan(bufferstream_entity)
#         self.assertTrue(updated_res.called)
#         self.assertTrue(w_sort.called)
#         self.assertTrue(w_filter.called)
#         self.assertTrue(w_init.called)
#         self.assertTrue(event_count.called)
#         self.assertTrue(req_serv.called)
#         self.assertEquals(len(ret), 2)
#         self.assertEquals(ret[1], load_shedding)

#     @patch('adaptation_planner.planners.load_shedding_based_scheduling.SingleBestForQoSSinglePolicyLSSchedulerPlanner.create_buffer_stream_plan')
#     def test_create_buffer_stream_choices_plan_creates_plan_choices_with_load_shedding(self, create_bsp):
#         self.planner.all_buffer_streams = self.all_buffer_streams
#         self.planner.all_queries_dict = self.all_queries_dict
#         self.planner.all_services_worker_pool = self.all_services_worker_pool
#         bufferstream_entity = self.all_buffer_streams['b41eeb0408847b28474f362f5642635e']

#         create_bsp.return_value = ['plan', 'load_shedding']
#         ret = self.planner.create_buffer_stream_choices_plan(bufferstream_entity)
#         self.assertEqual(len(ret), 1)
#         best_plan = ret[0]
#         self.assertEqual(len(best_plan), 3)
#         self.assertEqual(best_plan[0], 'load_shedding')
#         self.assertEqual(best_plan[1], None)
#         self.assertEqual(best_plan[2], 'plan')



# class TestWeightedRandomQoSSinglePolicyLSSchedulerPlanner(TestCase):


#     def test_update_workers_planned_resources(self):
#         self.all_services_worker_pool = {}
#         self.all_services_worker_pool['ObjectDetection'] = {
#             'object-detection-data': {
#                 'monitoring': {
#                     'service_type': 'ObjectDetection'
#                 },
#                 'resources': {
#                     'planned': {
#                         'events_capacity': 100,
#                     }
#                 }
#             },
#             'object-detection-data2': {
#                 'monitoring': {
#                     'service_type': 'ObjectDetection'
#                 },
#                 'resources': {
#                     'planned': {
#                         'events_capacity': 200,
#                     }
#                 }
#             }
#         }
#         self.all_services_worker_pool['ColorDetection'] = {
#             'color-detection-data': {
#                 'monitoring': {
#                     'service_type': 'ColorDetection'
#                 },
#                 'resources': {
#                     'planned': {
#                         'events_capacity': 300,
#                     }
#                 }
#             }
#         }
#         self.planner.required_services_workload_status = {
#             'ObjectDetection': {'is_overloaded': True},
#             'ColorDetection': {'is_overloaded': True},
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
#         planned_event_count = 404
#         # first one is from obj
#         # second from color which is actually the worst
#         expected_load_shedding_rate_choices = [0.00990099, 0.343234323]
#         self.planner.all_services_worker_pool = self.all_services_worker_pool
#         load_shedding_rate_choices = self.planner.update_workers_planned_resources(
#             dataflow_choices_with_cum_weights, dataflow_choices_weights, planned_event_count
#         )

#         self.assertEqual(len(load_shedding_rate_choices), 2)
#         self.assertAlmostEqual(load_shedding_rate_choices[0], expected_load_shedding_rate_choices[0])
#         self.assertAlmostEqual(load_shedding_rate_choices[1], expected_load_shedding_rate_choices[1])

#     def test_format_dataflow_choices_to_buffer_stream_choices_plan(self):
#         dataflow_choices = [
#             (25.0,
#              ([25, 'ObjectDetection', 'object-detection-data'],
#               [85, 'ColorDetection', 'color-detection-data'])),
#             (100.0,
#              ([75, 'ObjectDetection', 'object-detection-data2'],
#               [85, 'ColorDetection', 'color-detection-data'],)),
#         ]
#         load_shedding_rate_choices = [0.00990099, 0.343234323]
#         ret = self.planner.format_dataflow_choices_to_buffer_stream_choices_plan(
#             dataflow_choices, load_shedding_rate_choices)

#         expected = [
#             (0.00990099, 25.0, [['object-detection-data'], ['color-detection-data'], ['wm-data']]),
#             (0.343234323, 100.0, [['object-detection-data2'], ['color-detection-data'], ['wm-data']])
#         ]

#         self.assertListEqual(ret, expected)

#     @patch('adaptation_planner.planners.load_shedding_based_scheduling.WeightedRandomQoSSinglePolicyLSSchedulerPlanner.get_buffer_stream_required_services')
#     @patch('adaptation_planner.planners.load_shedding_based_scheduling.WeightedRandomQoSSinglePolicyLSSchedulerPlanner.get_bufferstream_planned_event_count')
#     @patch('adaptation_planner.planners.load_shedding_based_scheduling.WeightedRandomQoSSinglePolicyLSSchedulerPlanner.create_filtered_and_weighted_workers_pool')
#     @patch('adaptation_planner.planners.load_shedding_based_scheduling.WeightedRandomQoSSinglePolicyLSSchedulerPlanner.create_dataflow_choices_with_cum_weights_and_relative_weights')
#     @patch('adaptation_planner.planners.load_shedding_based_scheduling.WeightedRandomQoSSinglePolicyLSSchedulerPlanner.update_workers_planned_resources')
#     @patch('adaptation_planner.planners.load_shedding_based_scheduling.WeightedRandomQoSSinglePolicyLSSchedulerPlanner.format_dataflow_choices_to_buffer_stream_choices_plan')
#     def test_create_buffer_stream_choices_plan_when_non_acc_max_qos(self, format_df, updated_res, create_choices, filtered, get_event_count, get_req_serv):
#         self.all_queries_dict['3940d2cad2926150093a9a786163ee14']['qos_policies'] = {
#             'latency': 'min'
#         }

#         self.planner.all_buffer_streams = self.all_buffer_streams
#         self.planner.all_queries_dict = self.all_queries_dict
#         self.planner.all_services_worker_pool = self.all_services_worker_pool
#         bufferstream_entity = self.all_buffer_streams['b41eeb0408847b28474f362f5642635e']

#         get_req_serv.return_value = ['ObjectDetection', 'ColorDetection']
#         get_event_count.return_value = 10
#         filtered.return_value = 'mocked_per_service_worker_keys_with_weights'
#         create_choices.return_value = ['dataflow_choices', 'weights']
#         format_df.return_value = 'mocked_buffer_stream_choices_plan'
#         mocked_load_shedding = [0.5, 0.6]
#         updated_res.return_value = mocked_load_shedding

#         ret = self.planner.create_buffer_stream_choices_plan(bufferstream_entity)
#         self.assertEqual(ret, 'mocked_buffer_stream_choices_plan')

#         self.assertTrue(get_event_count.called)
#         self.assertTrue(get_req_serv.called)

#         self.assertTrue(filtered.called)
#         filtered.assert_called_once_with(
#             ['ObjectDetection', 'ColorDetection'], 10, 'latency', 'min')

#         self.assertTrue(create_choices.called)
#         create_choices.assert_called_once_with('mocked_per_service_worker_keys_with_weights')

#         self.assertTrue(updated_res.called)
#         updated_res.assert_called_once_with('dataflow_choices', 'weights', 10)

#         self.assertTrue(format_df.called)
#         format_df.assert_called_once_with('dataflow_choices', mocked_load_shedding)

#     @patch('adaptation_planner.planners.load_shedding_based_scheduling.WeightedRandomQoSSinglePolicyLSSchedulerPlanner.get_buffer_stream_required_services')
#     @patch('adaptation_planner.planners.load_shedding_based_scheduling.WeightedRandomQoSSinglePolicyLSSchedulerPlanner.get_bufferstream_planned_event_count')
#     @patch('adaptation_planner.planners.load_shedding_based_scheduling.WeightedRandomQoSSinglePolicyLSSchedulerPlanner.create_filtered_and_weighted_workers_pool')
#     @patch('adaptation_planner.planners.load_shedding_based_scheduling.WeightedRandomQoSSinglePolicyLSSchedulerPlanner.create_dataflow_choices_with_cum_weights_and_relative_weights')
#     @patch('adaptation_planner.planners.load_shedding_based_scheduling.WeightedRandomQoSSinglePolicyLSSchedulerPlanner.update_workers_planned_resources')
#     @patch('adaptation_planner.planners.load_shedding_based_scheduling.WeightedRandomQoSSinglePolicyLSSchedulerPlanner.format_dataflow_choices_to_buffer_stream_choices_plan')
#     def test_create_buffer_stream_choices_plan_when_acc_max_qos(self, format_df, updated_res, create_choices, filtered, get_event_count, get_req_serv):
#         self.planner.all_buffer_streams = self.all_buffer_streams
#         self.planner.all_queries_dict = self.all_queries_dict
#         self.planner.all_services_worker_pool = self.all_services_worker_pool
#         bufferstream_entity = self.all_buffer_streams['b41eeb0408847b28474f362f5642635e']

#         get_req_serv.return_value = ['ObjectDetection', 'ColorDetection']
#         get_event_count.return_value = 10
#         filtered.return_value = 'mocked_per_service_worker_keys_with_weights'
#         create_choices.return_value = ['dataflow_choices', 'weights']
#         format_df.return_value = 'mocked_buffer_stream_choices_plan'
#         mocked_load_shedding = [0.5, 0.6]
#         updated_res.return_value = mocked_load_shedding

#         ret = self.planner.create_buffer_stream_choices_plan(bufferstream_entity)
#         self.assertEqual(ret, 'mocked_buffer_stream_choices_plan')

#         self.assertTrue(get_event_count.called)
#         self.assertTrue(get_req_serv.called)

#         self.assertTrue(filtered.called)
#         filtered.assert_called_once_with(
#             ['ObjectDetection', 'ColorDetection'], 10, 'accuracy', 'max')

#         self.assertTrue(create_choices.called)
#         create_choices.assert_called_once_with('mocked_per_service_worker_keys_with_weights')

#         self.assertTrue(updated_res.called)
#         updated_res.assert_called_once_with('dataflow_choices', 'weights', 10)

#         self.assertTrue(format_df.called)
#         format_df.assert_called_once_with('dataflow_choices', [0, 0])
