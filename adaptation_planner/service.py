import threading

from event_service_utils.logging.decorators import timer_logger
from event_service_utils.services.event_driven import BaseEventDrivenCMDService
from event_service_utils.tracing.jaeger import init_tracer

from adaptation_planner.conf import (
    LISTEN_EVENT_TYPE_QUERY_CREATED,
    LISTEN_EVENT_TYPE_SERVICE_WORKERS_STREAM_MONITORED,
    LISTEN_EVENT_TYPE_SERVICE_SLR_PROFILES_RANKED,
    LISTEN_EVENT_TYPE_NEW_QUERY_SCHEDULING_PLAN_REQUESTED,
    LISTEN_EVENT_TYPE_SERVICE_WORKER_OVERLOADED_PLAN_REQUESTED,
    LISTEN_EVENT_TYPE_SERVICE_WORKER_SLR_PROFILE_CHANGE_PLAN_REQUESTED,
    LISTEN_EVENT_TYPE_SERVICE_WORKER_BEST_IDLE_REQUESTED,
    LISTEN_EVENT_TYPE_UNNECESSARY_LOAD_SHEDDING_REQUESTED,
    PUB_EVENT_TYPE_NEW_QUERY_SCHEDULING_PLANNED,
    PUB_EVENT_TYPE_SERVICE_WORKER_SLR_PROFILE_PLANNED,
    PUB_EVENT_TYPE_SERVICE_WORKER_OVERLOADED_PLANNED,
    PUB_EVENT_TYPE_SERVICE_WORKER_BEST_IDLE_PLANNED,
    PUB_EVENT_TYPE_UNNECESSARY_LOAD_SHEDDING_PLANNED,
)

from adaptation_planner.planners.event_driven.baselines import (
    RandomSchedulerPlanner,
    RoundRobinSchedulerPlanner
)
from adaptation_planner.planners.event_driven.qqos_based import (
    QQoS_W_HP_SchedulerPlanner,
    QQoS_TK_LP_SchedulerPlanner
)
from adaptation_planner.planners.event_driven.load_shedding import (
    QQoS_W_HP_LS_SchedulerPlanner,
    QQoS_TK_LP_LS_SchedulerPlanner
)


class AdaptationPlanner(BaseEventDrivenCMDService):

    def __init__(self,
                 service_stream_key, service_cmd_key_list,
                 pub_event_list, service_details,
                 stream_factory,
                 scheduler_planner_type,
                 logging_level,
                 tracer_configs):
        tracer = init_tracer(self.__class__.__name__, **tracer_configs)
        super(AdaptationPlanner, self).__init__(
            name=self.__class__.__name__,
            service_stream_key=service_stream_key,
            service_cmd_key_list=service_cmd_key_list,
            pub_event_list=pub_event_list,
            service_details=service_details,
            stream_factory=stream_factory,
            logging_level=logging_level,
            tracer=tracer,
        )
        self.cmd_validation_fields = ['id']
        self.data_validation_fields = ['id']

        self.latest_plan = {}
        self.current_plan = None
        self.queued_plan = None

        self.ce_endpoint_stream_key = 'wm-data'
        self.scheduler_planner_type = scheduler_planner_type
        self.available_scheduler_planners = {}
        self.scheduler_planner = None
        self.setup_scheduler_planner()
        self.all_services_worker_pool = {}
        self.all_buffer_streams = {}
        self.all_queries = {}
        self.slr_profiles_by_service = {}

        self.request_type_to_plan_map = {
            LISTEN_EVENT_TYPE_NEW_QUERY_SCHEDULING_PLAN_REQUESTED: PUB_EVENT_TYPE_NEW_QUERY_SCHEDULING_PLANNED,
            LISTEN_EVENT_TYPE_SERVICE_WORKER_SLR_PROFILE_CHANGE_PLAN_REQUESTED: PUB_EVENT_TYPE_SERVICE_WORKER_SLR_PROFILE_PLANNED,
            LISTEN_EVENT_TYPE_SERVICE_WORKER_OVERLOADED_PLAN_REQUESTED: PUB_EVENT_TYPE_SERVICE_WORKER_OVERLOADED_PLANNED,
            LISTEN_EVENT_TYPE_SERVICE_WORKER_BEST_IDLE_REQUESTED: PUB_EVENT_TYPE_SERVICE_WORKER_BEST_IDLE_PLANNED,
            LISTEN_EVENT_TYPE_UNNECESSARY_LOAD_SHEDDING_REQUESTED: PUB_EVENT_TYPE_UNNECESSARY_LOAD_SHEDDING_PLANNED,
        }

    def setup_scheduler_planner(self):
        self.available_scheduler_planners = {
            'random': RandomSchedulerPlanner(
                self, self.ce_endpoint_stream_key,
            ),
            'round_robin': RoundRobinSchedulerPlanner(
                self, self.ce_endpoint_stream_key,
            ),
            'QQoS-W-HP': QQoS_W_HP_SchedulerPlanner(
                self, self.ce_endpoint_stream_key
            ),
            'QQoS-W-HP-LS': QQoS_W_HP_LS_SchedulerPlanner(
                self, self.ce_endpoint_stream_key
            ),
            'QQoS-TK-LP': QQoS_TK_LP_SchedulerPlanner(
                self, self.ce_endpoint_stream_key
            ),
            'QQoS-TK-LP-LS': QQoS_TK_LP_LS_SchedulerPlanner(
                self, self.ce_endpoint_stream_key
            ),
        }

        self.scheduler_planner = self.available_scheduler_planners[self.scheduler_planner_type]

    def publish_adaptation_plan(self, event_type, new_plan):
        new_event_data = {
            'id': self.service_based_random_event_id(),
            'plan': new_plan
        }
        self.latest_plan = new_plan
        self.publish_event_type_to_stream(
            event_type=event_type, new_event_data=new_event_data
        )
        self.current_plan = None

    def update_bufferstreams_from_new_query(self, new_query):
        query_bufferstream_dict = new_query.get('buffer_stream', None)
        buffer_stream_key = query_bufferstream_dict.get('buffer_stream_key', None)
        if query_bufferstream_dict is None or buffer_stream_key is None:
            raise RuntimeError(f'Missing Bufferstream data on new query: {new_query}')

        bufferstream_dict = self.all_buffer_streams.setdefault(buffer_stream_key, {})
        bufferstream_dict[new_query['query_id']] = new_query

    def process_query_created(self, event_data):
        query_id = event_data['query_id']
        new_query = {}
        for k, v in event_data.items():
            if k != 'tracer':
                new_query[k] = v
        self.all_queries[query_id] = new_query
        self.update_bufferstreams_from_new_query(new_query)

    def initialize_plan(self, change_request):
        request_type = change_request['type']
        plan_type = self.request_type_to_plan_map[request_type]
        new_plan = {
            'type': plan_type,
            'execution_plan': None,
            'change_request': change_request
        }
        self.current_plan = new_plan
        return new_plan

    def process_plan_requested(self, event_data):
        change_request = event_data['change']
        new_plan = self.scheduler_planner.plan(change_request=change_request)
        event_type = new_plan['type']
        self.publish_adaptation_plan(event_type, new_plan=new_plan)

    def process_service_workers_monitored(self, event_data):
        service_workers = event_data['service_workers']
        self.all_services_worker_pool = service_workers

    def process_service_slr_profile_ranked(self, event_data):
        service_type = event_data['service_type']
        slr_profiles = event_data['slr_profiles']
        self.slr_profiles_by_service[service_type] = slr_profiles

    def process_event_type(self, event_type, event_data, json_msg):
        if not super(AdaptationPlanner, self).process_event_type(event_type, event_data, json_msg):
            return False

        if event_type == LISTEN_EVENT_TYPE_QUERY_CREATED:
            self.process_query_created(event_data)
        elif event_type == LISTEN_EVENT_TYPE_SERVICE_WORKERS_STREAM_MONITORED:
            self.process_service_workers_monitored(event_data)
        elif event_type == LISTEN_EVENT_TYPE_SERVICE_SLR_PROFILES_RANKED:
            self.process_service_slr_profile_ranked(event_data)
        elif event_type in self.request_type_to_plan_map.keys():
            self.process_plan_requested(event_data)

    def get_destination_streams(self, destination):
        return self.stream_factory.create(destination, stype='streamOnly')

    def log_state(self):
        super(AdaptationPlanner, self).log_state()
        # self.logger.info(f'Last execution_plan: {self.latest_plan.get("execution_plan", {})}')
        self._log_dict('All queries', self.all_queries)
        self._log_dict('All Service workers', self.all_services_worker_pool)
        self._log_dict('All SLR profiles', self.slr_profiles_by_service)
        self._log_dict('All Bufferstreams', self.all_buffer_streams)
        self._log_dict('Last plan executed:', self.latest_plan)
        self.logger.debug(f'- Scheduler Planner: {self.scheduler_planner}')

    def run(self):
        super(AdaptationPlanner, self).run()
        self.log_state()
        self.cmd_thread = threading.Thread(target=self.run_forever, args=(self.process_cmd,))
        self.cmd_planning_thread = threading.Thread(
            target=self.run_forever, args=(self.process_cmd,), kwargs={'cg_sub_group': 'planning'})
        self.cmd_thread.start()
        self.cmd_planning_thread.start()
        self.cmd_thread.join()
        self.cmd_planning_thread.join()
