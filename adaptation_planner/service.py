import threading

from event_service_utils.logging.decorators import timer_logger
from event_service_utils.services.tracer import BaseTracerService
from event_service_utils.tracing.jaeger import init_tracer


class AdaptationPlanner(BaseTracerService):
    def __init__(self,
                 service_stream_key, service_cmd_key,
                 stream_factory,
                 logging_level,
                 tracer_configs):
        tracer = init_tracer(self.__class__.__name__, **tracer_configs)
        super(AdaptationPlanner, self).__init__(
            name=self.__class__.__name__,
            service_stream_key=service_stream_key,
            service_cmd_key=service_cmd_key,
            stream_factory=stream_factory,
            logging_level=logging_level,
            tracer=tracer,
        )
        self.cmd_validation_fields = ['id', 'action']
        self.data_validation_fields = ['id']

    @timer_logger
    def process_data_event(self, event_data, json_msg):
        if not super(AdaptationPlanner, self).process_data_event(event_data, json_msg):
            return False
        # do something here
        pass

    def process_action(self, action, event_data, json_msg):
        if not super(AdaptationPlanner, self).process_action(action, event_data, json_msg):
            return False
        if action == 'someAction':
            # do some action
            pass

    def scheduler_prepare_plan(self):
        return {
            'dataflow': {
                'buffer1': ['od-data']
            }
        }

    def get_destination_streams(self, destination):
        return self.stream_factory.create(destination, stype='streamOnly')

    def send_plan_to_scheduler(self):
        event_data = {
            'action': 'executeAdaptivePlan',
        }
        event_data.update(self.scheduler_prepare_plan())

        # another hacky hack. hardcoding the stream key for the scheduler
        self.write_event_with_trace(event_data, self.get_destination_streams('sc-cmd'))

    def log_state(self):
        super(AdaptationPlanner, self).log_state()
        self.logger.info(f'My service name is: {self.name}')

    def run(self):
        super(AdaptationPlanner, self).run()
        self.cmd_thread = threading.Thread(target=self.run_forever, args=(self.process_cmd,))
        self.data_thread = threading.Thread(target=self.run_forever, args=(self.process_data,))
        self.cmd_thread.start()
        self.data_thread.start()
        self.cmd_thread.join()
        self.data_thread.join()
