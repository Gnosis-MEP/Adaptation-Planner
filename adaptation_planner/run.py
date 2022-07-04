#!/usr/bin/env python
from event_service_utils.streams.redis import RedisStreamFactory

from adaptation_planner.service import AdaptationPlanner

from adaptation_planner.conf import (
    REDIS_ADDRESS,
    REDIS_PORT,
    PUB_EVENT_LIST,
    SERVICE_STREAM_KEY,
    SERVICE_CMD_KEY_LIST,
    LOGGING_LEVEL,
    TRACER_REPORTING_HOST,
    TRACER_REPORTING_PORT,
    SERVICE_DETAILS,
    SCHEDULER_PLANNER_TYPE,
)


def run_service():
    tracer_configs = {
        'reporting_host': TRACER_REPORTING_HOST,
        'reporting_port': TRACER_REPORTING_PORT,
    }
    stream_factory = RedisStreamFactory(host=REDIS_ADDRESS, port=REDIS_PORT)
    service = AdaptationPlanner(
        service_stream_key=SERVICE_STREAM_KEY,
        service_cmd_key_list=SERVICE_CMD_KEY_LIST,
        pub_event_list=PUB_EVENT_LIST,
        service_details=SERVICE_DETAILS,
        stream_factory=stream_factory,
        scheduler_planner_type=SCHEDULER_PLANNER_TYPE,
        logging_level=LOGGING_LEVEL,
        tracer_configs=tracer_configs
    )
    service.run()


def main():
    try:
        run_service()
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
