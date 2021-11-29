#!/usr/bin/env python
import uuid
import json
from event_service_utils.streams.redis import RedisStreamFactory

from adaptation_planner.conf import (
    REDIS_ADDRESS,
    REDIS_PORT,
    LISTEN_EVENT_TYPE_QUERY_CREATED,
    LISTEN_EVENT_TYPE_SERVICE_WORKERS_STREAM_MONITORED,
    LISTEN_EVENT_TYPE_NEW_QUERY_SCHEDULING_PLAN_REQUESTED,
)


def make_dict_key_bites(d):
    return {k.encode('utf-8'): v for k, v in d.items()}


def new_msg(event_data):
    event_data.update({'id': str(uuid.uuid4())})
    return {'event': json.dumps(event_data)}


def main():
    stream_factory = RedisStreamFactory(host=REDIS_ADDRESS, port=REDIS_PORT)
    query_created_cmd = stream_factory.create(LISTEN_EVENT_TYPE_QUERY_CREATED, stype='streamOnly')
    servicesmon_cmd = stream_factory.create(LISTEN_EVENT_TYPE_SERVICE_WORKERS_STREAM_MONITORED, stype='streamOnly')
    queryschplan_cmd = stream_factory.create(LISTEN_EVENT_TYPE_NEW_QUERY_SCHEDULING_PLAN_REQUESTED, stype='streamOnly')

    query_created_cmd.write_events(
        new_msg(
            {'buffer_stream': {'buffer_stream_key': 'f79681aaa510938aca8c60506171d9d8',
                               'fps': '30',
                               'publisher_id': 'pid',
                               'resolution': '300x300',
                               'source': 'psource'},
             'parsed_query': {'content': ['ObjectDetection', 'ColorDetection'],
                              'from': ['pid'],
                              'match': "MATCH (c1:Car {color:'blue'}), (c2:Car "
                                       "{color:'white'})",
                              'name': 'my_first_query',
                              'optional_match': '',
                              'qos_policies': {},
                              'ret': 'RETURN *',
                              'where': '',
                              'window': {'args': [2],
                                         'window_type': 'TUMBLING_COUNT_WINDOW'}},
             'query_id': '48321bfc85870426a28298662d458b10',
             'service_chain': ['ObjectDetection', 'ColorDetection'],
             'subscriber_id': 'sid'
             }
        )
    )

    servicesmon_cmd.write_events(
        new_msg(
            {'service_workers': {'ColorDetection': {'total_number_workers': 1,
                                                    'workers': {'clrworker-key': {'accuracy': 0.9,
                                                                                  'energy_consumption': 10,
                                                                                  'queue_limit': 100,
                                                                                  'queue_size': 0,
                                                                                  'queue_space': 100,
                                                                                  'queue_space_percent': 1.0,
                                                                                  'service_type': 'ColorDetection',
                                                                                  'stream_key': 'clrworker-key',
                                                                                  'throughput': 1}}},
                                 'ObjectDetection': {'total_number_workers': 2,
                                                     'workers': {'objworker-key': {'accuracy': 0.1,
                                                                                   'energy_consumption': 100,
                                                                                   'queue_limit': 100,
                                                                                   'queue_size': 0,
                                                                                   'queue_space': 100,
                                                                                   'queue_space_percent': 1.0,
                                                                                   'service_type': 'ObjectDetection',
                                                                                   'stream_key': 'objworker-key',
                                                                                   'throughput': 10},
                                                                 'objworker-key2': {'accuracy': 0.9,
                                                                                    'energy_consumption': 10,
                                                                                    'queue_limit': 100,
                                                                                    'queue_size': 0,
                                                                                    'queue_space': 100,
                                                                                    'queue_space_percent': 1.0,
                                                                                    'service_type': 'ObjectDetection',
                                                                                    'stream_key': 'objworker-key2',
                                                                                    'throughput': 1}}}},
             }
        )
    )
    queryschplan_cmd.write_events(
        new_msg(
            {

                'change': {
                    'type': LISTEN_EVENT_TYPE_NEW_QUERY_SCHEDULING_PLAN_REQUESTED,
                    'cause': {'change_cause': 'some dict'}
                }
            }
        )
    )
    # addworker_cmd.write_events(
    #     new_msg(
    #         {
    #             'worker': {
    #                 'service_type': 'ColorDetection',
    #                 'stream_key': 'clrworker-key',
    #                 'queue_limit': 100,
    #                 'throughput': 1,
    #                 'accuracy': 0.9,
    #                 'energy_consumption': 10,
    #             }
    #         }
    #     )
    # )

    # repeat_cmd = stream_factory.create(LISTEN_EVENT_TYPE_REPEAT_MONITOR_STREAMS_SIZE_REQUESTED, stype='streamOnly')

    # repeat_cmd.write_events(
    #     new_msg(
    #         {
    #             'repeat_after_time': 1,
    #             # 'services': {
    #             #     'ObjectDetection': {
    #             #         'workers': [
    #             #             {
    #             #                 'stream_key': 'object-detection-ssd-gpu-data',
    #             #                 'queue_limit': 100
    #             #             },
    #             #             {
    #             #                 'stream_key': 'object-detection-ssd-data',
    #             #                 'queue_limit': 100
    #             #             }
    #             #         ]
    #             #     },
    #             #     'ColorDetection': {
    #             #         'workers': [
    #             #             {
    #             #                 'stream_key': 'color-detection-data',
    #             #                 'queue_limit': 100
    #             #             }
    #             #         ]
    #             #     }
    #             # }
    #         }
    #     )
    # )

    import ipdb
    ipdb.set_trace()


if __name__ == '__main__':
    main()
