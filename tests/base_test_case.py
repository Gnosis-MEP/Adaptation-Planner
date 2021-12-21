from unittest import TestCase
from unittest.mock import MagicMock


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
