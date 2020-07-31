import os

from decouple import config

SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SOURCE_DIR)

REDIS_ADDRESS = config('REDIS_ADDRESS', default='localhost')
REDIS_PORT = config('REDIS_PORT', default='6379')

TRACER_REPORTING_HOST = config('TRACER_REPORTING_HOST', default='localhost')
TRACER_REPORTING_PORT = config('TRACER_REPORTING_PORT', default='6831')

MOCKED_OD_STREAM_KEY = config('MOCKED_OD_STREAM_KEY', default='object-detection-data')
MOCKED_WORKERS_ENERGY_USAGE_DICT = config('MOCKED_WORKERS_ENERGY_USAGE_DICT', cast=lambda v: dict([
                                          (m.split('/')[0], float(m.split('/')[1])) for m in v.split(',')]),
                                          default="object-detection-ssd-gpu-data/163.8,object-detection-ssd-data/188.0")

SCHEDULER_PLANNER_TYPE = config('SCHEDULER_PLANNER_TYPE', default='single_best')

SERVICE_STREAM_KEY = config('SERVICE_STREAM_KEY')
SERVICE_CMD_KEY = config('SERVICE_CMD_KEY')

LOGGING_LEVEL = config('LOGGING_LEVEL', default='DEBUG')
