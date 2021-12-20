from .base import BaseSchedulerPlanner


class TKSchedulerPlanner(BaseSchedulerPlanner):
    def __init__(self, parent_service, ce_endpoint_stream_key):
        super(TKSchedulerPlanner, self).__init__(parent_service, ce_endpoint_stream_key)
        self.strategy_name = 'TK'