from .base import BaseSchedulerPlanner

class QQoS_TK_LP_LS_SchedulerPlanner(BaseSchedulerPlanner):

    def __init__(self, parent_service, ce_endpoint_stream_key):
        super(QQoS_TK_LP_LS_SchedulerPlanner, self).__init__(parent_service, ce_endpoint_stream_key)
        self.strategy_name = 'QQoS-TK-LP-LS'
    pass
