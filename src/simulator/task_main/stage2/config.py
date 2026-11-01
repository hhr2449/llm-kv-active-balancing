from dataclasses import dataclass

from ..stage0.config import CapacityStudyConfig


@dataclass(frozen=True)
class Stage2Config(CapacityStudyConfig):
    stage2_version: str = 'ACTIVE_CAPACITY_STAGE2_V1'
    routing_schedule: str = 'CONSTANT'

    def __post_init__(self):
        super().__post_init__()
        if self.stage2_version != 'ACTIVE_CAPACITY_STAGE2_V1':
            raise ValueError('invalid Stage 2 identity')
        if self.routing_schedule not in {'CONSTANT', 'C', 'D'}:
            raise ValueError('unsupported routing schedule')
        if self.routing_schedule == 'CONSTANT':
            if self.routing_policy != 'R_AFF' or self.proactive_policy not in {'FUTURE_DEMAND', 'PERSISTENCE', 'RECENCY'}:
                raise ValueError('Stage 2A requires AFF and one frozen active policy')
        elif self.proactive_policy != 'NONE' or self.routing_policy != self.route_at(0):
            raise ValueError('Stage 2B requires the declared initial route and no proactive policy')
        if (self.action, self.future_window_ms, self.history_window_ms, self.shortlist_k,
                self.recency_quantile, self.recency_decay_ms, self.visibility_end_ms) != (
                'COPY', 300000, 300000, 10, .9, 60000, 3537000):
            raise ValueError('Stage 2 parameters are frozen')
        if self.experiment_kind == 'capacity_study':
            if self.num_pods != 16 or self.capacity_pages not in {585, 1170, 2340, None}:
                raise ValueError('invalid formal capacity matrix')
            if self.routing_schedule != 'CONSTANT' and self.capacity_pages != 2340:
                raise ValueError('ablation uses C_ref=2340')

    def route_at(self, external_arrival_ms):
        after = external_arrival_ms >= 1500000
        if self.routing_schedule == 'C':
            return 'R_AFF' if after else 'R_REQ_KV_TASK'
        if self.routing_schedule == 'D':
            return 'R_REQ_KV_TASK' if after else 'R_AFF'
        return self.routing_policy
