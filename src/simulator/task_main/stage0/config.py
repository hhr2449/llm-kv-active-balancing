from dataclasses import asdict, dataclass, fields
from pathlib import Path
import yaml

from ..config import TaskMainConfig, STAGE_C_VERSION


@dataclass(frozen=True)
class CapacityStudyConfig(TaskMainConfig):
    """Separate study identity; no relaxation of canonical formal/D1/D2 matrices."""
    num_pods: int = 16
    capacity_pages: int | None = 585
    capacity_mode: str = 'finite'
    study_version: str = 'TASK_MAIN_CAPACITY_STAGE0_V1'
    protocol_version: str = STAGE_C_VERSION

    def __post_init__(self):
        if self.study_version != 'TASK_MAIN_CAPACITY_STAGE0_V1':
            raise ValueError('invalid capacity study version')
        if self.experiment_kind not in {'synthetic', 'capacity_study'}:
            raise ValueError('capacity studies cannot reuse canonical formal run identity')
        if self.capacity_mode not in {'finite', 'infinite'}:
            raise ValueError('capacity_mode must be finite/infinite')
        if self.capacity_mode == 'infinite' and self.capacity_pages is not None:
            raise ValueError('infinite mode requires explicit null capacity_pages')
        if self.capacity_mode == 'finite' and (type(self.capacity_pages) is not int or self.capacity_pages < 0):
            raise ValueError('finite capacity must be a nonnegative integer')
        if self.routing_policy not in {'R_AFF', 'R_LEAST', 'R_REQ_KV_TASK'}:
            raise ValueError('unsupported routing policy')
        if self.routing_policy == 'R_LEAST' and self.proactive_policy != 'NONE':
            raise ValueError('R_LEAST is a routing-only baseline')
        raw = {f.name:getattr(self,f.name) for f in fields(TaskMainConfig)}
        raw['experiment_kind'] = 'synthetic'
        raw['capacity_pages'] = 0 if self.capacity_pages is None else self.capacity_pages
        raw['routing_policy'] = 'R_AFF' if self.routing_policy == 'R_LEAST' else self.routing_policy
        TaskMainConfig(**raw)

    @classmethod
    def from_yaml(cls, path):
        raw = yaml.safe_load(Path(path).read_text())
        if not isinstance(raw, dict) or raw.get('study_version') != 'TASK_MAIN_CAPACITY_STAGE0_V1':
            raise ValueError('explicit Stage 0 study version required')
        return cls(**raw)
