"""What a task receives: Spark, the platform config, the job run id and its parameters."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

from ..config import PlatformConfig

TRUE = ("true", "1", "yes", "y", "on")


@dataclass
class TaskContext:
    spark: Any
    cfg: PlatformConfig
    run_id: str
    params: dict[str, str] = field(default_factory=dict)

    def param(self, name: str, default: str | None = None) -> str | None:
        value = self.params.get(name)
        return default if value is None or value == "" else value

    def int_param(self, name: str, default: int) -> int:
        return int(self.param(name, str(default)))

    def float_param(self, name: str, default: float) -> float:
        return float(self.param(name, str(default)))

    def bool_param(self, name: str, default: bool = False) -> bool:
        return str(self.param(name, str(default))).strip().lower() in TRUE

    def date_param(self, name: str) -> date | None:
        value = self.param(name)
        return date.fromisoformat(value) if value else None

    def table(self, ref: str) -> str:
        return self.cfg.table_ref(ref)
