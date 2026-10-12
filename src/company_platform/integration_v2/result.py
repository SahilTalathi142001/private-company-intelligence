from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any

@dataclass
class ComponentResult:
    component_id: str
    status: str
    output_artifacts: list[str] = field(default_factory=list)
    row_count: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    dq_check_count: int = 0
    dq_failed_count: int = 0
    dq_fatal_count: int = 0
    dq_warning_count: int = 0
    dq_message: str = ''
