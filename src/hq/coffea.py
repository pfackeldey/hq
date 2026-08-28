from __future__ import annotations

from dataclasses import dataclass, field
from types import ModuleType
from typing import Any, Callable, Iterable, Sequence

import cloudpickle

try:
    from coffea.processor import accumulate
    from coffea.processor.executor import ExecutorBase
except ImportError as exc:  # pragma: no cover - clear optional-dep message
    raise ImportError(
        "hq.coffea requires coffea. Install coffea in this environment "
        "(e.g. conda env coffea_env) before using CoffeaHQExecutor."
    ) from exc

from hq.executor import HQExecutor


def register_modules_by_value(*modules: ModuleType) -> None:
    """Ship local analysis packages inside cloudpickle payloads (AGC Dask pattern)."""
    for mod in modules:
        cloudpickle.register_pickle_by_value(mod)


@dataclass
class CoffeaHQExecutor(ExecutorBase):
    """coffea ExecutorBase that maps work items over HQ and merges results."""

    host: str = "http://localhost"
    port: int = 3000
    verify: bool | str | None = None
    n_workers: int = 2
    queue: str | None = None
    poll_interval: float = 3.0
    manage_workers: bool = True
    pickle_modules: Sequence[ModuleType] = field(default_factory=tuple)
    # Skip coffea LZ4 wrapping for v1; tasks return plain accumulatables.
    compression: int | None = None

    def __call__(
        self,
        items: Iterable[Any],
        function: Callable[[Any], Any],
        accumulator: Any,
    ) -> tuple[Any, int]:
        if self.pickle_modules:
            register_modules_by_value(*self.pickle_modules)

        work_items = list(items)
        if len(work_items) == 0:
            return accumulator, 0

        with HQExecutor(
            host=self.host,
            port=self.port,
            verify=self.verify,
            n_workers=self.n_workers,
            queue=self.queue,
            manage_workers=self.manage_workers,
        ) as hq:
            task_ids = hq.map(function, work_items)
            results = hq.wait_and_gather(
                *task_ids, poll_interval=self.poll_interval
            )

        out = accumulate(results, accumulator)
        return out, 0
