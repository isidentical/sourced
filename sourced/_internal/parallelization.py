from __future__ import annotations

import os
from collections.abc import Iterator
from concurrent import futures
from itertools import islice
from typing import Callable, Literal, TypeVar

InputType = TypeVar("InputType")
ReturnType = TypeVar("ReturnType")


def workers(*, heavy: Literal["io", "cpu", "both"]) -> int:
    """Return the number of workers to use for the given
    heavy workload type."""
    cpu_count = os.cpu_count() or 4

    if heavy == "io":
        return cpu_count * 4
    elif heavy == "cpu":
        return round(cpu_count * 1.25)
    elif heavy == "both":
        return round(cpu_count * 2)
    else:
        raise ValueError(f"Invalid heavy workload type: {heavy!r}")


def buffered_execution(
    executor: futures.Executor,
    iterator: Iterator[InputType],
    target_func: Callable[[InputType], ReturnType],
    /,
    max_buffered_tasks: int,
    _per_check_delay: float = 0.25,
) -> Iterator[set[futures.Future[ReturnType]]]:
    """Execute the target function over the input values obtained
    from the iterator in a buffered manner. Will yield a set of
    results for each batch of tasks that have been completed on
    each iteration (_per_check_delay seconds)."""

    running_tasks: set[futures.Future[ReturnType]] = set()
    while True:
        inputs = list(islice(iterator, max_buffered_tasks - len(running_tasks)))
        if not inputs and not running_tasks:
            break

        running_tasks.update(
            executor.submit(target_func, input_value) for input_value in inputs
        )
        completed_tasks, running_tasks = futures.wait(
            running_tasks,
            timeout=_per_check_delay,
        )
        yield completed_tasks
