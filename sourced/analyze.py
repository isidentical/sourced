from __future__ import annotations

import glob
from collections.abc import Iterator
from concurrent.futures import Future, ProcessPoolExecutor, as_completed, wait
from dataclasses import dataclass, field
from itertools import islice
from typing import Callable, TypeVar

try:
    from itertools import batched
except ImportError:
    # https://docs.python.org/3.12/library/itertools.html#itertools.batched
    def batched(iterable, n):
        if n < 1:
            raise ValueError("n must be at least one")
        it = iter(iterable)
        while batch := list(islice(it, n)):
            yield batch


from rich.console import Console
from rich.progress import Progress, track

from sourced._internal.parallelization import workers
from sourced.dataset.db import GlobalStore

# Number of sources per scan task
_SCAN_TASK_BATCH_SIZE = 64


def scan_projects(projects: list[str]) -> list[str]:
    return [
        file
        for project in projects
        for file in glob.iglob(f"{project}/**/*.py", recursive=True)
    ]


ReturnType = TypeVar("ReturnType")


@dataclass
class Sourced:
    num_processes: int = workers(heavy="both")
    store: GlobalStore = field(default_factory=GlobalStore.from_file)
    console: Console = field(default_factory=Console)

    def run_on(
        self,
        dataset_name: str,
        analysis_func: Callable[[str], ReturnType],
    ) -> Iterator[ReturnType]:
        dataset = self.store.datasets[dataset_name]
        sources = [
            # Representing all the sources as pathlib objects are
            # really slow. We represent only the main path as a
            # pathlib object and the rest as strings.
            str(source.path)
            for source in dataset.sources
            if source.path is not None
        ]

        self.console.print("Found", len(sources), "sources")
        all_files = []
        with ProcessPoolExecutor(max_workers=self.num_processes) as executor:
            scan_futures = [
                executor.submit(scan_projects, batch)
                for batch in batched(sources, n=_SCAN_TASK_BATCH_SIZE)
            ]

            for future in track(
                as_completed(scan_futures),
                console=self.console,
                transient=True,
                description="Scanning python files :eyes:",
                total=len(scan_futures),
            ):
                all_files.extend(future.result())

            self.console.print(
                f"Collected {len(all_files)} files from {len(sources)} unique projects."
            )

            with Progress(transient=True, console=self.console) as progress:
                file_tracker = progress.add_task("Files", total=len(all_files))

                running_futures: set[Future[ReturnType]] = set()
                left_files = iter(all_files)
                max_task_buffer = self.num_processes * 64

                while True:
                    files = list(
                        islice(left_files, max_task_buffer - len(running_futures))
                    )
                    if not files:
                        break

                    running_futures.update(
                        executor.submit(analysis_func, file) for file in files
                    )
                    completed_futures, running_futures = wait(
                        running_futures, timeout=0.25
                    )
                    progress.update(file_tracker, advance=len(completed_futures))

                    for future in completed_futures:
                        yield future.result()
