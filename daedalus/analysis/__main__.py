from __future__ import annotations

import glob
import importlib
import time
from argparse import ArgumentParser
from collections import Counter
from concurrent.futures import Future, ProcessPoolExecutor, as_completed, wait
from itertools import islice
from pathlib import Path

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


def scan_projects(projects: list[str]) -> list[str]:
    return [
        file
        for project in projects
        for file in glob.iglob(f"{project}/**/*.py", recursive=True)
    ]


def main():
    parser = ArgumentParser()
    parser.add_argument("pypi_path", type=Path)
    parser.add_argument("analysis_func", type=str)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--sample-size", type=int, default=None)

    options = parser.parse_args()

    console = Console()
    with console.status(":python: Loading the analyzer function"):
        analysis_package, _, analysis_func = options.analysis_func.partition(":")

        module = importlib.import_module(analysis_package)
        analysis_func = getattr(module, analysis_func)

    projects = [
        project
        for project in options.pypi_path.iterdir()
        if project.is_dir() and project.name[0] != "."
    ]
    console.print("Found", len(projects), "projects")

    if options.sample_size is not None:
        projects = projects[: options.sample_size]
        console.print("Sampling only first", len(projects), "projects")

    all_files = []
    with ProcessPoolExecutor(max_workers=options.workers) as executor:
        scan_futures = [
            executor.submit(scan_projects, batch)
            for batch in batched(projects, n=min(options.workers, 16))
        ]

        for future in track(
            as_completed(scan_futures),
            console=console,
            transient=True,
            description="Scanning python files :eyes:",
            total=len(scan_futures),
        ):
            all_files.extend(future.result())

        console.print(
            f"Collected {len(all_files)} files from {len(projects)} unique projects."
        )

        stats = Counter()
        start_time = time.perf_counter()

        def show_stats():
            spent_time = time.perf_counter() - start_time
            parsed_files, skipped_files = stats["CAN_PARSE"], stats["PARSE_FAILURE"]
            total_files = parsed_files + skipped_files

            return (
                f"{parsed_files} successfully parsed, {skipped_files} skipped.\n"
                f"Average speed: {total_files / spent_time:.2f} files per second."
            )

        with Progress(transient=True, console=console) as progress:
            file_tracker = progress.add_task("Files", total=len(all_files))
            stats_tracker = progress.add_task(show_stats(), total=None)

            running_futures: set[Future[str]] = set()
            left_files = iter(all_files)
            max_task_buffer = options.workers * 64

            while True:
                files = list(islice(left_files, max_task_buffer - len(running_futures)))
                if not files:
                    break

                running_futures.update(
                    executor.submit(analysis_func, file) for file in files
                )
                completed_futures, running_futures = wait(running_futures, timeout=0.25)
                progress.update(file_tracker, advance=len(completed_futures))

                for future in completed_futures:
                    state = future.result()
                    stats[state] += 1

                progress.update(stats_tracker, description=show_stats())

        console.print(show_stats())


if __name__ == "__main__":
    main()
