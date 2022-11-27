from __future__ import annotations

import json
import shutil
from argparse import ArgumentParser
from collections.abc import Iterator
from concurrent import futures
from contextlib import contextmanager
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from urllib.request import Request, urlopen, urlretrieve

from rich.console import Console
from rich.progress import Progress

from daedalus._internal import parallelization
from daedalus.dataset import db

BASE_PYPI_URL = "https://pypi.org"
_POPULAR_PYPI_PACKAGES_INDEX = (
    "https://hugovk.github.io/top-pypi-packages/top-pypi-packages-30-days.min.json"
)


@dataclass
class SkipError(Exception):
    project: str
    description: str | None

    @classmethod
    def from_source(cls, source: db.Source, *args, **kwargs) -> SkipError:
        source.status = db.SourceStatus.SKIPPED
        return cls(source.name, *args, **kwargs)


def collect_all_pypi_packages(console: Console) -> Iterator[str]:
    request = Request(
        BASE_PYPI_URL + "/simple",
        headers={
            "Accept": "application/vnd.pypi.simple.v1+json",
        },
    )
    with console.status("Fetching the initial PyPI index..."):
        with urlopen(request) as page:
            pypi_index = json.load(page)
            for project in pypi_index["projects"]:
                yield project["name"]


def collect_popular_pypi_packages(console: Console) -> Iterator[str]:
    with console.status("Fetching the initial PyPI index..."):
        with urlopen(_POPULAR_PYPI_PACKAGES_INDEX) as page:
            popular_packages = json.load(page)
            for project in popular_packages["rows"]:
                yield project["project"]


def prepare_download_url(source: db.Source) -> tuple[str, str]:
    request = Request(
        BASE_PYPI_URL + f"/simple/{source.name}",
        headers={
            "Accept": "application/vnd.pypi.simple.v1+json",
        },
    )
    with urlopen(request) as page:
        project_index = json.load(page)
        for file in reversed(project_index["files"]):
            # TODO: Support any source wheels
            if unpack_format := shutil._find_unpack_format(file["filename"]):
                return file["url"], unpack_format
        else:
            raise SkipError.from_source(source, "no suitable archive found")


@contextmanager
def _clear_on_failure(
    progress: Progress,
    project_name: str,
    cache_path: Path,
) -> Iterator[None]:
    download_task = progress.add_task(
        f":alarm_clock: Preparing {project_name}", total=3
    )
    try:
        yield download_task
    except BaseException:
        # Do not leave the cache in a corrupted state
        shutil.rmtree(cache_path, ignore_errors=True)
        raise
    finally:
        progress.remove_task(download_task)


def download_target(
    progress: Progress,
    base_path: Path,
    source: db.Source,
) -> None:
    source_path = base_path / source.name
    if source_path.exists():
        shutil.rmtree(source_path)
        source.status = db.SourceStatus.AWAITING_DOWNLOAD

    source_path.mkdir(parents=True)
    with _clear_on_failure(progress, source.name, source_path) as download_task:
        download_url, unpack_format = prepare_download_url(source)

        progress.update(
            download_task,
            description=f":right_arrow_curving_down:, Downloading {source.name}",
            advance=1,
        )

        total_seen = 0

        def update_download(_, block_size: int, total_size: int) -> None:
            nonlocal total_seen
            total_seen += block_size

            progress.update(
                download_task,
                description=(
                    ":right_arrow_curving_down:, Downloading"
                    f" {source.name} ({total_seen} / {total_size} bytes)"
                ),
            )

        path, _ = urlretrieve(
            download_url,
            reporthook=update_download,
        )

        progress.update(
            download_task,
            description=f":open_book: Extracting {source.name}",
            advance=1,
        )
        shutil.unpack_archive(path, source_path, format=unpack_format)

        num_files = list(source_path.iterdir())
        if len(num_files) == 1 and num_files[0].is_dir():
            num_files[0].rename(source_path / "src")
            source.status = db.SourceStatus.DOWNLOADED


def download_targets(
    console: Console,
    dataset: db.Dataset,
) -> None:
    awaiting_sources = [
        source
        for source in dataset.sources
        if source.status is db.SourceStatus.AWAITING_DOWNLOAD
    ]

    workers = parallelization.workers(heavy="io")
    with futures.ThreadPoolExecutor(max_workers=workers) as executor:
        with Progress(console=console) as progress:
            total_progress = progress.add_task(
                "Fetching PyPI targets",
                total=len(dataset.sources),
                completed=len(dataset.sources) - len(awaiting_sources),
            )
            for completed_tasks in parallelization.buffered_execution(
                executor,
                iter(awaiting_sources),
                partial(download_target, progress, dataset.path),
                max_buffered_tasks=workers * 2,
            ):
                progress.update(total_progress, advance=len(completed_tasks))
                for completed_task in completed_tasks:
                    try:
                        completed_task.result()
                    except SkipError as error:
                        console.print(
                            f"Skipping {error.project}:"
                            f" {error.description or 'unknown cause'}"
                        )

                    dataset.cache()


def main() -> None:
    parser = ArgumentParser()
    parser.add_argument("base_dir", type=Path)
    parser.add_argument("--fresh-index", action="store_true")
    parser.add_argument(
        "--all",
        action="store_true",
        help=(
            "Include all PyPI packages, not just the popular ones in the index (if it"
            " is a fresh one)."
        ),
    )

    options = parser.parse_args()
    options.base_dir.mkdir(parents=True, exist_ok=True)

    try:
        dataset = db.Dataset.from_cache(options.base_dir)
    except FileNotFoundError:
        dataset = db.Dataset("pypi", options.base_dir)
        fresh_index = True
    else:
        fresh_index = options.fresh_index

    console = Console()

    if fresh_index:
        if options.all:
            package_index = collect_all_pypi_packages(console)
        else:
            package_index = collect_popular_pypi_packages(console)

        dataset.sources = [
            db.Source(name=project_name) for project_name in package_index
        ]
        dataset.cache()
    else:
        assert not options.all, (
            "Cannot use --all with a cached index, try passing --fresh-index along"
            " with it"
        )

    download_targets(console, dataset)


if __name__ == "__main__":
    main()
