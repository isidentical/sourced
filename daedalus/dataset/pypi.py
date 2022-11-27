from __future__ import annotations

import json
import shutil
from argparse import ArgumentParser
from collections.abc import Iterator
from concurrent import futures
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from urllib.request import Request, urlopen, urlretrieve

from rich.console import Console
from rich.progress import Progress

from daedalus.dataset import db

BASE_PYPI_URL = "https://pypi.org"
_POPULAR_PYPI_PACKAGES_INDEX = (
    "https://hugovk.github.io/top-pypi-packages/top-pypi-packages-30-days.min.json"
)


@dataclass
class SkipError(Exception):
    project: str
    description: str | None


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


def prepare_download_url(project_name: str) -> tuple[str, str]:
    request = Request(
        BASE_PYPI_URL + f"/simple/{project_name}",
        headers={
            "Accept": "application/vnd.pypi.simple.v1+json",
        },
    )
    with urlopen(request) as page:
        project_index = json.load(page)
        for file in reversed(project_index["files"]):
            if unpack_format := shutil._find_unpack_format(file["filename"]):
                return file["url"], unpack_format
        else:
            raise SkipError(project_name, "No suitable archive found")


@contextmanager
def _clear_on_failure(
    progress: Progress,
    project_name: str,
    cache_path: Path,
) -> Iterator[None]:
    download_task = progress.add_task(
        f"Preparing {project_name} :alarm_clock:", total=None
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
    project_name: str,
    base_path: Path,
) -> Path:
    source_path = base_path / project_name
    if source_path.exists():
        shutil.rmtree(source_path)

    source_path.mkdir(parents=True)
    with _clear_on_failure(progress, project_name, source_path) as download_task:
        download_url, unpack_format = prepare_download_url(project_name)

        progress.update(
            download_task,
            description=f"Downloading {project_name} :right_arrow_curving_down:",
        )
        path, _ = urlretrieve(
            download_url,
            reporthook=lambda _, block_size, total_size: progress.update(
                download_task, total=total_size, advance=block_size
            ),
        )
        shutil.unpack_archive(path, source_path, format=unpack_format)

        [unpacked_path] = source_path.iterdir()
        unpacked_path.rename(source_path / "src")

    return source_path


def download_targets(
    console: Console,
    dataset: db.Dataset,
) -> None:
    awaiting_sources = [
        source
        for source in dataset.sources
        if source.status is db.SourceStatus.AWAITING_DOWNLOAD
    ]

    with futures.ThreadPoolExecutor(max_workers=128 * 4) as executor:
        with Progress(console=console) as progress:
            total_progress = progress.add_task(
                "Fetching PyPI targets", total=len(awaiting_sources)
            )

            download_tasks = {
                executor.submit(
                    download_target,
                    progress,
                    awaiting_source.name,
                    dataset.path,
                ): awaiting_source
                for awaiting_source in awaiting_sources
            }
            for download_task in futures.as_completed(set(download_tasks.keys())):
                progress.advance(total_progress)

                source = download_tasks.pop(download_task)
                try:
                    source.path = download_task.result()
                except SkipError as error:
                    console.print(
                        f"Skipping {error.project}:"
                        f" {error.description or 'unknown cause'}"
                    )
                    source.status = db.SourceStatus.SKIPPED
                else:
                    source.status = db.SourceStatus.DOWNLOADED

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
