from __future__ import annotations

from argparse import ArgumentParser
from pathlib import Path

from rich.console import Console

from sourced.dataset.db import SOURCED_DATA_DIR, GlobalStore


def list_datasets(store: GlobalStore, console: Console) -> None:
    for dataset_name, dataset in store.datasets.items():
        console.print(dataset_name, dataset.path)


def create_dataset(
    store: GlobalStore,
    console: Console,
    name: str,
    source: str,
    sample_size: int,
    base_data_dir: Path,
) -> None:
    download_dir = base_data_dir / name
    download_dir.mkdir(exist_ok=True)

    if source == "pypi-popular" or source == "pypi-all":
        from sourced.dataset.pypi import create_pypi_dataset, download_pypi_dataset

        dataset = store.datasets[name] = create_pypi_dataset(
            console,
            name=name,
            download_dir=download_dir,
            all=source == "pypi-all",
            sample_size=sample_size,
        )
        store.cache()

        download_pypi_dataset(console, dataset)
    else:
        raise NotImplementedError(f"Unknown source: {source}")


def main(argv: list[str] | None = None) -> None:
    parser = ArgumentParser("daedalus datasets")
    subparsers = parser.add_subparsers(title="action", required=True)

    list_parser = subparsers.add_parser("list")
    list_parser.set_defaults(func=list_datasets)

    create_parser = subparsers.add_parser("create")
    create_parser.add_argument(
        "name",
        type=str,
        help="The name of the dataset to create",
    )
    create_parser.add_argument(
        "--source",
        type=str,
        help="The source of dataset to create",
        choices=["pypi-all", "pypi-popular"],
        default="pypi-popular",
    )
    create_parser.add_argument(
        "--sample-size",
        type=int,
        help="The number of packages to sample from the source",
        default=20,
    )
    create_parser.add_argument(
        "--base-data-dir",
        type=Path,
        help="The directory to download the packages to",
        default=SOURCED_DATA_DIR,
    )
    create_parser.set_defaults(func=create_dataset)

    options = vars(parser.parse_args(argv))
    action = options.pop("func")

    store = GlobalStore.from_file()
    with Console() as console:
        action(store, console, **options)


if __name__ == "__main__":
    main()
