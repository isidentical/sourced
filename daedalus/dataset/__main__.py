from __future__ import annotations

from argparse import ArgumentParser

from rich.console import Console

from daedalus.dataset.db import GlobalStore


def list_datasets(console: Console) -> None:
    store = GlobalStore()
    for dataset_name, dataset in store.datasets.items():
        console.print(dataset_name, dataset.path)


def create_dataset(console: Console, name: str, source: str) -> None:
    raise NotImplementedError


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
        choices=[
            "pypi",
        ],
        default="pypi",
    )
    create_parser.set_defaults(func=create_dataset)

    options = vars(parser.parse_args(argv))
    action = options.pop("func")
    with Console() as console:
        action(console, **options)


if __name__ == "__main__":
    main()
