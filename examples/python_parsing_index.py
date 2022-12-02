from __future__ import annotations

import ast
import json
import tokenize
import sysconfig
import tempfile
from pathlib import Path
from argparse import ArgumentParser
from functools import partial

from sourced import Sourced


CACHE_DIR = Path(tempfile.gettempdir()) / "python-comparison"
CACHE_DIR.mkdir(exist_ok=True)


def can_parse(file: str) -> tuple[str, bool]:
    try:
        with tokenize.open(file) as stream:
            ast.parse(stream.read())
    except BaseException:
        return (file, False)
    else:
        return (file, True)


def compare_action(dataset: str):
    dataset_dir = CACHE_DIR / dataset
    data = {}
    for file in dataset_dir.iterdir():
        version_key = tuple(map(int, file.stem.split(".")))

        raw_data = json.loads(file.read_text())
        data[version_key] = {
            "parsed": {file for file, result in raw_data.items() if result},
            "not_parsed": {file for file, result in raw_data.items() if not result},
        }
        print(
            f"Stats for {version_key}:",
            "parsed:",
            len(data[version_key]["parsed"]),
            "not parsed:",
            len(data[version_key]["not_parsed"]),
        )

    # Files that can be parsed with earlier versions but not
    # the newer ones
    for version_key, version_data in sorted(data.items()):
        for other_version_key, other_version_data in sorted(data.items()):
            if other_version_key <= version_key:
                continue

            print(
                f"Files that can be parsed with {version_key} but not"
                f" {other_version_key}:"
            )
            for file in version_data["parsed"] & other_version_data["not_parsed"]:
                print(f"  {file}")

            print()


def load_cache(cache_dir: Path) -> dict[str, bool]:
    cache_file = cache_dir / (sysconfig.get_python_version() + ".json")
    if cache_file.exists():
        return json.loads(cache_file.read_text())
    else:
        return {}


def save_cache(cache_dir: Path, cache: dict[str, bool]):
    cache_file = cache_dir / (sysconfig.get_python_version() + ".json")
    cache_file.write_text(json.dumps(cache))


def run_action(dataset: str):
    sourced = Sourced()

    py_cache_dir = CACHE_DIR / dataset
    py_cache_dir.mkdir(exist_ok=True)

    cache = load_cache(py_cache_dir)
    data = cache.copy()
    try:
        for file, result in sourced.run_on(
            dataset,
            can_parse,
            filter_func=lambda file: file not in cache,
        ):
            data[file] = result
    except KeyboardInterrupt:
        save_cache(py_cache_dir, data)
        raise

    save_cache(py_cache_dir, data)


def main():
    parser = ArgumentParser()
    subparser = parser.add_subparsers(required=True)

    compare_parse = subparser.add_parser("compare")
    compare_parse.add_argument("dataset", type=str)
    compare_parse.set_defaults(func=compare_action)

    run_parse = subparser.add_parser("run")
    run_parse.add_argument("dataset", type=str)
    run_parse.set_defaults(func=run_action)

    options = vars(parser.parse_args())
    action = options.pop("func")
    action(**options)


if __name__ == "__main__":
    main()
