from __future__ import annotations

import ast
import tokenize
from argparse import ArgumentParser
from collections import Counter

from sourced import Sourced


def most_common_name(file: str) -> dict[str, int]:
    usage: dict[str, int] = {}
    try:
        with tokenize.open(file) as stream:
            tree = ast.parse(stream.read())
    except BaseException as exc:
        return usage

    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            usage.setdefault(node.id, 0)
            usage[node.id] += 1
    return usage


def main():
    parser = ArgumentParser()
    parser.add_argument("dataset")

    options = parser.parse_args()
    sourced = Sourced()

    results = Counter()
    for result in sourced.run_on(options.dataset, most_common_name):
        results.update(result)

    for name, count in results.most_common(n=20):
        print(f"{name}: {count}")


if __name__ == "__main__":
    main()
