# Sourced

Tooling around mass-scale Python source code analysis.

## Usage

Currently there are two datasets: `pypi-all` and `pypi-popular` although I highly recommend `pypi-popular` if you intend to keep your sample size low (the chance of getting far more relevant results with it higher compared to `pypi-all`).

You can check out any number of datasets with different sample sizes:
```
$ sourced datasets create \
    --source pypi-popular \
    --sample-size 10 \
    playground
```


By default it will download all then source code under `~/.cache/sourced/0.0.1/<name>` but it might be more pleasant to have a separate directory outside of your home:

```
$ sourced datasets create \
    --source pypi-popular \
    --sample-size 5000 \
    --base-data-dir /mnt/my-giant-disk/sourced-datasets \
    top-5000-packages
```

All these datasets are accessible through the CLI as long as those paths
exist:

```
$ sourced datasets list
playground /path/to/.cache/sourced/0.0.1/playground
top-5000-packages /path/to/my-giant-disk/sourced-datasets/top-5000-packages
```

## Running analyses on source code

As soon as you have a dataset checked out, you can run any analyses on
it with the tooling offered in this package. Here is a simple program
that parses every file in the dataset to find out what is the most common
name:

```py
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
```

```console
$ python examples/python_specific_source.py playground
Found 10 sources
Collected 959 files from 10 unique projects.
self: 24489
os: 1821
str: 1735
request: 1157
response: 1064
value: 1029
pytest: 984
mock: 966
name: 837
r: 770
isinstance: 715
len: 705
cmd: 701
client: 674
params: 672
path: 668
key: 659
pool: 623
int: 599
config: 553
```
