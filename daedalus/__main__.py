from __future__ import annotations

import argparse
import sys

from daedalus.dataset import __main__ as datasets_cli


def main(argv: list[str] | None = None) -> None:
    daedalus = argparse.ArgumentParser("daedalus")
    daedalus.add_argument(
        "action",
        type=str,
        help="Choose either the analysis or the dataset mode",
        choices=["analysis", "datasets"],
    )

    argv = argv or sys.argv[1:]

    # Only parse the first argument or the parser will interpret
    # daedalus datasets list --help as daedalus --help.
    options, _ = daedalus.parse_known_args(argv[:1])
    leftover_args = argv[1:]
    if options.action == "analysis":
        raise NotImplementedError
    elif options.action == "datasets":
        datasets_cli.main(leftover_args)
    else:
        raise NotImplementedError(f"Unknown action {options.action}")


if __name__ == "__main__":
    main()
