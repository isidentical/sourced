from __future__ import annotations

import argparse
import sys

from sourced.dataset import __main__ as datasets_cli


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser("sourced")
    parser.add_argument(
        "action",
        type=str,
        help="Choose a subcommand to run",
        choices=["datasets"],
    )

    argv = argv or sys.argv[1:]

    # Only parse the first argument or the parser will interpret
    # daedalus datasets list --help as daedalus --help.
    options, _ = parser.parse_known_args(argv[:1])
    leftover_args = argv[1:]
    if options.action == "datasets":
        datasets_cli.main(leftover_args)
    else:
        raise NotImplementedError(f"Unknown action {options.action}")


if __name__ == "__main__":
    main()
