"""Entrypoint when running this package as `python -m package`."""

import logging
import sys


def setup_logging():
    """Set up basic logging to stdout."""
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format="%(asctime)s;%(levelname)s;%(message)s",
    )


def main():
    """Entrypoint when running `python -m package`."""
    setup_logging()
    # Add your code here.


if __name__ == "__main__":
    main()
