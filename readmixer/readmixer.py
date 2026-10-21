"""Compatibility location for the installed paired-only command line."""

import sys


def main():
    try:
        from readmixer_py.cli import main as run
    except ModuleNotFoundError as exc:
        if exc.name not in ("readmixer_py", "readmixer_py.cli"):
            raise
        print("Install the new package from the repository root with: python -m pip install -e .", file=sys.stderr)
        return 1
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
