#!/usr/bin/env python3
"""Run development test modules in fresh interpreters for independent RSS budgets."""

import argparse
from pathlib import Path
import subprocess
import sys


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--test-dir', type=Path,
                        default=Path(__file__).resolve().parents[1] / 'tests')
    parser.add_argument('--junit-dir', type=Path,
                        help='new directory for one JUnit XML report per module')
    args = parser.parse_args(argv)
    directory = args.test_dir.resolve()
    modules = sorted(directory.glob('test_*.py')) if directory.is_dir() else []
    if not modules:
        parser.error('test directory contains no test modules')
    if args.junit_dir is not None:
        reports = args.junit_dir.resolve()
        reports.mkdir(parents=True, exist_ok=False)
    failures = []
    for module in modules:
        print('\nRunning ' + module.name, flush=True)
        command = [sys.executable, '-m', 'pytest', '-q', str(module)]
        if args.junit_dir is not None:
            command.append('--junitxml=' + str(reports / (module.stem + '.xml')))
        result = subprocess.run(command)
        if result.returncode == 2:
            return 2
        if result.returncode:
            failures.append(module.name)
    print('\n' + str(len(modules)) + ' modules; '
          + str(len(failures)) + ' failed.', flush=True)
    if failures:
        print('Failed modules: ' + ', '.join(failures), file=sys.stderr)
    return 1 if failures else 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
