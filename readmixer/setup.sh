#!/bin/bash
# Install the development package in the caller's selected Python environment.
set -eu
readmixer_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
python -m pip install -e "$readmixer_root"
printf '%s\n' 'Installed readmixer-py with inspect, mix, verify, and recover. Run readmixer-py --help.'
