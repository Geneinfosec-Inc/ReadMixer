# Development installation

Use 64-bit CPython 3.11 or later and install from the repository root following
[README](../README.md). The package is `readmixer-py`, with an installed
`readmixer-py` command and the equivalent `python -m readmixer_py` interface.
The development build supports inspect, mix, verify and indexed/sequential recovery.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e .
readmixer-py --help
readmixer-py inspect --input /path/to/paired-inputs --deep
```

For a regular installation, use `python -m pip install .` instead of the editable
installation above. Use `python -m pip install -e '.[dev]'` for development tools
or `python -m pip install -e '.[plots]'` for optional historical chart scripts.
The README includes build commands and the complete mix, verify and recovery workflow.

Linux and macOS are the intended platforms. CPython 3.11 is the locally exercised
baseline. The frozen dev10 candidate had CPython 3.14 regression failures;
the current checkout includes test portability corrections, but has not been
validated locally on CPython 3.14.
Supported-platform release qualification remains open. CI definitions and local
functional runs do not by themselves establish release qualification. Compiler
requirements depend on the dependency wheels available for the chosen environment.

For Conda, run from the repository root:

```bash
conda env create -f readmixer/environment.yml
conda activate readmixer-env
python -m pip install .
readmixer-py --help
```

To use the setup helper in an already activated environment, run
`bash readmixer/setup.sh`; it performs an editable package install.
`requirements*.txt` list core
dependencies only and do not install the package or its console entrypoint.

The old `-s`, `-c`, and compression aliases are intentionally rejected. There is
no compatibility execution path or legacy-pool salvage. Follow the
[plans](python_update_plans/README.md) for completed functionality and remaining qualification work.

Public mix/verify/recover may launch admitted `spawn` workers in dev7. Protect
library entry code with `if __name__ == "__main__":` (the CLI already does this).
A CPU budget of one stays synchronous. Requested workers are an upper bound;
resource admission chooses the actual process/compression partition.
