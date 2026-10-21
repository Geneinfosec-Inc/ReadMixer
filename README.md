# ReadMixer

![ReadMixer](read-mixer-simple-cartoon.jpg)

ReadMixer mixes paired FASTQ samples into a shared pool, verifies the complete
pool, and recovers selected samples in their original pair order. Both mates
stay together through one global permutation. Original headers, plus lines,
sequences and qualities are preserved; line endings normalize to LF, including
a final LF. Recovery does not reproduce the original gzip container bytes.

The current Python package is **`readmixer-py` 2.0.0.dev10**, a development
version. Its interface is `inspect`, `mix`, `verify` and `recover`. The predecessor
engine and its flags are retired. This checkout includes local Linux and macOS
validation; supported-platform release qualification and package publication
remain open. See [validation](#validation-and-development) for scope and evidence.

## Requirements

- 64-bit CPython **3.11 or later** on Linux or macOS. Python 3.11 is the locally
  exercised baseline; development CI also targets Python 3.14.
- Enough disk space for the input, pool, selected recovery outputs and scratch.
  Required space depends on record sizes, compression and storage mode.
- NumPy, PyCryptodome and psutil, installed automatically by pip.

Conda, Rust, HTSlib and plotting packages are not required for normal operation.
Pip normally uses dependency wheels; a compiler may be needed if your platform
requires building a dependency from source. The current runtime does not support
Windows, single-end, interleaved or multilane input, legacy pools, or resuming
incomplete runs.

## Install

Run these commands in a terminal on Linux or macOS:

```bash
git clone https://github.com/Geneinfosec-Inc/ReadMixer.git
cd ReadMixer
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install .
readmixer-py --version
readmixer-py --help
```

Use your Python 3.11+ executable if it has a different name. For an existing
checkout, start at `cd ReadMixer`. Keep the virtual environment activated for the
commands below. `python -m readmixer_py` provides the same CLI. The compatibility
script `python readmixer/readmixer.py` forwards to the installed package.

For development, replace the regular install with an editable install:

```bash
python -m pip install -e '.[dev]'
```

The `dev` extra installs pytest and the package builder. Optional historical
plotting utilities use `python -m pip install -e '.[plots]'`.
[Conda and setup-script instructions](readmixer/INSTALLATION.md) are also available.

## Build a wheel and source archive

From the repository root, in the activated environment:

```bash
python -m pip install -e '.[dev]'
python -m build
```

The build creates `dist/readmixer_py-2.0.0.dev10-py3-none-any.whl` and
`dist/readmixer_py-2.0.0.dev10.tar.gz`. To install the wheel in another environment:

```bash
python -m pip install /path/to/ReadMixer/dist/readmixer_py-2.0.0.dev10-py3-none-any.whl
readmixer-py --version
```

Pip installs runtime dependencies with the wheel. These commands build the
current checkout; they do not reproduce or qualify a previously frozen release
candidate. [Release tooling](tools/release/README.md) documents the separate
archive, dependency-lock and qualification workflow. Build products are ignored
by Git; generated FASTQ pools are not package contents.

## Run: inspect, mix, verify, recover one sample

Replace `/path/to/paired-inputs` with your input directory and `SAMPLE_A` with a
sample ID discovered from its filenames or supplied in a sample sheet. For the
local dataset, the input argument is `readmixer/input`. A pool requires at least
**two nonempty samples**, even when recovering only one afterward.

The following example uses fresh output directories below `output/`, outside
the input tree. Its parent must exist. Keep the JSON reports: they contain elapsed
time in seconds, counts and the exact generated completion-marker name.

```bash
mkdir -p output

# Optional: scan every input record before mixing; this adds a separate read pass.
readmixer-py inspect --input /path/to/paired-inputs --deep --max-ram 2GiB

readmixer-py mix --input /path/to/paired-inputs \
  --output output/pool --storage auto --max-ram 2GiB --workers 4 \
  --json > output/mix-report.json

# Use the exact manifest from this run, rather than choosing a pool by a wildcard.
pool_manifest=$(python -c 'import json; print("output/pool/" + json.load(open("output/mix-report.json", encoding="utf-8"))["manifest"])')

readmixer-py verify --manifest "$pool_manifest" \
  --max-ram 2GiB --workers 4 --json > output/verify-report.json

readmixer-py recover --manifest "$pool_manifest" \
  --output output/recovered --sample SAMPLE_A \
  --max-ram 2GiB --workers 4 --json > output/recover-report.json
```

Run each step only after the preceding command succeeds. Without `--json`, the
CLI prints a human-readable report. Indexed recovery is the default. The recovered
mate FASTQs use BGZF compression with `.fastq.gz` names and `.gzi` indexes; the
committed recovery receipt lists their exact names. A batch receipt commits the recovery
last. Repeat `--sample` to recover multiple samples in one operation.

Existing artifact files are never overwritten. Choose fresh destination names
for another run. New output directories are private; an existing destination must
be owned by the current user and not writable by others. Output and scratch
paths must use real directories rather than symlink ancestors. Scratch defaults
to the output destination for mixing and recovery; `--temp-dir` selects another
existing directory outside the input tree.

### Input layout and sample sheets

Recursive discovery recognizes paired names such as:

```text
paired-inputs/
  SAMPLE_A_R1.fastq.gz
  SAMPLE_A_R2.fastq.gz
  SAMPLE_B_R1.fastq.gz
  SAMPLE_B_R2.fastq.gz
```

`ID_R1_001` / `ID_R2_001` and `ID_1` / `ID_2` are also recognized, with `.fastq`
or `.fq`, optionally `.gz`. IDs must be unique. Missing mates, symlinks, duplicate
physical files, recognizable lane layouts and mixed naming families within a sample are rejected.
Plain/gzip files and concatenated gzip members are supported. Malformed FASTQ,
mismatched mates and damaged gzip streams fail explicitly.

For explicit IDs or filenames, create `samples.tsv` in the input directory with
this exact **tab-separated** header and columns (not space-separated):

```text
sample_id	r1	r2
SAMPLE_A	SAMPLE_A_R1.fastq.gz	SAMPLE_A_R2.fastq.gz
SAMPLE_B	SAMPLE_B_R1.fastq.gz	SAMPLE_B_R2.fastq.gz
```

Paths resolve relative to the sheet's parent and must stay inside that directory.
Replace `--input /path/to/paired-inputs` with
`--samples /path/to/paired-inputs/samples.tsv` in `inspect` or `mix`.

Full verification can additionally compare the explicitly supplied originals:

```bash
readmixer-py verify --manifest "$pool_manifest" \
  --originals /path/to/paired-inputs/samples.tsv --max-ram 2GiB
```

The verifier never automatically reopens source paths stored in a manifest.
To recover sequentially, including when mapping indexes are unavailable, request
that mode explicitly and use another fresh destination:

```bash
readmixer-py recover --manifest "$pool_manifest" \
  --output output/recovered-sequential --sample SAMPLE_A \
  --io sequential --max-ram 2GiB
```

### Resource settings

| Option | Meaning |
| --- | --- |
| `--max-ram 2GiB` | Admission and monitoring allowance, not an OS-enforced RSS limit. |
| `--workers 4` | Upper bound; reports show actual admitted concurrency. `1` runs synchronously. |
| `--storage auto` | Default; can spill protected algorithmic segments to disk. |
| `--storage external` | Uses encrypted algorithmic scratch. |
| `--storage memory` | Keeps admitted algorithmic state in RAM; refuses work that cannot fit. |
| `--compression-level 0..9` | Mixing/recovery BGZF compression; default `6`. |
| `--max-artifact-bytes 5GB` | Mixing file cap; recovery defaults to the pool's cap. Default is 5,000,000,000 bytes. |
| `--max-record-bytes 1MiB` | Mixing/inspection normalized mate-record limit; default 1 MiB. |
| `--max-total-uncompressed SIZE` | Caps actual normalized bytes processed by an operation. |
| `--json` | Bounded aggregate report, including `elapsed_seconds`, without original sample IDs or read contents. |

Sizes are exact: `5GB` is 5,000,000,000 bytes; `5GiB` is 5,368,709,120 bytes.
Whole pairs roll together into synchronized bundles or recovery parts. Every
artifact, including indexes and completion metadata, must fit the file cap;
an oversized singleton fails. Use each command's `--help` for its accepted flags.

## Keep the complete pool

Each bundle contains R1/R2 BGZF FASTQs, their two `.gzi` sidecars, a private BGZF
mapping CSV and a mapping index. The manifest is committed last. Keep and back up
all artifacts together: the manifest and complete private mappings are essential
for recovery. The ephemeral scratch encryption key is not saved or required to
recover a completed pool. Generated files have mode `0600`.

Full `verify` checks all declared artifacts, original/secured identity coverage,
tag uniqueness, offsets, canonical bundle boundaries and reconstructed original
digests. Indexed recovery checks selected records and complete indexes/sidecars;
it does not constitute full pool verification. Both recovery modes prove selected
original ordinals and normalized digests before committing the receipt.

Final mappings, manifests and receipts are private metadata, not an encrypted
keystore. Sequence content remains sensitive and pseudonymous tags do not imply
anonymity. Unsigned hashes establish consistency with the supplied manifest,
not authenticity against wholesale replacement. Hard stops can leave incomplete
residue; a completed manifest or receipt identifies a committed operation.

Exit codes: `0` success, `2` invalid configuration, `1` operational failure,
`130` SIGINT and `143` handled SIGTERM.

## Docker (optional)

Build from the **repository root**; `.dockerignore` excludes local datasets,
outputs, environments and Git metadata from the build context:

```bash
docker build -f readmixer/Dockerfile -t readmixer:dev .
docker run --rm readmixer:dev --help
mkdir -p output
docker run --rm --user "$(id -u):$(id -g)" \
  -v "/absolute/path/to/paired-inputs:/inputs:ro" \
  -v "$PWD/output:/outputs" \
  readmixer:dev mix --input /inputs --output /outputs/pool \
  --max-ram 2GiB --workers 4
```

Allow sufficient RAM and disk in Docker's VM. The Conda alternative builds with
`-f readmixer/Dockerfile.conda`. These are development images; container builds
are separate from native release qualification.

## Validation and development

Install the development extra, then run the application and release-tool suites
in **separate pytest processes**:

```bash
python -m pip install -e '.[dev]' pysam
python scripts/run_tests.py
python -m pytest -q tools/release/tests
```

`pysam` enables independent HTSlib interoperability checks. Install `bgzip` for
the additional executable oracle (`brew install htslib` on macOS, or your Linux
distribution's `tabix` package). These tools are optional for normal operation.
Development CI builds packages and runs source and outside-checkout wheel tests
on Linux/macOS with Python 3.11/3.14. Linux kernel-specific fixtures are individually
nonapplicable on macOS; native portable limit/timeout checks run on both systems.
An unexecuted CI matrix is not evidence of a passing platform.

The application runner starts a fresh interpreter per test module, runs every
case, and returns nonzero if any module fails. This keeps earlier modules' native
allocator memory out of later resource-admission checks. To run one module,
use `python -m pytest -q tests/test_pipeline.py`.

For a small artificial-data trial, generate two samples and run the workflow
above with `--input output/demo/input` and `--sample sample000000`:

```bash
mkdir -p output
python scripts/generate_fixtures.py --output-dir output/demo --counts 1000,1000
```

Keep the pool and recovered directories outside `output/demo`, which is the
fixture catalog root. To check a bounded prefix of the local dataset through
mixing, full verification and both recovery modes:

```bash
python scripts/check_roundtrip_inputs.py --input readmixer/input --pairs-per-sample 128
```

That helper cleans its private temporary data; it does not process the full input.
Automated tests use artificial data. See [TESTING.md](TESTING.md) for fault,
resource, signal and oracle coverage.

A complete local macOS arm64 / Python 3.11.9 run processed **48 samples,
30,042,389 pairs (60,084,778 reads)**, verified the entire pool, and recovered one
sample containing 1,358,331 pairs. An independent comparison consumed both
original and restored mates to EOF and matched fields, order, normalized counts
and digests. Original input files were unchanged.

| Process | Measured wall time |
| --- | --- |
| Mix all 48 samples | 5 h 54 min 47 s |
| Full pool verification | 1 h 41 min 19 s |
| Indexed recovery of one sample | 6 min 17 s |
| Independent recovered/original comparison | 15.43 s |

This run used `auto`, a 24 GiB allowance, four requested workers and compression
level 6. Timings are specific to that workload and machine, with existing OS
caches; they are not universal performance or memory guarantees. The
[aggregate validation record](docs/validation/local-macos-2026-10-21.json)
contains settings and exact times without private paths or sample identifiers.
Its source hashes identify the timed run before the subsequent CLI cancellation
cleanup correction; that correction has separate regression validation.

Historical frozen-candidate evidence retains its original scope. The
[implementation record](readmixer/python_update_plans/IMPLEMENTATION.md),
[plans](readmixer/python_update_plans/README.md) and
[release guide](tools/release/README.md) track remaining release gates. Historical
CPython 3.14 fixture failures have working-tree corrections; local execution of
that Python version remains unverified. Do not relabel historical evidence as
qualification of a newly built archive. Library callers that launch workers must
use Python's `if __name__ == "__main__":` guard.

## Attribution, license and support

ReadMixer was created by GeneInfoSec, Sterling Sawaya and Joseph Parks.
[CITATION.cff](CITATION.cff) and [ARCHIVAL_STRATEGY.md](ARCHIVAL_STRATEGY.md)
describe the historical release and preservation record.

Copyright 2025 GeneInfoSec. Licensed under [Apache License 2.0](LICENSE).
Contact [joe@geneinfosec.com](mailto:joe@geneinfosec.com) for support.
