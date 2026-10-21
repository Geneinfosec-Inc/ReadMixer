# Plan 01 — Packaging, interfaces, and compatibility

Status: Planned.

Prerequisite: the accepted scope and defect register in
[Plan 00](00-audit-and-transfer-decisions.md). Input semantics are owned by
[Plan 02](02-paired-input-and-lossless-parsing.md), resource resolution by
[Plan 03](03-resource-admission-and-storage.md), and artifact schemas by
[Plan 06](06-bundles-keys-indexes-and-manifests.md).

## Problem and source evidence

The predecessor's [CLI](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/readmixer.py:20)
calls `process.scramble`, has no inspection, verification, or recovery command,
and prints `Process Completed` after the call even when the engine logged and
swallowed a failure. Its `--max-ram` help uses GB while the implementation
multiplies by powers of 1024. Its chunk-size control describes independent reads.
These are defects PY-12, PY-18, and PY-20 in the audit register.

The source tree has requirements and a Conda environment but no modern installed
package interface. Importing `process` depends on running from the expected
directory. The [dependency files](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/requirements.txt)
also mix runtime and plotting requirements.

Transfer the separation of typed configuration, reports, and CLI dispatch from
Rust [config.rs](../../crates/readmixer-core/src/config.rs),
[recovery.rs](../../crates/readmixer-core/src/recovery.rs), and
[main.rs](../../crates/readmixer-cli/src/main.rs). Their exact API and wire
versions are not Python compatibility targets. Rust's public recovery selects
one sample; this Python API deliberately accepts a selection of several.

## Package and dependency contract

Create a `pyproject.toml`-managed `src/readmixer_py/` package inside the Python
repository, with a console script `readmixer-py` and module invocation
`python -m readmixer_py`. Use the distinct package name to avoid the old
`readmixer/readmixer.py` script shadowing an installed module of the same name.

Use CPython 3.11 as the minimum language version on 64-bit Linux and macOS.
Declare NumPy, PyCryptodome, and psutil as core dependencies. Standard-library
modules provide configuration parsing, filesystem operations, hashing, gzip,
zlib, binary framing, and multiprocessing. No new Rust binding, pandas, SciPy,
database, or runtime `bgzip` dependency is needed.

Separate `dev` and `plots` optional dependencies. The implementation PR must
choose and lock a tested compatible dependency set, including platform markers
where necessary; do not infer reproducibility from open-ended lower bounds.
Record those resolved versions in Plan 10's acceptance evidence. A dependency
security scan does not prove the algorithm secure or data recoverable.

Keep the old `readmixer/readmixer.py` location as a small installed-package
forwarder. It invokes the same new argument parser and exits with its result.
It must not import the legacy `process.scramble`. The old `process` API is not
kept as a second execution path. Remove it from supported examples and exports;
historical source remains available through Git history.

## Public command contract

All examples here describe future commands. Paths in examples are placeholders.

```bash
readmixer-py inspect --samples inputs/samples.tsv
readmixer-py inspect --samples inputs/samples.tsv --deep
readmixer-py mix --samples inputs/samples.tsv --output pooled --max-ram 4GiB
readmixer-py mix --input input-directory --output pooled --storage external
# Set POOL_MANIFEST to the manifest path reported by a completed mix.
readmixer-py verify --manifest "$POOL_MANIFEST"
readmixer-py verify --manifest "$POOL_MANIFEST" --originals inputs/samples.tsv
readmixer-py recover --manifest "$POOL_MANIFEST" --sample sampleA --sample sampleB --output restored
readmixer-py recover --manifest "$POOL_MANIFEST" --sample sampleA --output restored --io sequential
```

`mix` requires exactly one of `--input DIR` or `--samples TSV`, plus `--output
DIR`. `inspect` accepts the same mutually exclusive input selectors and has no
output directory. With a TSV, its parent directory is the input containment
root; with discovery, the explicit input directory is the root. Plan 02 owns
path, naming, and TSV validation. No implicit current-directory discovery runs
when a required selector is missing.

`inspect` without `--deep` reads directory/catalog and resource facts without
creating artifacts or claiming decoded counts. Deep inspection validates both
mate streams and reports exact counts and normalized bytes. It still creates
no pool or scratch data. It is a separate requested pass, not an automatic
pre-count imposed on mixing. Both-empty sample status and whether at least two
nonempty samples exist are reported after deep inspection.
Inspection of a valid catalog may succeed while reporting fewer than two
nonempty samples and therefore `mix_eligible: false`. Malformed records or
mismatched mates still fail inspection; this is not a successful mix.

`verify` requires `--manifest PATH`; all pool artifacts resolve beneath that
manifest's parent. `--originals TSV` enables comparison with original inputs.
Default verification is complete pool verification, not a quick selected-data
check. No `--skip-integrity` or acceptance of legacy manifests is introduced.

`recover` requires a manifest, one or more repeatable `--sample ID` selectors,
and a destination. Resolve all selectors before creating scratch or outputs;
unknown or repeated IDs are configuration errors. CLI order does not affect
processing order: use canonical catalog order. The `--io` choices are `indexed`
(default) and `sequential`; there is no silent fallback. No sample regexes,
wildcards, replacement operations, or partial read-range recovery are included.

## Options, defaults, and units

Use shared parsing and validation rather than reimplementing flags per command:

- `--workers N`: positive integer upper bound; absent means automatic resource
  admission. One worker uses the serial path without a needless child process.
  The report distinguishes requested and admitted concurrency.
- `--max-ram SIZE`: absent means Plan 03's detected ceiling; supplied values
  never increase a tighter detected limit. It covers the entire process group,
  not a per-worker allocation. This is an admission/monitoring contract rather
  than a universal OS-enforced RSS guarantee.
- `--storage auto|external|memory`: default `auto`. External scratch is always
  encrypted. Memory mode forbids algorithmic spill and fails if admission
  cannot complete. Final output files still require disk.
- `--temp-dir DIR`: default to the output destination for mix/recover and the
  platform temporary directory for verify. Inspect never uses scratch.
- `--max-record-bytes SIZE`: default `1MiB` for input/deep inspection; it bounds
  the complete normalized original mate record. Verify/recover consume the
  manifest's recorded bound and may accept a stricter caller bound, but may not
  silently weaken the manifest's validation contract. Encoded frames and
  rewritten headers have separate derived bounds.
- `--max-total-uncompressed SIZE`: optional stricter decoded-work guard with
  operation-specific accounting defined in Plan 03. Without it, mixing still
  checks integer overflow, individual-record bounds, storage, and resources.
- `--max-artifact-bytes SIZE`: mix/recover only; mix defaults to `5000000000B`,
  allowed range `65564B..5000000000B`. Omitted recovery limits inherit the
  pool's cap; an explicit recovery limit may reduce it but must not exceed it.
  Verify enforces the committed cap without changing it.
- `--compression-level N`: mix/recover only, integers 0 through 9, default 6.
  Output is BGZF at every level, including level 0. No ordinary-gzip or plain
  pool-output mode is introduced in this release.
- `--log-level error|warning|info|debug`: default `info`; logs go to stderr and
  never contain record fields, mapping rows, secret keys, or raw sample paths.
- `--json`: emit the operation's bounded, versioned, redacted result on stdout.
  Human-readable status is the default. Sensitive restoration metadata stays in
  the private manifest/receipt rather than general logs.

`SIZE` accepts a positive integral magnitude with `B`, `KB`, `MB`, `GB`, `KiB`,
`MiB`, or `GiB`; a bare integer means bytes. Decimal and binary suffixes retain
their conventional meanings. Reject fractional, negative, zero, nonfinite,
overflowing, and whitespace-ambiguous values. Reports include resolved integer
bytes; never label GiB as GB. Inspect accepts relevant input/resource options
but does not pretend that write-dependent filesystem guarantees were exercised.

## Python API and module boundaries

Expose from `readmixer_py`:

- `mix(MixConfig) -> MixReport`.
- `inspect(InspectConfig) -> InspectReport`.
- `verify(VerifyConfig) -> VerifyReport`.
- `recover(RecoverConfig) -> RecoverReport`.

Use frozen typed dataclasses for public configurations and reports, `pathlib.Path`
for paths, enums for storage/recovery modes, integer bytes for size values, and
an immutable tuple of sample IDs for recovery. Configuration, catalog, and
path admission precede scratch creation; record validation occurs during the
single streaming pass before admitting each pair and before committing output.
The library never calls `exit`, `sys.exit`, configures global logging,
changes the global multiprocessing start method, or silently catches an error.
It uses its own documented spawn context when concurrency is required.

Separate `cli`, `config`, `errors`, `catalog`, `fastq`, `resources`, `storage`,
`crypto`, `shuffle`, `artifacts`, `verify`, and `recover` responsibilities beneath
`src/readmixer_py/`. Plans may split modules internally, but there is one
configuration validator, one pair codec, one artifact reader, and one transaction
implementation. A writer must not be the sole oracle for its corresponding test.

`ReadMixerError` is the library base exception. Public categories are
`ConfigurationError`, `InputError`, `UnsupportedFormatError`, `ResourceError`,
`IntegrityError`, `PublicationError`, and `CancelledError`. Preserve chained
causes. A typed worker failure records stage and opaque task identity and is
translated back without masking the causal error with cancellation fallout.
Never attach the failing raw FASTQ header to an exception message.

Reports include `report_version: 1`, operation, completion state, aggregate
counts, normalized/physical byte totals where known, resolved resource settings,
and timings. Successful mix/recover results name the committed marker. Verify
records whether original inputs were checked; recover records the chosen I/O
strategy and selected-data assurance. Results are not an unbounded list per
pair. Detailed private artifact inventories belong to the bounded manifest or
receipt. Inspector unknown counts are null, not fabricated zeros.
Mix/recover reports also carry `cleanup_complete` and bounded warning codes.
A handled cleanup-only error after durable commit returns the committed result
with `cleanup_complete: false` and a warning; it does not delete valid output or
turn the completed data operation into a failed mix/recovery.

## Compatibility, exit status, and failure semantics

This is a major Python release boundary. Preserve ordinary original plain/gzip
FASTQ input support; reject legacy pools, lossy legacy CSVs, Rust v2/v3 manifests,
and unknown Python schema/algorithm versions. A familiar filename is not format
identification. The schema identifier is `readmixer-python-pool`, version 1,
with explicit normalization and shuffle algorithm versions owned by Plan 06.

Reject `-s`, `--scramble`, old `-c`/`--chunk_size`, and old compression aliases
with a short replacement example. No legacy argument is silently translated to
a changed semantic. Support `--help` and `--version` without importing NumPy or
starting workers. Missing installation from the forwarding script produces an
installation hint and nonzero status.

Exit 0 means the requested operation completed its documented checks and any
required marker was committed. Exit 2 is a CLI/configuration error, exit 1 an
operational/input/integrity/resource failure, and exit 130 a user interrupt.
SIGTERM cancellation exits 143 where a signal handler can run. No success banner
or success-shaped JSON is emitted for a failed or indeterminate operation. A
requested command is not successfully completed merely because cleanup
succeeded. Handled post-commit cleanup warnings retain exit 0. An explicit user
interrupt still uses its cancellation exit status; if commit already completed,
report that fact and preserve the committed artifacts. A marker-link/fsync
failure is an operational error whose on-disk commit state can be indeterminate,
as specified in Plan 05.

## Implementation sequence

1. Add packaging, supported-platform declarations, optional extras, and clean
   editable/wheel installation checks; keep runtime algorithms unexposed until
   the complete paired path is ready.
2. Define dataclasses, enums, exceptions, size parsing, and bounded reports;
   freeze their CLI/API correspondence in tests.
3. Add subcommands and the old-path forwarding wrapper. Stubbed internal work
   must fail explicitly during development, never create plausible empty pools.
4. Integrate catalog/resource inspection, then mix, full verify, and batch
   recovery as their dependent plans complete.
5. Remove supported entrypoints into the old engine, rewrite help/migration
   examples, and require subprocess tests of real exit behavior.

## Tests and completion criteria

- `API-01`: clean wheel/editable installs, console command, module invocation,
  and the old-path forwarder work outside the source working directory.
- `API-02`: help/version start no workers; missing/ambiguous selectors, invalid
  sizes, unsupported flags, and duplicate recovery selections exit 2.
- `API-03`: missing inputs, malformed FASTQ, disk failure, worker failure, and
  compression failure propagate to nonzero CLI status without a success marker.
- `API-04`: library failures are typed exceptions, with no process exit, global
  logger mutation, or global start-method mutation.
- `API-05`: unknown/legacy/Rust formats fail before output creation; plain/gzip
  originals still enter the new parser normally.
- `API-06`: shallow inspection reports unknown decoded counts; deep inspection
  reports validated counts and creates no output or scratch artifacts.
- `API-07`: reports and logs remain bounded and exclude source record data and
  secrets, including at debug level and through chained failures.

- [ ] One installed package and execution path replace working-directory imports.
- [ ] Public interfaces, units, defaults, error categories, and exit statuses agree.
- [ ] No old engine, old artifact inference, or hidden seed override remains.
- [ ] Linux/macOS packaging and command tests pass for Plan 10's support matrix.
- [ ] Documentation describes the paired-only compatibility break explicitly.
