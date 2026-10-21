# Python paired-workflow tests

Run from the repository root with 64-bit CPython 3.11+:

```bash
python -m pip install -e '.[dev]' pysam
python scripts/run_tests.py
```

The automated suite is in `tests/`. Fixtures use artificial bytes in temporary
directories. Private or downloaded reads are not required.

The development runner executes every `test_*.py` module in a separate Python
process and returns nonzero if any module fails. Resource admission includes the
coordinator's real RSS: running all modules in one long-lived pytest process can
retain native allocator arenas from earlier array/codec tests and cause unrelated
small-budget fixtures to refuse before their intended assertions. Module isolation
preserves the original budgets, assertions, fixture setup and retained-traceback
lifetime checks. Use `python -m pytest -q tests/test_pipeline.py` for a single
module, and `python scripts/run_tests.py --junit-dir output/source-test-results`
to retain one report per module in a new directory. Run external tools separately:

```bash
python -m pytest -q tools/release/tests
```

Development CI uses this runner for source and outside-checkout wheel tests on
Linux/macOS with CPython 3.11/3.14. Exactly nine Linux namespace, systemd, cgroup
or `/proc` mechanism cases are individually nonapplicable on macOS. The external
suite includes real native portable child-limit and timeout checks on both OSes.
These changes do not waive failures or reclassify historical frozen-candidate
qualification. The working-tree reference ownership oracle uses an independent
runtime baseline plus an extra-owner negative control; boundary-fixture setup
explicitly selects one worker. Native ENOSPC cleanup is exercised in the storage
and transaction modules; only the Linux `/proc/self/fd` qualification observer is
nonapplicable on macOS. Frozen evidence below retains its original counts.

`test_worker_storage_cancellation.py` injects real SIGINT/SIGTERM at the actual
exclusive-open, identity and close handoff instructions. The six cases rejected
the original worker-write implementation and pass with the CLI adoption guard.
Additional controls preserve the first pending signal, a primary error and clean
state for the next invocation. Real public workflow signal tests retain exact
descriptor, child-process, lease and artifact cleanup assertions.
Four directory-check cases inject real signals during traversal and caller
handoff, retain strict descriptor equality, and prove the anchor remains usable.
All four reject the original directory check and pass with protected adoption.

The 2026-10-01 redundancy review inspected all 95 test modules, including the
external release tools. It reduced the working-tree suite from 2,977 to 2,966
collected cases: 2,694 application cases and 272 release-tool cases. Historical
frozen-candidate counts below still describe their original archives.

Only cases with preserved assertions and scenarios were removed:

- Four middle-position LF cases in `test_paircodec_native_scan.py` duplicate
  byte 10 in `test_every_byte_matches_old_predicate_in_every_line` for fields
  1, 3, 5, and 7 with the same mixed buffer representations. Beginning/end
  positions, metadata fields, and all buffer types remain covered.
- The STOP row in the positive-grant test in `test_worker_protocol.py` returned
  without assertions. STOP's wire-format and rejection tests remain.
- `test_P06_009_fixed_context_bundle_bytes_match_all_storage_modes` is subsumed
  by `test_P15_T01_serial_six_artifacts_match_complete_independent_reference`,
  which compares every mode to the same independent bytes and manifest, at two
  compression levels with both tiny and rollover inputs.
- The older index-reserved-field, duplicate-original-domain, and duplicate-tag
  tests in `test_pipeline.py` and `test_verification_faults.py` are subsumed by
  `test_P15_T03_every_index_entry_field_reaches_public_join[reserved-verify]`,
  `test_P15_T03_equal_totals_and_identical_biology_do_not_hide_domain_substitution[duplicate_missing-verify]`,
  and `test_P15_T03_duplicate_tags_preserve_domain_and_have_explicit_scope[verify]`
  in `test_hostile_pool_matrix.py`. These retain the mutations and additionally
  witness validation, cleanup, and unchanged sources. Each retained test also
  rejected an audit-only in-memory bypass of its corresponding validator.
- The standalone platform-suite success assertion is covered by
  `test_complete_bound_platform_and_raw_integrity` in the release gates suite,
  which invokes the same assessment for source and installed results. The
  standalone report-writing test is covered by
  `test_portable_child_nofile_observed_parent_unchanged_and_reports_exclusive`,
  whose retained assertions now check the complete JSON after overwrite refusal.

Storage modes, public entrypoints, independent oracles, boundary values, and
failure/cleanup stages remain separate where they exercise distinct behavior.
Test counts and passing checks alone do not establish complete functional
coverage or a mathematically minimal suite.

Validation after the reduction: 2,694 application tests passed and 272 external
release-tool tests passed, in separate pytest processes as documented here.
Per-test line/branch tracing found no lost coverage attributable to the removed
cases; report-writer coverage was also traced in the retained child process.
The suites must run separately because the release applicability test starts a
nested pytest collection with a module name also used by the application suite.

The retained suite covers:

- `test_api.py`, `test_catalog.py`, `test_fastq.py`, `test_resources.py`: typed
  API/CLI errors, exact size syntax, lazy imports, safe catalogs, lossless paired
  parsing, complete gzip validation, normalized counts, and resource leases.
- `test_metadata_workflows.py`, `test_catalog_metadata.py`, `test_storage_metadata.py`: shared metadata ceilings across complete workflows
  in all three storage modes, serialization overlap, retained descriptors/reports,
  manifest parser admission, and failure cleanup with retained tracebacks.
- `test_rollover_boundaries.py`: independent reachable R1/R2/key exact/one-over
  cases, decimal-offset changes at rollover, and index/sidecar domination proofs.
- `test_crypto.py`, `test_paircodec.py`: independent key-derivation vectors,
  domain separation, pseudonymous tags, OS entropy, exact private frame bytes,
  bounds, opaque metadata, and frame rejection before destination mutation.
- `test_header_native_scan.py`, `test_paircodec_native_scan.py`: independent
  exhaustive byte/role/error equivalence, borrowed mutable views and bounded
  allocation for native buffer searches.
- `test_sorting_auto_scale.py`: encrypted auto-mode merge outputs larger than
  admitted RAM, strict-memory refusal, storage-policy isolation and write faults.
- `test_qualification_observation_journal.py`: private exclusive completed-row
  checkpoints, interrupted evidence, causal journal failures and success ordering.
- `test_disk_paths.py`: shared-filesystem admission, partial writes, capacity
  query failures, atomic accounting, no-follow anchors, permission changes,
  input-tree exclusion, and foreign-directory preservation.
- `test_storage.py`: all three storage modes, bounded authenticated blocks,
  corruption, truncation, reordered blocks, compression expansion, seals,
  cross-block records, spill overlap, pinned readers, cancellation, allocation,
  I/O failures, ENOSPC, and cleanup.
- `test_ledger.py`, `test_ledger_faults.py`: authenticated backward page chains,
  expected roots, bounded metadata, ownership transfer, corrupt earlier pages,
  interrupted deletion retries, and cleanup with saturated memory/descriptors.
- `test_sorting.py`: multiple encrypted runs and merge passes, bounded fan-in,
  enlarged records across blocks, independent item totals, exact bitmap/sorted
  coverage, duplicates/gaps, cancellation, and faulted run ownership transfers.
- `test_transaction.py`: no-replace publication, identity-checked private
  partials, physical caps, final-directory sync, last completion marker,
  rollback, foreign-file preservation, postcommit cleanup, and signal handling.
- `test_storage_integration.py`: synthetic parser/frame/sorter/coverage and
  publication composition, with independent expected bytes and identities.
- `test_shuffle.py`, `test_shuffle_oracle.py`: independent bucket/rank oracles, full-batch collision retry,
  canonical refinement, all storage modes, the fixed 32-root distribution fixture,
  and the exact 100,000-pair predecessor chunk-boundary regression.
- `test_bgzf.py`, `test_artifact_metadata.py`: canonical BGZF, standard sidecars,
  virtual offsets, exact key/index wire layouts, strict JSON, physical limits,
  hostile bytes, and independent installed HTSlib/pysam interoperability.
- `test_pipeline.py`, `test_verification_faults.py`, `test_pool_scan.py`: complete
  pools, optional originals, hostile paths, same-byte hashing, exact bijections,
  duplicated tags, self-consistent index mutations, greedy boundary proofs,
  and rollback after actual protected writes.
- `test_recovery.py`, `test_recovery_lifetimes.py`, `test_large_records.py`: indexed/sequential union gathering,
  original-order bytes, opaque and enlarged metadata, empty samples, different
  restored part boundaries, index bypass, whole-batch rollback, and immediate
  descriptor/buffer release while callers retain failure tracebacks.
- `test_ingress_workflow.py`: one forward decoding pass per original and exact
  recovery of sample-sheet paths with repeated leaf basenames.
- `test_synthetic_tooling.py`: reproducible versioned fixture bytes, independent
  normalized expectations, benchmark correctness checks, and package fingerprints.
- `test_execution.py`, `test_parallel_pipeline.py`: CPU/affinity/quota admission,
  bounded shared compression, delayed completions, lease ownership faults,
  first-error propagation, and exact artifacts at one/two/four admitted CPUs.
- `test_cli_workflow.py`: public command roundtrips, explicit validation scopes,
  redacted reports, lazy API exports, and postcommit cancellation context.

For optional local data checks (the input directory is ignored by Git):

```bash
PYTHONPATH=src python -m readmixer_py inspect --input readmixer/input --deep --max-ram 512MiB --json
PYTHONPATH=src python scripts/check_storage_inputs.py --input readmixer/input --pairs-per-sample 128
python scripts/check_roundtrip_inputs.py --input readmixer/input --pairs-per-sample 128
```

The first command validates all input records. The second checks only the stated
prefix of each sample through encrypted scratch and frame decoding. It reports
aggregate counts and equality, without printing reads, source identities, keys,
or private digests. It is not a mix/verify/recover acceptance run. The third command creates private,
numerically aliased prefix fixtures and checks complete mixing, full verification,
and both recovery modes against independent original-parser hashes and gzip
reads. It removes its fixtures and outputs before reporting success. That test
still covers only the stated prefix, not the complete supplied dataset.

Passing these tests does not satisfy the full
[Plan 10 gate](readmixer/python_update_plans/10-acceptance-documentation-and-release.md).
Complete serial/spawned workflows, local HTSlib interoperability and Plan 15's
bounded failure acceptance are exercised. Plan 16's measured local resource,
greater-than-RAM, performance and full supplied-data lanes have passed for the
frozen dev10 candidate under the recorded controls. Plan 17's supported-platform
qualification and the Plan 10 release-completion gate remain open; package
publication is a separate action.
Commands, results, source hashes, and dependency versions are recorded in
[implementation progress](readmixer/python_update_plans/IMPLEMENTATION.md) and
[historical dev2 evidence](readmixer/python_update_plans/pipeline-evidence.json) and
[historical dev3 evidence](readmixer/python_update_plans/metadata-evidence.json).
Historical dev4 results are recorded in
[ownership evidence](readmixer/python_update_plans/ownership-evidence.json).
Dev5 uses separate [spawn evidence](readmixer/python_update_plans/spawn-evidence.json).


The versioned synthetic generator and benchmark harness are development tools:
`scripts/generate_fixtures.py` produces public artificial fixtures; run
`scripts/benchmark_pipeline.py --help` for a complete-workflow benchmark with a
warm-up and at least five measurements. Preserve individual observations and
validation results. A small startup benchmark does not qualify large workloads,
cold-cache performance, native memory coefficients, or speedup claims.

Plan 12 tests include 1,000-artifact inventories in all storage modes, interrupted
registration/link/refund handoffs, authenticated cleanup failure, and a public
80-pair workflow forcing 80 bundles and 80 recovery parts in each mode. The latter
uses public repetitive long records to cross real 64 KiB ownership pages; it is
not a greater-than-RAM qualification. `test_indexed_union_io.py` independently
checks real compressed payload reads, boundary traversal, shared decoded members,
full index/sidecar consumption and sequential bypass. `test_secured_field_mutations.py`
rebuilds self-consistent artifacts before testing semantic join rejection.

The generator retains `v1` golden bytes and adds `high-compressibility-v2` and
`low-compressibility-v2` profiles with `--length-mode varied-v2`. Example:

```bash
python scripts/benchmark_pipeline.py --counts 2,1,0 --sequence-bytes 8 \
  --profile low-compressibility-v2 --length-mode varied-v2 --startup --workers 1
```

`--startup` records separate fresh-interpreter, API-import and real CLI-inspect
lanes, each with one warm-up and five measurements. CPU reports distinguish
coordinator CPU, waited child CPU, and sampled live-child observations to avoid
double counting. OS-thread deltas are observations, not library attribution;
short-lived unsampled peaks remain a limitation. The dev10 Plan 16 evidence
includes component timings, artifact-class I/O, controlled backpressure and the
complete local host matrix; qualification applies only to those recorded builds
and configurations.

The existing `.github/workflows/python.yml` is development feedback: each job
builds/resolves its own artifacts and is not immutable release evidence. The new
manual `.github/workflows/release-qualification.yml` transfers the exact frozen
candidate to Linux x86-64/macOS arm64 CPython 3.11/3.14 cells, captures native
wheel locks before isolated installs, and retains reports and optional encrypted
raw diagnostics. Configuration is not execution. The
[release operator guide](tools/release/README.md) contains runnable host/CI commands,
resource/supplement scope, applicability rules and evidence-return instructions.

A fresh Linux CPython 3.11.7 lane passed 2,703 source and 2,703 installed tests.
The repaired CPython 3.14.7 lane exposed two frozen recovery-lifetime assertions
that assume an interpreter-specific exact reference count; its installed suite also
fails a boundary fixture that encounters an earlier resource refusal. Source
finished 2,701 passed / 2 failed; installed finished 2,700 passed / 3 failed. Failed reports remain
unqualified; an independent control probe and preparation-only portable correction
do not waive them. See [current release evidence](readmixer/python_update_plans/release-evidence.json).
Run the external tools' regression suite separately from frozen candidate tests:

```bash
python -m pytest -q tools/release/tests -o addopts=
```

These external-tool counts are not additions to historical dev10 source/installed
counts. No successful Linux lane or partial native calibration qualifies macOS,
and no raw skip is silently converted into a production pass.


Dev5 private worker tests are independent of the production pipeline:

- `test_worker_protocol.py`: exact closed wire schemas, encoded bounds and private representations.
- `test_stage_admission.py`: fixed CPU partitions, RAM/metadata/descriptor windows,
  READY growth, exact registered PID observations and excess-only pressure charges.
- `test_stage_scheduler.py`: real spawn, CPU-one synchronous operation, multi-chunk
  transfer, ordered retained results, parent environment restoration, pre-import
  native limits, and shared compression partition accounting.
- `test_stage_review.py`: interrupted ownership returns, duplicate sender leases,
  real terminate/kill escalation, and retained reservations after failed reaping.
- `test_worker_state.py`: forged task/grant IDs, early or oversized results,
  wrong/repeated ACKs, bad offsets and digest failures, with safe diagnostics.
- `test_serial_artifact_oracle.py`: 14 bounded Plan 15 cases build expected six-file
  bytes independently of producer codecs/order/tag helpers and compare both recovery
  modes. Public fixed roots remain test-only; forced-refinement/process coverage is separate.

Spawn tests use guarded fresh interpreters. As with Python spawn generally, a
library caller must protect process-launching entry code with
`if __name__ == "__main__":`; the private scheduler has no supported public API.
The ReadMixer launch lock restores inherited native-thread variables, including
failed starts, but cannot isolate unrelated callers changing `os.environ`.
The interpreter's resource-tracker helper is observed and reserved while an
invocation runs; it may persist afterward and is not a task worker to terminate.
Baseline, channel and sort-workspace coefficients remain unqualified on other
builds. Shortened shutdown deadlines appear only in fault tests; production
uses a global ten-second grace period, five seconds after terminate and a bounded
five-second final reap check. An unreaped worker retains its reservations.
These checks do not implement encrypted worker grants or qualify publication
under production worker failures.


Post-dev5 adds `test_stage_launch_concurrency.py` (controlled launch-boundary
contention and failed-start restoration) and `test_stage_backpressure.py` (real
workers, delayed earliest result, bounded retained outputs and cooperative STOP).
The [handoff record](readmixer/python_update_plans/POST_DEV5_WORKER_ACCEPTANCE.md)
separates 261 focused passes from historical full-suite/installed-wheel evidence.


Dev6 adds `test_stage_startup.py`, `test_worker_memory.py`,
`test_stage_memory_refs.py`, `test_worker_storage.py`, `test_stage_external.py`,
`test_stage_external_faults.py`, `test_worker_external_protocol.py` and
`test_stage_signals.py`. These cover startup reductions, explicit lease receipts,
local subleases, encrypted file capabilities, shared disk credits, paged task
ownership, real worker faults, signals and concurrent launches. The signal and
module harnesses exercise the private scheduler; production parallel CLI stages
remain a Plan 14 requirement. Exact new candidate results are recorded in
[worker handoff evidence](readmixer/python_update_plans/worker-handoff-evidence.json).


Dev7 Plan 14 tests exercise production workers, not just the private scheduler:

- `test_parallel_parser.py` and `test_parser_protocol.py`: deterministic LPT,
  closed snapshots, exact normalized admission and hostile streaming states.
- `test_direct_scatter.py`, `test_worker_scatter.py`, `test_scatter_protocol.py`:
  complete writer admission, shared flush exclusion, paged inventories, raw and
  encrypted generations, interrupted acknowledgments and cleanup ownership.
- `test_parallel_terminals.py` and `test_bulk_tags.py`: fixed topology/rank keys,
  delayed completion, refinement overlap, retained views and bounded reusable AES.
- `test_parallel_sort_policies.py`: closed worker keys, stable multi-pass sorting,
  large-record fallback and full verify/indexed/sequential recovery integration.
- `test_parallel_pipeline.py`: real production CPU/storage/strategy equivalence,
  exact six-artifact bytes, one forward input pass, one union gather and public CLI.
- `test_parallel_workflow_faults.py`: failures at observed production checkpoints,
  no completion marker, preserved originals and reaped workers.

Historical dev7 source/installed-candidate results and exact fingerprints belong to
[parallel pipeline evidence](readmixer/python_update_plans/parallel-pipeline-evidence.json).
The earlier dev5/dev6 sections describe historical scope. Dev8 adds Plan 15's
local acceptance matrix below. Dev10 completes the recorded local Plan 16
qualification; Plan 17's locked supported-platform qualification remains open.


Dev8 Plan 15 acceptance adds:

- `test_parallel_artifact_oracle.py`: real CPU-two/four workflows compare every
  artifact and semantic manifest against the independent serial reference in
  all storage modes, then verify and recover both ways.
- `test_workflow_boundaries.py`, `test_hostile_pool_matrix.py`: exact completion
  and inventory ceilings, independent encoded/resident bounds, repaired outer
  integrity with hostile index/key/member addresses, domain/tag separation and
  the deliberate recovery-scope differences.
- `test_originals_acceptance.py`, `test_recovery_scope_acceptance.py`: relocated
  equivalent originals, content/identity/role/order rejection without scope
  downgrade, and corrupt unread payload versus complete-stream validation.
- `test_workflow_acceptance_faults.py`: actual workflow allocation refusals,
  protected/staged partial writes, ENOSPC, multi-pass merges, source-close and
  compression failures, late recovery rollback and marker/directory-sync faults.
- `test_workflow_signals.py` and guarded `workflow_signal_harness.py`: acknowledged
  parser/native/gather/merge/compression/publication/report checkpoints, real
  SIGINT/SIGTERM, broken IPC, reaped workers, and the three SIGKILL publication
  boundaries. Native checkpoints retain a completed native result before its
  handoff; they do not assert interruption at a particular C instruction.

Tests retain exceptions while checking readers, descriptors, leases, owned
workers and source/foreign-file preservation. SIGKILL tests supervise a private
single-process group and inspect residue; no cleanup after uncatchable termination
or power-loss durability is promised. Production shutdown constants remain
10 seconds cooperative, 5 after terminate, then kill and bounded reaping; actual
escalation tests use shorter private deadlines.

The installed-wheel lane sets `READMIXER_TEST_CONSOLE` to its exact absolute
`readmixer-py` executable, so the reusable CLI tests exercise the installed console
rather than PATH lookup. The full copied suite runs outside the checkout without
`src`; signal helpers import that same installed candidate. This environment
variable is a test-harness selector, not a production option. Candidate-specific
results and source/archive hashes are in
[workflow acceptance evidence](readmixer/python_update_plans/workflow-acceptance-evidence.json).

## Executed local Plan 16 qualification

The frozen dev10 wheel and sdist passed 2,703 source tests and 2,703 tests in an
isolated installed environment outside the checkout. All 25 declared local matrix
cells passed, retaining one warm-up and five observations each: 150 observations,
comprising 126 complete workflows and 24 expected typed resource refusals.
Another 270 supplemental observations cover components, startup, calibration,
fresh processes, independent six-artifact references and compression ownership.

External and auto workflows completed with normalized input greater than four
times every operation's actual admitted RAM budget. Strict-memory scale attempts
refused cleanly. The physical 5,000,000,000-byte cap, controlled resource/device
lanes and required backpressure checkpoints passed. The separate private task
mixed all 30,042,389 supplied pairs, fully verified the pool, recovered every
sample in both modes sequentially, and independently compared normalized fields,
ordinals, counts and digests through EOF. Originals remained unchanged.

The [current qualification evidence](readmixer/python_update_plans/resource-qualification-evidence.json)
binds these results to exact archives, dependencies, host controls and individual
observations. Reused caches, concurrent storage activity, sampled peaks and
build-specific calibration limit interpretation; no universal RSS bound or
speedup is claimed. Historical failures remain separate and preserved. Plan 17
platform qualification and the Plan 10 release gate remain open; publication is
not part of this documentation closure.

## Plan 16 qualification tools

`requirements-qualification-linux-py311.lock` pins the selected local Linux
x86-64 CPython 3.11 build/test/runtime/oracle wheels by hash. Install it in a fresh
venv without system site packages; the locally built psutil wheel must match its
recorded hash. Build the candidate once, hash both archives, then copy the sdist's
scripts/tests outside the checkout and install the exact wheel with `--no-deps`.
Run qualification with that interpreter and native thread environment limits set
before imports. Record commands and exact dependencies; other platforms require
their own resolved locks and actual host evidence.

- `benchmark_stages.py`: bounded independent component timing.
- `benchmark_pipeline.py --telemetry`: in-process complete workflow observations.
- `benchmark_fresh.py`: fresh-process complete workflow timing and import scope.
- `calibrate_resources.py`: versioned working-set charge observations.
- `qualification_matrix.py`: frozen workload definitions and fail-closed evidence
  validation; definitions are not passing measurements.
- `run_qualification.py`: candidate-bound cells, dedicated cgroups and private
  mount/descriptor controls. `--tier` selects bounded, scale, controlled-host or
  release-host lanes. Original inputs are never used by this synthetic matrix.
  Every completed iteration is fsynced to an exclusive private observation journal;
  interrupted journals never count as completed cells or support resume. Run with
  the installed interpreter and the archived scripts directory available for
  sibling imports; `python -I` requires an explicit archived-directory bootstrap.
- `qualify_supplied_data.py`: private complete original inventory, mix, full
  verification and independent all-sample recovery comparison in both modes.
  Provide exact `--wheel`, `--sdist`, their SHA-256 arguments, `--input` and a new
  `--workspace` outside input. Public output is aggregate JSON; private journal
  and fingerprints remain permission-restricted. A failure leaves its diagnostic
  workspace; it is not resumable. Recovery outputs are removed sequentially.

All performance configurations retain a warm-up and at least five observations.
Failures suppress successful performance summaries. The single full supplied-data
correctness task is separate. Neither sampled RSS nor a requested resource limit
proves host enforcement; actual limits and observed checkpoints are required.
