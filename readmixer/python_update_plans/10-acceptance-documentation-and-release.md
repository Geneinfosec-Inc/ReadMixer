# Plan 10 — Acceptance, documentation, and release

Status: Planned. This is the sole release-completion gate for the Python
modernization described in this directory. Writing these plans does not satisfy
the implementation or release checks below.

Prerequisites: [Plans 01–09](README.md#documents-and-implementation-order)
implemented with their required local tests. The complete serial mix/verify/
recover path must pass before enabling or benchmarking concurrent execution.
No partial writer-only format is a supported public release.

## 1. Problem and evidence to transfer

The predecessor's [integration test](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/tests/test_readmixer.py:197)
checks one small file, a line count, and a header prefix. It cannot establish
global mixing, mate integrity, key fidelity, or recovery. The audit's PY-19
and PY-20 concern this gap and the resulting documentation claims; every other
defect also needs a regression case here.

Use Rust's [paired acceptance tests](../../crates/readmixer-core/src/paired_acceptance_tests.rs),
[resource tests](../../crates/readmixer-core/src/paired_resource_tests.rs),
[BGZF interoperability tests](../../crates/readmixer-cli/tests/bgzf_restart.rs),
and [Plan 12](../plans/12-paired-mixing-validation-and-release-closure.md) as
scenario references. Do not copy passing status or private-host evidence.
[Rust Plan 13](../plans/13-fixing-four-blockers.md) demonstrates why release
scripts must build the exact candidate before trying to validate its binary.

## 2. Test organization and independent oracles

Keep tests in the Python repository, separate from the installed production
package. Organize parser/catalog, algorithm, crypto/storage, format, recovery,
resource/fault, CLI, and interoperability tests by their contracts. Preserve
the IDs declared by each numbered plan and map `AUD-01..AUD-24` to executable
regressions in a committed coverage manifest.

Build an independent small-fixture oracle using explicit symbolic pair identity
and complete expected normalized record bytes. It must not use the production
writer/parser to define its own expected result. Verify that both mates and
their private metadata follow the same permutation; byte-identical input reads
remain separate occurrences with distinct canonical ordinals.

Provide a streaming synthetic fixture generator, version 1. Derive its public
generator key as SHA-256 of UTF-8 `readmixer-python/fixtures/v1`. Use only
artificial identifiers and symbolic byte patterns, with no biological reference,
patient metadata, downloaded accession, or private source file. Generate large
files on demand rather than committing them.

Test-only root keys may be injected through the test harness. For shuffle
regressions, derive 32 fixture roots from SHA-256 of UTF-8
`readmixer-python/shuffle-test/v1/` followed by the canonical decimal counter
0 through 31. Tests also fix a public run identifier when comparing rendered
headers. Production configurations, commands, and environment variables expose
no fixed-root shortcut. Golden fixtures and reports mark these roots as public,
non-production test material.

## 3. Required correctness matrix

Run the following scenarios through mixing, full verification, indexed
recovery, and sequential recovery where those operations are applicable:

1. Two samples with equal counts; two with strongly unequal counts; three or
   more samples; a catalog containing empty samples; exactly one nonempty
   sample, which must be rejected by mix without a completion marker.
2. Repeated basenames in different directories, repeated original identifiers,
   identical record payloads at different ordinals, custom TSV filenames,
   recognizable unsupported lane/interleaved layouts, missing mates, duplicate
   physical files, symlinks, and input mutation detected by admitted identities.
3. LF, CRLF, accepted missing final newline, long headers and plus fields,
   non-UTF-8 accepted bytes, empty sequence/quality pairs, and literal CR bytes
   that canonical LF-only readers must preserve. Check exact digests and bytes.
4. Invalid markers, unequal sequence/quality lengths, inconsistent mate markers,
   count mismatch, record-size overrun, incomplete terminal records, gzip trailer
   corruption, and arbitrary `.gz` files. Errors must not silently truncate.
5. No refinement, one refinement, several sibling refinements, and depth at
   least two. Reach boundary limits through small injected internal fixtures
   without requiring enormous files. Depth exhaustion must fail explicitly.
6. Forced duplicate rank batches, collisions across checking windows, all-eight
   retry exhaustion, fixed stream layout/endianness vectors, and canonical
   identity overflow. No pair may disappear or be silently re-ranked alone.
7. Fixed-topology runs at admitted worker counts 1, 2, and 4, and applicable
   storage modes. Compare logical order and mappings. Require identical bytes
   for all six bundle artifacts when inputs, root key, run identifier, topology,
   codec build/level, artifact cap, and other byte-affecting settings are fixed.
   Compare manifests semantically excluding `execution`, whose worker, storage,
   and resource observations may differ. Reports and timings need not match.
8. Empty, boundary-sized, and incompressible BGZF blocks at levels 0, 1, 6, and
   9; forced level-zero fallback; record/key rows spanning blocks; `.gzi` and
   virtual-offset round trips through an independent HTSlib `bgzip` oracle.
9. Tiny allowed artifact caps that force multiple synchronized bundles, each
   reachable R1/R2/key limiting lane forcing rollover, an oversized singleton,
   and restored metadata large enough to change recovery part boundaries.
   Under the common cap, admitted key/FASTQ growth dominates index/sidecar growth;
   [Plan 15](15-workflow-boundaries-and-failure-acceptance.md) supplies the proofs,
   direct bound tests and hostile over-cap cases instead of impossible independent
   rollover fixtures. Check final physical sizes for every artifact, including
   indexes and receipts.
10. Recover one sample, several overlapping block occupants, all nonempty
    samples, and an empty sample. Multi-selection results must match separate
    recoveries byte-for-byte after decompression. Unknown/repeated selections
    fail before output creation. Test indexed corruption and explicit sequential
    recovery as distinct cases, not an automatic fallback.
11. Altered headers, tags, keys, indexes, sidecars, manifests, counts, positions,
    digests, paths, duplicate JSON keys, and contradictory lengths. Full verify
    must reject broken bijections even if counts alone agree. Original-input
    verification must reject a self-consistent pool for the wrong source.

Check the independent invariant after every successful case: each expected
canonical pair occurs exactly once, with correct mate roles, complete original
metadata, and matching sequence/quality bytes. Every sample's recovered ordinal
domain is exactly `0..pair_count-1`; do not substitute a count or probabilistic
checksum for exact coverage.

For the global-mixing regression, use 4,096 pairs split equally between two
samples and the 32 public roots above. Require the aggregate number of adjacent
sample transitions to be between 60,000 and 71,000 inclusive, and each root's
first 1,024 output pairs to contain between 256 and 768 pairs from sample zero.
These deliberately broad, fixed-fixture checks catch gross chunk/sample locality
and forced alternation. They are not a security proof, a balanced-window
production policy, or a statement about every possible random output. Freeze
thresholds before inspecting candidate results; investigate failures rather
than picking favorable roots. Also exhaustively check tiny ideal-random
permutation primitives against the independent oracle.

Add a separate `AUD-02` chunk-boundary regression with 100,000 pairs split
equally between two samples and fixture root zero. Number the canonical input
union from 0 through 99,999. At least 50,000 identities must reach an output
position whose `position // 32768` differs from their canonical input position's
quotient. This deliberately exceeds two predecessor-style contiguous chunks;
a correct within-chunk shuffle cannot pass merely by changing local order.
Keep this fixed-fixture regression separate from the production distribution
contract and from capacity-refinement tests.

## 4. Resource, fault, and publication matrix

Exercise budget admission below the operational minimum, at one-worker
capacity, and at parallel capacity. Charge and measure complete process-group
memory, including native arrays and completed compression tasks. Include data
whose normalized input size exceeds four times the admitted run budget.

On Linux, use a dedicated constrained host or container for memory/CPU quota
and affinity tests; distinguish cgroup hard limits from the application's own
admission policy. On macOS, test portable ceilings, spawn behavior, and
monitoring without presenting them as Linux cgroup guarantees. Both platforms
must exercise low descriptor limits, slow storage/backpressure, shared versus
separate scratch/output devices, and disk-space-query failure.

The fault suite must inject failures at actual exercised stages, including:

- Parse/decode after valid pairs have entered scratch.
- Corrupt AEAD headers, ciphertext, tags, counters, segment identity, completion
  seals, and authenticated-prefix truncation.
- Worker exception, unexpected worker exit, interrupted native work, SIGINT,
  SIGTERM, and uncatchable process termination.
- ENOSPC, short writes, compression failure, fsync failure, no-replace link
  collision, path replacement, and failure immediately before/after each
  publication boundary.
- Recovery failure in a later selected sample after earlier private outputs
  have already been produced.

Assertions must show that the injection was reached; passing an early argument
error does not test rollback. Pre-marker failures preserve the causal error,
join workers, remove only owned files where cleanup succeeds, and produce no
usable completion marker. A crash before marker publication can leave residue
but cannot make that incomplete set a committed pool or successful batch. Test
filename collisions without deleting or overwriting the other party's file.

Distinguish the final marker's atomic visibility from durable completion. A
failure syncing its parent after link creation may leave an indeterminate
on-disk state; report the error, do not claim guaranteed absence of the marker,
and use ownership-aware rollback where possible. Acceptance must match the
precise state machine in Plan 05 rather than promise impossible crash behavior.
After durable commit, a cleanup-only failure or later crash must preserve the
valid committed set. Exercise the warning/report contract and avoid turning
post-commit housekeeping into destructive rollback.

## 5. Performance evidence and acceptance

Follow [Plan 09](09-parallel-execution-and-performance.md)'s methodology. A
benchmark is meaningful only after correctness succeeds on the same candidate.
The baseline is the new complete serial workflow, not the predecessor's no-op
shuffle. Record old timings only as non-equivalent historical observations.

Measure at least a small startup-sensitive workload, balanced and imbalanced
multi-sample workloads, a larger-than-RAM external workload, and sparse versus
broad recovery selections. Use compressed and plain originals, small and long
metadata, and both compressible and incompressible artificial payloads. Test
memory, external, and automatic storage where each can be admitted legitimately.

For each measured configuration, perform one warm-up and at least five timed
runs in a controlled host configuration; preserve individual observations,
median, and variability. Report cold-cache observations separately when obtained
with a documented safe method; do not claim caches were cleared implicitly.
Record pair/read counts, logical/physical bytes, wall and phase times, CPU time,
admitted workers, process-group memory measurements, descriptor peaks, temporary
high-water bytes, selected recovery fraction, and bytes read per artifact class.

Any throughput improvement claim must identify the compared correct workloads
and observed effect. Unexplained material regressions require investigation;
disable an unhelpful optimization as the default rather than weakening
correctness or memory admission. Do not invent a mandatory Rust speed-parity
target or declare that multiprocessing must accelerate a one-pair fixture.

Before release, validate the conservative native workspace charges against the
tested dependency builds. If measured demand exceeds a lease, revise the
versioned resource profile and rerun the affected topology/determinism tests.
The memory coefficient is not accepted merely because it looks conservative.

## 6. CI, packaging, and release sequence

Use Linux and macOS runners, testing CPython 3.11 plus the highest stable CPython
version supported by the locked dependency set at implementation time. Publish
that explicit matrix. Include Linux x86-64 and macOS arm64 evidence; do not claim
an untested architecture just because a wheel may install there.

The ordinary CI lane runs unit/property tests, CLI subprocess tests, format
goldens, modest synthetic round trips, and documentation/link validation. A
separate resource lane runs constrained and fault scenarios. Full release hosts
run the larger data and benchmark matrix; CI success does not substitute for
unrun host evidence.

The release sequence is:

1. Resolve and record the supported dependencies and candidate source tree.
2. Build the wheel and source distribution once; hash both artifacts.
3. Install that wheel into fresh environments outside the source tree and run
   command/API, round-trip, schema, permissions, and interoperability checks.
4. Run resource, fault, statistical regression, and benchmark lanes against the
   same candidate. Record any changed package or dependency hash as a new
   candidate requiring affected checks again.
5. Reconcile help, README, API/security documentation, migration examples,
   supported platforms, and every claimed benchmark with the measured evidence.
6. Produce a bounded acceptance record with source/wheel/lock hashes, platform
   details, test IDs/results, commands, metrics, and unresolved limitations.
7. Mark Plan 10 complete only after every required lane passes. Publishing a
   package is a subsequent release action, not an automatic consequence of
   creating these planning documents.

## 7. Documentation and evidence privacy

Document the paired-only major-version boundary, recognized inputs, exact
normalization, bundle names, private mapping requirements, supported schema,
multi-sample selectors, recovery I/O scopes, and the 5,000,000,000-byte physical
cap. Explain that final mapping CSVs are recovery information rather than the
ephemeral cryptographic key used for scratch or pseudonymous tags.

Explain private final-format partial staging separately from encrypted algorithmic
scratch. State that memory mode is not a promise of locked/unswappable or
forensically erased process memory. Explain incomplete-set handling and that
old truncated keys cannot restore lost metadata. Document unsigned-manifest
trust and the continued sensitivity of underlying sequence content.

Public evidence may include artificial fixtures, aggregated counts, aliased
configuration, and timings. Real input paths, sample identities, reads, original
headers, recovery mappings, roots, and private normalized fingerprints belong
only in access-controlled local records. Real datasets are optional later
validation inputs, not a requirement to download or expose data for this plan.

Document how to retain and back up the complete private artifact set. The
manifest and lossless mapping keys are essential recovery material; the
ephemeral mixing key is not a substitute, and missing mapping rows cannot be
regenerated from the mixed FASTQs alone. Indexed recovery additionally requires
the declared indexes and sidecars. Explain the explicit sequential alternative
for index loss without presenting it as recovery from missing mapping keys.

## 8. Completion checklist

- [ ] Every `PY-01..PY-24` defect maps to an executed `AUD-NN` regression.
- [ ] Every numbered implementation plan's completion checks are satisfied.
- [ ] Serial and concurrent complete workflows preserve exact paired bijections.
- [ ] Successful, failed, and cancelled operations leave original input file
  contents unchanged; ownership cleanup never removes source or foreign files.
- [ ] Full verification and both recovery strategies pass independent oracles.
- [ ] Resource/fault tests reach the intended stages and enforce stated limits.
- [ ] HTSlib interoperability and physical caps pass on the final candidate.
- [ ] Linux/macOS package and support-matrix results are recorded.
- [ ] Benchmarks justify all published performance statements.
- [ ] Documentation, wire versions, CLI help, and private-file policy agree.
- [ ] Candidate fingerprints and acceptance evidence are complete, with no
  skipped required lane represented as passing.

The new release still does not provide legacy salvage, Rust interchange,
single-end/interleaved/multilane input, malicious wholesale-replacement
authentication, a database, or automatic incomplete-run resumption.
