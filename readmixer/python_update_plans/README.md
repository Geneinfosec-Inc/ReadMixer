# Python ReadMixer modernization plans

Status: Implementation specifications and candidate-specific acceptance records.
Plans 11/12 and Plan 13 worker infrastructure are implemented locally. Plan 14
production integration is implemented. Plan 15's local acceptance matrix is
implemented in `2.0.0.dev8`. Executed Plan 16 qualification exposed an auto-sort
scale failure; dev10 corrects it and completes all required local Plan 16
qualification on new frozen candidate hashes, including the full supplied-data
task. Plan 17 and the Plan 10 release-completion gate remain open.
Plan 17 now has [executable qualification tools and an operator handoff](../../tools/release/README.md).
See [release-evidence.json](release-evidence.json) for exact candidate/host attempts
and [Plan 17 §10](17-platform-packaging-and-release-closure.md#10-executable-handoff-and-remaining-closure)
for the evidence needed to close the remaining gates.

The current implementation provides paired mixing, complete verification and
indexed/sequential recovery with admitted production workers, direct scatter,
canonical terminal delivery and shared bounded sorting/compression. See
[implementation progress](IMPLEMENTATION.md) for actual behavior, results and
remaining work. Historical dev2–dev8 evidence and archive hashes stay attached
to their original candidates. [workflow-acceptance-evidence.json](workflow-acceptance-evidence.json)
records dev8 local acceptance; [resource qualification evidence](resource-qualification-evidence.json)
distinguishes dev9 historical execution from renewed dev10 qualification.
[Dev9 execution history](resource-qualification-dev9-evidence.json) retains its
completed, failed and administratively interrupted lanes.
Each result belongs to its exact archives and isolated dependency lock; later
documentation does not change an archived candidate's evidence.

The implementation target is the existing Python repository at
`/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/`. The Rust working tree at
`/home/user/cursor/readmixer_rust/` supplies design references. These plans do not
require a Rust executable or make Python pools compatible with Rust artifacts.

## Objective and accepted scope

Replace the predecessor's independent-record chunk pipeline with a Python-native
paired-sample engine. Two or more nonempty samples enter one global permutation
of intact R1/R2 pairs. Sharding, position swaps, temporary storage, output
splitting, verification, and recovery must preserve the same pair identity.
Recover one or several complete samples in original order using the new pool's
private metadata, without the original input files.

The accepted first-release boundary is:

- Linux and macOS; 64-bit CPython 3.11 or later within the tested support matrix.
- One R1/R2 file pair per sample; single-end, interleaved, and multilane input
  are deferred and must fail clearly when recognizable.
- A new Python-specific versioned pool format. No legacy Python pool salvage,
  Rust-v2/v3 interoperability, database backend, or incomplete-run resumption.
- Complete original header and plus-line metadata in private final mappings;
  authenticated encryption for on-disk algorithmic scratch.
- Normalized exact restoration: CRLF becomes LF and the final newline is
  present. Original gzip container bytes are not reproduced.
- Established compiled libraries are allowed. Core dependencies remain NumPy,
  PyCryptodome, and psutil; Python orchestrates the algorithm. BGZF uses stdlib
  zlib and reviewed format framing. Charting is an optional extra.
- Swaps mean changing intact pairs' positions, not exchanging mates between
  samples or replacing a sample inside an already committed pool.

## Documents and implementation order

Read [00 — Audit and transfer decisions](00-audit-and-transfer-decisions.md)
first. Its defect IDs connect the predecessor's failures to remediation plans.

1. [01 — Packaging, interfaces, and compatibility](01-packaging-interfaces-and-compatibility.md)
   establishes the new package, command/API surface, error contract, and
   deliberate compatibility break.
2. [02 — Paired input and lossless parsing](02-paired-input-and-lossless-parsing.md)
   defines the sample catalog, input grammar, mate checks, and normalization.
3. [03 — Resource admission and storage](03-resource-admission-and-storage.md)
   and [05 — Encrypted scratch and safe publication](05-encrypted-scratch-and-safe-publication.md)
   establish the bounded storage and transaction foundations.
4. [04 — Global pair randomization and sharding](04-global-pair-randomization-and-sharding.md)
   implements the canonical serial shuffle on those foundations.
5. [06 — Bundles, keys, indexes, and manifests](06-bundles-keys-indexes-and-manifests.md)
   fixes the complete new artifact contract and synchronized emission.
6. [07 — Complete verification](07-complete-verification.md) and
   [08 — Multi-sample recovery](08-multi-sample-recovery.md) complete the
   independently checkable and reversible serial workflow.
7. [09 — Parallel execution and performance](09-parallel-execution-and-performance.md)
   adds bounded concurrency after the serial correctness baseline exists.
8. [10 — Acceptance, documentation, and release](10-acceptance-documentation-and-release.md)
   is the sole release-completion gate for this Python modernization.

Plan numbers identify ownership, not a requirement to implement unrelated
foundations serially. Plans 03 and 05 may proceed together after the input and
public-interface contracts are fixed. Plans 07 and 08 share validated readers
and bounded sort primitives but require independent test oracles.

## Follow-on plans 11–17

These plans are implementation handoffs against the dev2 baseline. Plans 00–10
retain their contract ownership; Plan 10 remains the sole release-completion
gate. The follow-ons name unfinished work rather than replacing existing tests
or restarting implemented components.

1. [11 — Shared metadata budget and lifetimes](11-shared-metadata-budget-and-lifetimes.md)
   now implements one resident-metadata accounting dimension and cross-owner
   lifetimes in dev3. Host calibration and downstream integration remain open. Parent: 03; audits: `AUD-10`, `AUD-22`, `AUD-23`.
2. [12 — Paged output ownership and publication](12-paged-output-ownership-and-publication.md)
   now implements authenticated inventory/link journals and publication cleanup
   locally in dev4, including bounded pending ownership and retryable drains.
   Requires 11. Parents: 03/05; audits: `AUD-10`, `AUD-12`–`AUD-14`, `AUD-17`,
   `AUD-23`.
3. [13 — Spawn workers and bounded storage handoffs](13-spawn-workers-and-bounded-storage-handoffs.md)
   now has memory/segment grants, paged task ownership, reconciled disk credits,
   startup retry and bounded worker lifecycle. Plan 14 now supplies production
   integration; platform/resource qualification remains open. Requires 11/12. Parents: 03/05/09; audits: `AUD-10`, `AUD-12`–`AUD-14`,
   `AUD-17`, `AUD-22`, `AUD-23`.
4. [14 — Deterministic parallel pipeline](14-deterministic-parallel-pipeline.md)
   now implements one-pass LPT parsing, admitted 64-writer scatter, canonical
   terminal delivery, bounded bulk tags and verification/recovery sorting locally. Requires 13. Parents:
   02/04/06–09; audits: `AUD-01`–`AUD-03`, `AUD-08`, `AUD-10`–`AUD-12`, `AUD-15`,
   `AUD-18`, `AUD-22`, `AUD-23`.
5. [15 — Workflow boundaries and failure acceptance](15-workflow-boundaries-and-failure-acceptance.md)
   completes the bounded local boundary/adversarial/resource/signal matrix,
   including independent serial/worker artifact oracles, exact metadata/inventory
   limits, public scope checks, real signals and acknowledged crash residue.
   Supported-platform execution remains with 17; scale/performance belongs to 16.
   Worker cases use 14 and ownership cases use 12.
   Parents: 01/05–10; all applicable audit mechanisms remain traceable.
6. [16 — Resource scale and performance qualification](16-resource-scale-and-performance-qualification.md)
   has complete qualification tooling and dev10 corrections driven by executed
   scale tests. All 25 matrix cells, 270 supplement observations and the full
   private supplied-data task pass; Plan 16 is complete locally. Mechanism
   qualification requires 11–15; measured runs use the candidate from 17A.
   Parents: 03/09/10; especially `P03-T07`, `P09-T07`.
7. [17 — Platform packaging and release closure](17-platform-packaging-and-release-closure.md)
   prepares an immutable candidate (17A), executes locked platform lanes, and
   reconciles release evidence (17B). Parents: 01/10 and every parent's platform
   gates; especially `AUD-19`, `AUD-20`.

### Execution order and candidate milestones

The mechanism sequence is **11 → 12 → 13 → 14**. Plan 15's existing-serial cases
and Plan 16's measurement tooling may proceed alongside that sequence. Complete
mechanism integration, applicable Plan 15 regressions, and measurement tooling
before freezing the candidate under **17A**. Dependency locking and CI
preparation may be developed earlier; candidate freeze is a distinct milestone.

Run Plan 16's measurements and Plan 17's platform qualification against those
exact artifacts, then perform **17B** evidence reconciliation. If calibration
or a failure requires source, packaging, or dependency changes, produce a new
candidate at 17A and rerun affected lanes. This is an explicit validation loop,
not a circular implementation prerequisite.

The complete supplied-data run in Plan 16 is a requested local task. It remains
separate from redistributable synthetic fixtures and public CI. Record its full,
prefix-only, failed, or unrun scope explicitly; missing local results cannot be
reported as a completed local task or inferred from a synthetic run.

### Gap classification and traceability

Each follow-on separates **missing implementation**, **missing regression
coverage**, and **implemented but awaiting qualification**. It supplies current
code/test anchors, concrete interfaces and ownership decisions, failure behavior,
`P11-Txx` through `P17-Txx` acceptance scenarios, parent scenario/audit mappings,
and a completion checklist.

Scenario IDs identify obligations rather than pytest nodes. Plans 11/12 and
Plan 13 infrastructure and Plans 14/15 have collected local acceptance;
Plan 16 has completed tooling regressions and every required local candidate-bound
qualification workload. Remaining platform/release gates belong to 17/10. Add
executable nodes to [regression-coverage.json](regression-coverage.json) only
when the tests exist; retain actual execution results in candidate evidence.
The current audit mappings remain partial. Platform qualification applies to
all original scenarios, including ones already covered locally.

The remaining-work ownership is exhaustive at the contract level: shared
metadata belongs to 11; final-file ownership to 12; process grants/IPC/lifecycle
to 13; deterministic production integration to 14; workflow boundary/adversarial
coverage to 15; measured resources, full local data and performance to 16; and
candidate/platform/documentation/release reconciliation to 17. Plans 15–17 also
carry the outstanding input/API/format scenarios whose runtime mechanisms
already exist. A new defect must acquire an explicit owner before closure.

## Contract ownership

Resolve disagreements by correcting the documents before implementation, not
by silently selecting different defaults in individual modules:

- Plan 01 owns package names, public configurations, CLI spelling, exit codes,
  and compatibility policy.
- Plan 02 owns sample identities, input path rules, mate grammar, accepted bytes,
  and normalized-record accounting.
- Plan 03 owns resource ceilings, metadata limits, storage modes, and admission.
- Plan 04 owns private pair framing, shard paths, randomization, and determinism.
- Plan 05 owns key derivation, scratch authentication, publication, and cleanup.
- Plan 06 owns public artifact bytes, names, indexes, hashes, and physical caps.
- Plan 07 owns full-verification claims; Plan 08 owns selective-recovery claims
  and batch completion.
- Plan 09 owns worker scheduling and benchmark methodology; Plan 10 owns the
  release evidence required to make claims about the resulting implementation.

Every numbered plan contains source references, concrete work, failure cases,
acceptance scenarios, and completion criteria. Plans 00–10 retain historical
baseline evidence and specifications; many of their proposed modules now exist.
Plans 11–17 identify current symbols separately from interfaces still to be
implemented. Historical predecessor/Rust anchors are design provenance, not
claims about current Python paths or execution results.

## Shared engineering decisions

Use a fresh 256-bit OS-generated secret for each operation and a separate opaque
run identifier. No production seed option or serialized root key is introduced.
The original pair identity is a dense numeric sample index plus its zero-based
ordinal. Both mates and all private restoration fields travel together.

The shuffle uses common keyed buckets and recursive refinement, followed by a
cryptographic 128-bit-rank permutation within each bounded terminal. The initial
limits are 64 root buckets, power-of-two recursive fanout up to 64, depth 32,
32,768 pairs per terminal, and 64 MiB of encoded pair-arena bytes. The complete
working set must also fit the canonical lease from Plan 03. Worker scheduling
must not determine that topology. Natural sample imbalance and same-sample
streaks are valid; quota balancing would change the distribution.

Each output bundle has R1 and R2 BGZF FASTQs, two standard `.gzi` sidecars, one
private BGZF mapping CSV, and one private physical-order recovery index. The
manifest commits the whole pool. Recovery scans the indexes once for the union
of requested samples, gathers selected data, and sorts complete pairs back into
original sample order. One receipt commits the entire recovery request.

The default physical file ceiling is **5,000,000,000 bytes**, not 5 GiB. It
applies to mappings, indexes, sidecars, manifests, and recovery artifacts as well
as FASTQs. A pair that cannot fit an empty bundle is an error.

Final mappings and manifests are private files, not an encrypted keystore.
Possession of the complete recovery material exposes all represented mappings;
sample selection is not an authorization boundary. Pseudonymous identifiers do
not make the underlying sequence content anonymous. Unsigned hashes bind data
to the supplied manifest, not to a trusted external identity.

## Evidence and provenance

The inspection snapshot is [source-snapshot.json](source-snapshot.json), captured
on 2026-09-21. It records 15 Python files and 104 Rust files, their exact-byte
SHA-256 fingerprints, selected-path Git state, and a digest of tracked changes
from HEAD. Untracked source files are covered by their individual fingerprints.
No input, mixed, or recovered datasets are included.

- Python HEAD: `6454d21b4566d34e7dbcfadedb6a6f3e412f5beb`.
- Rust HEAD: `a886706254786b8af4980c5e5afbe990658e89dc`.
- Python selected-content manifest SHA-256:
  `08c6276d684e05bdc4c46d0d7312e3082d34e8934d943d38d0db6c96e22d9fb2`.
- Rust selected-content manifest SHA-256:
  `63d3ce97bfc0a9a569f7948d3933f393933ee20457dcb38291f9f4c5f472b9a6`.

The Rust recommendations concern its inspected **working tree**, including
uncommitted changes, not just that commit. A file fingerprint establishes what
was inspected, not that every function was executed or every behavior proven.
Recheck changed source anchors before implementing a dependent plan.

Rust [Plan 12](/home/user/cursor/readmixer_rust/docs/plans/12-paired-mixing-validation-and-release-closure.md)
still withholds final paired-release qualification. Its
[Plan 13](/home/user/cursor/readmixer_rust/docs/plans/13-fixing-four-blockers.md) describes release-plumbing
prerequisites, and [Plan 14](/home/user/cursor/readmixer_rust/docs/plans/14-adding-database-to-readmixer.md) is a
database proposal. None supplies completed Python acceptance evidence.

### Historical candidate evidence

[foundation-evidence.json](foundation-evidence.json) and
[storage-evidence.json](storage-evidence.json) describe dev0/dev1.
[pipeline-evidence.json](pipeline-evidence.json) records the exact dev2 candidate:
1,122 source tests and 1,122 installed-wheel tests passed, the supplied-data
prefix passed all four workflow operations, and a tiny installed-package
benchmark completed a warm-up and five measurements. The local wheel environment
reused system-site dependencies; it was not the isolated locked release matrix.

The preserved dev3 [metadata evidence](metadata-evidence.json) adds 1,233 source
and 1,233 installed-wheel passes plus a repeated bounded supplied-data prefix.
Dev4 uses a separate [ownership evidence record](ownership-evidence.json).
Dev5 uses a separate [spawn evidence record](spawn-evidence.json), covering the
partial private worker runtime and independent serial oracle. Public processing
stages remain serial with compression threads; worker tests do not close Plan 14.

Preserve those records and their archive hashes unchanged. Adding Plans 11–17
and updating this index/status/coverage prose changes files included in dev2's
source manifest. That historical manifest therefore describes its original
candidate rather than the current documentation tree. It must not be silently
rehash-updated or presented as fresh test evidence. Future candidates require
new source/archive/lock fingerprints and actual results under Plan 17.

## Working rules for implementation

Implement in the Python repository; keep these design documents and their
completion evidence linked from this directory. Preserve the existing Rust
working tree. Do not revive the legacy chunk engine behind the new interface.

Keep plan status `Planned` until actual code and the plan's tests exist. Record
measured results with the candidate source and dependency versions. Do not mark
the release complete because documents, examples, or individual unit tests are
complete. Performance claims must compare equivalent, correct workflows.

Dev6 uses separate [worker handoff evidence](worker-handoff-evidence.json).
Historical dev5 evidence and archive hashes remain unchanged.

Dev7 uses preserved [parallel pipeline evidence](parallel-pipeline-evidence.json).
Dev8 uses separate [workflow acceptance evidence](workflow-acceptance-evidence.json),
including source and outside-checkout installed-console validation. These local
results do not replace the locked candidate and host matrix under Plan 17.

Dev9 prepares Plan 16's executable qualification matrix, complete private input
validator, component/fresh-process measurements and isolated Linux host runner.
[Plan 16 §9](16-resource-scale-and-performance-qualification.md#9-dev9-qualification-preparation-and-exact-execution-boundary)
describes the frozen preparation boundary. Executed results are candidate-bound;
no preparation document alone closes P16-T01–T06 or Plan 10.
