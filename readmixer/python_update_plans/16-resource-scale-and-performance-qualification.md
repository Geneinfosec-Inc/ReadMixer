# Plan 16 — Resource scale and performance qualification

Status: **Plan 16 complete for the frozen dev10 local qualification scope**.
The dev9 preparation described in §9 supersedes the older partial tooling scope.
Section 8 retains the dev4 historical tooling record; §9 describes the expanded
qualification harness, and §10 records candidate-bound execution. Historical
[ownership-evidence.json](ownership-evidence.json) remains unchanged. Individual
completed lanes qualify only their recorded candidate, workload and host envelope;
they do not establish a universal resource ceiling or release completion.
[Plan 10](10-acceptance-documentation-and-release.md) remains the portable release
completion gate. All required local scenarios, including full private supplied-data
validation and six physical default-cap workflows, passed on dev10 (§12).
Private supplied inputs are never a public CI dependency.

Parents: [03](03-resource-admission-and-storage.md),
[04](04-global-pair-randomization-and-sharding.md),
[05](05-encrypted-scratch-and-safe-publication.md),
[07](07-complete-verification.md), [08](08-multi-sample-recovery.md),
[09](09-parallel-execution-and-performance.md), and Plan 10 §§4–6.
Mechanism dependencies are [11](11-shared-metadata-budget-and-lifetimes.md),
[12](12-paged-output-ownership-and-publication.md),
[13](13-spawn-workers-and-bounded-storage-handoffs.md), and
[14](14-deterministic-parallel-pipeline.md). Acceptance fixtures and failure
invariants come from [15](15-workflow-boundaries-and-failure-acceptance.md).

## 1. Baseline and phase ordering

Historical [pipeline-evidence.json](pipeline-evidence.json) records dev2's local
1,122 source and 1,122 installed-wheel passing tests. Its installed-package tiny
benchmark already performed one warm-up and five measured complete workflows:
five pairs, three samples including an empty sample, external storage, and a
requested CPU upper bound of two. It is real evidence, not an unrun script.
It does not qualify large workloads, memory coefficients, or throughput gains.

The [generator](../../scripts/generate_fixtures.py) and
[benchmark harness](../../scripts/benchmark_pipeline.py) retain public fixtures,
complete correctness checks, per-operation timing, sampled process-tree RSS/
FD/thread observations, and temporary-workspace cleanup. The historical dev4
increment added waited-child CPU accounting and optional fresh-process startup
lanes under explicit measurement scopes (§8). The dev9 harness adds component
measurements, operation-scoped artifact-class I/O, backpressure witnesses and
resource-charge calibration (§9). The historical dev2 benchmark predates these
tooling changes.

Perform this plan in two phases to avoid a candidate-freeze dependency cycle:

1. **16A — Prepare tooling and scenarios.** Implement measurement, fixture,
   independent-oracle and controlled-host harness changes; run small correctness
   checks. Finish candidate-affecting resource-profile revisions and Plans 11–15.
2. **17A — Freeze and install the candidate.** Follow
   [Plan 17](17-platform-packaging-and-release-closure.md) to build/hash the exact
   archives and isolated dependency environments.
3. **16B — Execute qualification.** Run the prepared workload matrix and private
   local task against that frozen installed candidate, recording all observations.
4. **17B — Reconcile evidence.** Close applicable gates only after this plan's
   required results and the platform matrix are available.

A measurement-driven code/profile/dependency correction returns to 16A/17A and
creates a new candidate. Never attach later results to replaced archive hashes.

## 2. Synthetic fixtures and independent correctness

Retain version-1 fixture bytes and the public generator key derived from SHA-256
of UTF-8 `readmixer-python/fixtures/v1`. Existing goldens in
[test_synthetic_tooling.py](../../tests/test_synthetic_tooling.py) must remain
reproducible. Add explicitly versioned profiles/configuration for varied record
lengths, many small samples, long opaque metadata and high-entropy allowed bytes.
The current `varied` profile uses a 16-symbol sequence alphabet and 64-symbol
quality alphabet; it must not be labeled an incompressible-payload workload.

Generate large fixtures by streaming; do not commit datasets or retain all pair
identities in the harness. Record counts, maximum/actual record lengths, newline
policy, input codec, metadata distributions, and observed compression ratios.
Use artificial identifiers and symbolic bytes only, without clinical reference
sequences, accession downloads or private records in performance fixtures.

Every successful measured run includes mix, complete verification, indexed
recovery and explicit sequential recovery for the declared selection. Compare
reconstructed normalized fields/order against an independent generator oracle,
not against production-written expected content. Preserve exact numeric coverage
checks in the pipeline. Empty samples and repeated payloads remain distinct
occurrences. Optional-original verification is an additional declared pass.

For small fixed-context comparisons use Plan 15's independent six-artifact
reference. For larger cases establish a correct admitted workers=1 baseline,
then compare admitted concurrent modes. Require exact artifact bytes only when
root/run ID, topology, codec build/level, cap and other byte-affecting settings
are fixed through private test seams. Changed topology requires preservation,
recovery and cap invariants, not identical permutations. No public seed is added.

## 3. Workload matrix and legitimate storage admission

Freeze workload definitions and expected comparisons before inspecting timings.
Cover these dimensions with a bounded named matrix and explicit rationale for
combinations; an unexplained omission is not a passing cell:

- Small operation-initialization and separate process/CLI-startup workloads.
- Balanced, strongly imbalanced, three-or-more-sample, and many-small-file inputs.
- Plain/gzip/mixed originals, short/long metadata, variable and raised valid
  record limits, empty reads, and high/low observed compressibility.
- No refinement, forced refinement, several sibling refinements, auto spill,
  and forced multi-pass external merge with witnessed execution counters.
- Sparse selection, overlapping selected block occupants, all nonempty samples,
  and selected empty companions in both recovery modes.
- Admitted workers 1/2/4, synchronous/shared compression, and relevant memory,
  external and auto modes. Record actual workers/slots rather than requested ones.

A successful larger-than-RAM lane must have independently known normalized input
bytes `D > 4 * B` for each operation's actual resolved aggregate budget `B`.
Record `D`, every actual `B`, and the inequality; comparing compressed source
size or requested RAM alone is insufficient. Choose `D > 4 * max(B)` for a
single full-workflow claim. Witness ciphertext spill/merge and cleanup.
External and forced-auto lanes must complete. Strict memory mode must either
complete within its legitimate admission or refuse with a typed error and no
algorithmic disk spill; do not count a refusal as a successful scale run.

Exercise small allowed caps for many artifacts and the default decimal cap's
large-lane rollover. Include a separately budgeted high-entropy host case near
the default physical file ceiling, reporting actual sizes and conservative
headroom. Arithmetic-only default-cap checks do not imply that physical host
case ran. Index/GZI domination is proved in Plan 15, not evaded with invalid
independent-limiting-lane fixtures.

## 4. Process-group and native-workspace qualification

Use a dedicated Linux host/container with recorded memory/CPU cgroup limits and
affinity. Test below operational minimum, serial capacity, and admitted parallel
capacity. On macOS measure portable ceilings/monitoring without describing them
as Linux cgroup enforcement. Record platform, architecture, kernel, Python,
NumPy/PyCryptodome/psutil/zlib builds, native-thread configuration and devices.

Calibrate parser buffers, packed arenas/offsets, rank arrays, lexsort workspace,
AEAD/compression workspaces, IPC copies, resident metadata, thread/process stacks,
and completed results awaiting ordered drain. Measure ownership-transfer and
parent/child overlap. Block the earliest work item to expose maximum retained
completion windows instead of sampling only an uncongested steady state.

Collect current and peak process-tree RSS plus platform-specific complementary
measurements where available. RSS summation can double-count shared pages;
sampling can miss short peaks. Record interval, observer overhead, errors, and
what is measured versus inferred. Capture CPU time from every worker, including
workers that exit before the final sample; coordinator `process_time()` alone
is not aggregate child CPU. Record thread/process/descriptor peaks and actual
native-thread activity, with before-import and operation-stage observations.
The historical tiny lane observed up to 34 threads at CPU budget two; that is
not proof of 34 busy CPUs, nor proof of native-thread qualification.

Measure temporary high-water bytes by artifact class and coexistence phase,
not just final directory size. Cover shared/separate scratch/output devices,
low descriptor limits, slow storage/backpressure, space-query failure, and
actual constrained scratch exhaustion. Distinguish application admission from
OS hard enforcement and kernel OOM behavior; never promise a universal RSS cap.
If demand exceeds a charged envelope, revise the versioned profile and rerun
its admission, deterministic-topology, fault and measurement cases on a new
candidate. A conservative-looking coefficient is not measurement evidence.

## 5. Performance protocol and instrumentation

For every required measured configuration, execute one identified warm-up and
at least five timed correct runs. Retain individual observations, median, range
and other justified variability summaries. Keep warm/cache-reuse results apart
from cold-cache results. Do not claim cache clearing without a documented safe
host procedure; never drop a shared machine's caches as an implicit benchmark step.

Measure parser, frame codec, bucket hash, ranks, lexsort, tag rendering, scratch
AEAD/codec, BGZF, indexed gather, sort merge and complete workflow phases. Separate
component measurements from integrated timings. Include process launch/import
startup in a dedicated end-to-end lane; retain the existing in-process baseline
under its narrower label. Identify fixture generation, validation and cleanup
included/excluded from every interval. Warm-up can trigger lazy imports.

Report logical/physical bytes, pair/read counts, selected fraction, wall and
aggregate CPU time, bytes read/written per artifact class, descriptor/scratch/
RSS peaks, actual workers/CPU/compression slots, and stalls/backpressure.
Add throughput only from observed bytes and measured intervals with clear units.
Record B/T, storage medium, codec level/build, quota/affinity and cache treatment.
A correctness or resource failure invalidates that performance cell; preserve
its failed observation rather than silently selecting faster successful runs.
No predecessor no-op shuffle baseline or assumed Rust parity target is valid.

## 6. Required full supplied-data local task

This project-specific follow-on explicitly requires one complete local validation
of the supplied dataset: **48 samples, 30,042,389 pairs and 60,084,778 reads** in
the historical inventory. Historical normalized bytes are **10,983,150,366**;
compressed bytes are **1,691,881,688**. Re-admit current inputs and compare private
identity/content observations; do not silently assume the historical snapshot
still matches. An observed inventory change needs an explicit recorded scope.

The complete ingress scan was dev1 input-only evidence with unchanged relevant
component hashes carried into dev2. The dev2 full-workflow evidence covers only
6,144 prefix pairs. Neither result completes this full local task. This private
lane is additional to portable synthetic release tests; absent private inputs
leave the local task incomplete and are never redistributed or required by CI.

Use the frozen installed candidate and a new private validation workspace outside
the input tree. Budget scratch, pool and recovery coexistence on their real
devices; preserve originals read-only. Run full mix, full verification, and both
recovery modes selecting the complete canonical sample set. Consume original
streams to EOF and validate final gzip trailers; prefixes cannot satisfy this.

Build an independent bounded stdlib-based original/recovered comparison in
canonical sample/part/ordinal order, preserving opaque bytes, CRLF normalization,
accepted final missing LF and standalone CR. Compare every normalized field and
numeric occurrence; retain aggregate counts and private digest cross-checks.
Do not use production parsing/writing as the only source of expected bytes.
Additional oracle passes are explicit validation passes, not permission for mix
to reread prefixes. Instrument the mix source pass separately.

Run indexed recovery, verify all its restored output, then clean only that
validation-owned recovery output before running sequential recovery. Retain the
pool until both comparisons pass. Account for this sequential disk schedule in
the evidence. Remove owned temporary fixtures/scratch/pool/restorations only
according to the validation workspace policy; never remove originals, foreign
files or an unrelated existing output directory. Report cleanup residue honestly.
Publish only aggregate counts, equality flags, resource observations and scoped
operation timings. Keep source names, records, mappings, exact private paths and
fingerprints access-controlled. This correctness run is not a private-data
throughput comparison and does not require five repetitions of the real dataset.

## 7. Planned scenarios and closure

These scenario IDs describe completion obligations. Sections 8–9 distinguish
historical and current tooling; §10 and the linked execution evidence record
which candidate-bound lanes have actually run. A scenario remains open while
any of its required lanes is incomplete.

- **P16-T01:** reproducible profiles, independent oracle and complete instrumentation.
  Parents `P09-T07`, Plan 10 §§2/5; `AUD-11/19/20`.
- **P16-T02:** complete `D > 4 * actual B` external/auto workflows and honest
  strict-memory refusal. Parents `P03-T03/T05/T07`, `P07-007`, `P08-008`,
  Plan 10 §4; `AUD-10/18/19/22/23`.
- **P16-T03:** many artifacts, default-cap large lanes, real device/descriptor/
  scratch limits and overlap. Parents `P03-T06`, `P06-004`, `P08-006/008`;
  `AUD-10/14/16/17/23`.
- **P16-T04:** native/process/IPC profile calibration and constrained hosts.
  Parents `P03-T07`, `P09-T01/T04`; `AUD-10/19/22/23`.
- **P16-T05:** full synthetic performance matrix, warm-up plus five runs per cell,
  cache separation and measured correctness. Parents `P09-T07`, Plan 10 §5;
  `AUD-11/17/19/20/22`.
- **P16-T06:** complete supplied-data local mix/verify/both-recovery task with
  independent equality, read-only inputs and owned cleanup. Parents
  `PY02-IO-001/002`, `P07-001/006`, `P08-001/007`, Plan 10 §7;
  `AUD-03/04/05/06/11/18/19/24`.

Completion requires executed candidate-bound results for every required scenario,
with failed, skipped and unavailable cells explicit and still open. Recording a
failed or unavailable required run does not complete its scenario. Preserve the
historical records unchanged. Pass synthetic release evidence and the separate
private local-task status to Plan 17B; neither status substitutes for the other.


## 8. Current 16A tooling increment and its limits

[generate_fixtures.py](../../scripts/generate_fixtures.py) preserves the existing
version-1 FASTQ byte goldens. Explicit `high-compressibility-v2` and
`low-compressibility-v2` profiles use a separate public version-2 field recipe;
`fixed` and `varied-v2` length modes admit lengths from zero through configured
maxima. Low-compressibility fields use the 254 allowed byte values excluding
CR/LF, with independent field domains and bounded expansion chunks. High-
compressibility fields repeat a public artificial symbol. The generator streams
records and reports actual record/sequence/metadata length observations, decoded
and physical bytes, and observed physical-to-decoded ratios. Those names and
ratios are measurements, not a guarantee of incompressibility. The original
version-1 `varied` alphabet is unchanged and retains its narrower interpretation.

[benchmark_pipeline.py](../../scripts/benchmark_pipeline.py) now emits benchmark
report version 2 while keeping the four complete-workflow phases and independent
recovered-byte/count checks. This is a tooling-report revision, not a pool-schema,
shuffle or production execution-profile revision. New instrumentation includes:

- Coordinator CPU plus a separate `RUSAGE_CHILDREN` delta for waited children,
  including children that exit between sampler observations. Live descendant
  CPU is sampled separately and is never added again to the waited-child total.
  Unsupported platforms report unavailable child accounting; unjoined children
  remain outside that aggregate. The sampler distinguishes OS-thread census
  from observed CPU activity between samples, without native-library attribution.
- Optional `--startup` lanes for a fresh bare interpreter, a fresh API import,
  and a real module-entry CLI deep inspection. Each lane retains one warm-up
  and at least five observations. Correctness failures retain their observations
  and suppress that lane's performance summary; timeouts kill and reap the
  harness-owned child. Child diagnostic contents are omitted from public reports.
- Fresh-child observations before the first ReadMixer import and after its
  operation, with RSS, lifetime peak RSS where available, OS-thread counts,
  descriptors and CPU. Parent launch time includes observer imports, sampling
  and waiting; the child import interval excludes earlier stdlib/psutil imports.
  Parent monitoring CPU is separate. The CLI lane is deep inspection, not a
  fresh-process complete mix/verify/recovery benchmark, and API imports are lazy.
- Imported-package and tool fingerprints, plus candidate-path agreement between
  parent and startup children. These identify the code actually observed; they
  do not substitute for Plan 17A archive hashes or isolated installation checks.

[test_benchmark_profiles.py](../../tests/test_benchmark_profiles.py) adds
independent version-2 field/length comparisons, expansion-boundary and observed
compressibility checks, invalid/empty configuration cases, scoped process/thread
observations, an exited-between-samples child-CPU witness, fresh-startup
correctness/failure/timeout cases, and a complete tiny version-2 workflow through
both recovery modes. Existing [test_synthetic_tooling.py](../../tests/test_synthetic_tooling.py)
coverage preserves version-1 goldens and exercises the benchmark CLI with startup
lanes. Focused tests and a tiny three-pair, 600-normalized-byte public synthetic
smoke run have executed with owned temporary workspace cleanup. Final integrated
validation and persisted candidate-bound evidence are recorded separately in
[ownership-evidence.json](ownership-evidence.json).

This is partial `P16-T01` preparation. It does not complete 16A or any 16B cell.
The full bounded workload matrix, benchmark integration/qualification using the
independent six-artifact reference implemented in dev5 under Plan 15, internal
microstage timers, complete per-artifact-class I/O/high-water observations,
stall/backpressure instrumentation, actual native-workspace/thread attribution,
and constrained-host resource calibration remain required. Sampled RSS/thread/
descriptor peaks can miss short events and include observer overhead; summed RSS
can double-count shared pages. No admission coefficient is qualified by these
samples. Larger-than-four-times-actual-budget external/auto runs, legitimate
strict-memory behavior, many-artifact/default-cap physical cases, platform runs,
and the complete supplied-data local task are still open. Keep the required
ordering **16A → 17A → 16B → 17B**; code or profile changes after measurements
require a new frozen candidate and renewed affected measurements.

## 9. Dev9 qualification preparation and exact execution boundary

The dev9 source archive freezes the following tools before host measurements.
Preparation exposed a fixture-validity defect in historical low-compressibility
v2 headers: arbitrary final metadata could resemble a conflicting mate marker.
Version 1 and version 2 bytes remain unchanged. The matrix uses the explicitly
versioned `low-compressibility-v3` recipe with its own public key/domain and a
terminal `~` header guard. Its fixed record overhead is 76 bytes, versus 75 for
prior versions; reported normalized sizes include the guard. The raised-record
cell witnesses an actual record above the default 1 MiB limit. Historical v2
results retain their original limited scopes.

Prepared tools:

- [resource_telemetry.py](../../scripts/resource_telemetry.py) observes inclusive
  component calls, actual artifact-class reads/writes, bounded worker aggregate
  reports, storage coexistence, admission peaks and ordered-result windows.
  Worker observations compose with private fault controls; missing worker reports
  fail measurement completeness. Discovery codec probes have a separate I/O class
  from the one forward source decode pass. Fine-grained clocks can be disabled
  for the full private task. Observer overhead and sampling limitations are explicit.
- [benchmark_pipeline.py](../../scripts/benchmark_pipeline.py) report version 3
  retains failed warm-up/measured phases and suppresses invalid summaries. It
  observes source/foreign-file preservation and scratch cleanup before harness
  deletion, cleans recovery outputs between modes, and optionally hashes the six
  artifacts under a private fixed context. No public deterministic seed exists.
- [benchmark_stages.py](../../scripts/benchmark_stages.py) runs independently
  checked parser, frame, hash, rank, lexsort, tag, AEAD/codec, BGZF, indexed gather
  and sort/merge components. [benchmark_fresh.py](../../scripts/benchmark_fresh.py)
  separately measures complete workflows in fresh interpreters, retaining imports,
  launch, operation, observer and cleanup scopes.
- [qualification_matrix.py](../../scripts/qualification_matrix.py) freezes named
  workload definitions and rejects absent measured witnesses, unsuccessful
  correctness, unproved actual-budget inequalities or missing observations.
  [qualification_controls.py](../../scripts/qualification_controls.py) implements
  private forced topology, merge, backpressure and storage-fault seams, restores
  hooks on exit, and records execution checkpoints.
- [qualification_host.py](../../scripts/qualification_host.py) uses invocation-owned
  systemd user cgroups, affinity and descriptor limits. Scratch exhaustion uses a
  private user/mount namespace; host-wide limits and caches remain unchanged.
  [run_qualification.py](../../scripts/run_qualification.py) binds actual cells to
  candidate/lock hashes and retains each result separately.
- [calibrate_resources.py](../../scripts/calibrate_resources.py) measures fresh
  component working sets and explicit versioned charges. Sampled RSS deltas are
  observations, never proof of a universal cap or an automatic qualification flag.
- [qualify_supplied_data.py](../../scripts/qualify_supplied_data.py) re-admits all
  supplied originals to EOF, performs the complete installed-candidate workflow,
  independently compares every normalized field and occurrence in both recovery
  modes, and uses private ownership snapshots for cleanup. Its durable private
  progress journal is diagnostic only; automatic resume is not implemented.

The [Linux CPython 3.11 dependency lock](../../requirements-qualification-linux-py311.lock)
covers exact runtime, test, build and HTSlib-oracle wheels. The locally built
psutil wheel is fingerprinted explicitly. This is one Linux x86-64 environment,
not a macOS/CPython 3.14 support claim. Archive hashes, isolated installation
checks, all measurement observations and any failed/unavailable cells belong in
`resource-qualification-evidence.json`, excluded from the archives it fingerprints.
Documentation updated after this freeze is a separately labeled documentation
snapshot. Historical dev0–dev8 archives and evidence remain unchanged.

The fixture matrix, microbenchmarks, calibration and fresh-process lanes require
one warm-up plus at least five observations. The full private task runs once.
No executed-cell success is asserted by this preparation section. P16-T01–T06
close only when their required candidate-bound results pass. Plan 17's remaining
platform matrix and Plan 10 release reconciliation retain their original scope.


## 10. Historical post-freeze dev9 execution snapshot

The exact candidate archives and local dependency lock are frozen. The
[dev9 execution record](resource-qualification-dev9-evidence.json) is outside those archives
and records subsequent measurements; this documentation is a later snapshot.
The wheel was installed outside the checkout without system site packages. A
separate wheel rebuilt from the frozen sdist has identical member and installed
package contents; its ZIP archive bytes differ, so no byte-reproducible build is
claimed.

The complete outside-checkout installed suite passed **2,537 tests**. The first
source run passed 2,533 and had four RAM-admission refusals during verification
fixture setup with eight available CPUs. The failed run is preserved; a targeted
78-test replay passed, and an unchanged full source replay with four available
CPUs passed all **2,537 tests** in 396.02 seconds and is recorded separately. This is not evidence that the original failing
fixture assertions executed successfully.

Component, fresh-process and resource-charge observations retain their warm-up
and five measurements. A separately hashed supplemental lane reuses the archived
Plan 15 independent six-artifact reference: 18 serial/worker configurations each
passed a warm-up and five observations, including full verification and both
recovery modes. These 14-pair fixtures are distinct from the 256-pair matrix's
cross-production artifact comparisons. Integrated component clocks are inclusive
wall times; CPU and artifact-class I/O totals have their separately stated scopes.
No per-stage aggregate CPU or stage-by-artifact I/O decomposition is claimed.

The compression supplement observed one and three actual service threads in
fresh children, held concurrent native codec workspaces, and retained the full
two/six-result windows before drain. All twelve observations passed independent
BGZF decoding, thread joins and zero remaining leases. RSS growth remained below
the declared thread and combined job charges in these observations. Linux smaps
and stack limits are complementary facts, not exact native stack attribution.

At this historical snapshot, large synthetic and complete supplied-data workflows
were running. The final dev9 record retains 150 matrix observations: 23 accepted
cells and two rejected cells (auto scale and the 8 GiB physical host). The later
physical/full-data attempts were interrupted for the correction in §11 and do
not complete their scenarios. Sampled charge probes are observations for this build;
they do not change the runtime's universal-envelope qualification flags. Plan 17's
remaining hosts and Plan 10 release reconciliation remain open.


### Host-envelope correction discovered during execution

The first physical-cap attempt generated 10,496,960,000 bytes inside an 8 GiB
cgroup. All six mix attempts then refused RAM admission before reading pairs;
there were no successful performance observations or OOM kills. A separate
bounded reproduction measured clean inactive file cache filling cgroup usage:
admission passed before fixture creation, refused afterward, and passed again
after deleting only that owned fixture. No system cache was dropped.

The physical-cap workload was retried in a separately declared 64 GiB host
cgroup with the same frozen candidate, fixture, 2 GiB application budget, CPU
partition and cap. The 8 GiB failure remains an unqualified host configuration.
The full supplied-data correctness run's owned host limit was raised from 8 to
32 GiB during mix, with before/after facts retained; its 2 GiB application budget,
CPU settings and candidate remain unchanged. That run has a recorded variable
host envelope and cannot qualify a constant 8 GiB environment. Both unfinished
attempts were subsequently interrupted for dev10; originals were unchanged and
their process trees reaped. Preserved uncertain residue is identified in the
historical record.


## 11. Dev10 correction after executed scale measurements

The dev9 external scale lane passed all six complete workflows; strict memory
refused cleanly with no spill. Auto mode completed each mix but failed verification
when an unsealed sorted merge output outgrew RAM before its planned spill. Dev10
adds a per-writer encrypted output policy and uses it for sorter runs already
required to spill; it does not mutate the shared store policy or relax strict
memory. The regression exercises a 128 MiB final run under a 96 MiB budget and
preserves authenticated ownership, resource accounting and fault cleanup.

Measured native LF/header scans retain the original byte grammar, exception
ordering and borrowed-view lifetimes, with bounded temporary state. Source
preparation measurements and tests are distinct from the newly frozen candidate.
The complete affected measurements must repeat on dev10; dev9's timings and
passes cannot qualify it. The physical-cap host default is 64 GiB to include
reclaimable file-cache headroom, separate from application B = 2 GiB. Explicit
constrained-memory lanes retain their declared limits.

[run_qualification.py](../../scripts/run_qualification.py) now fsyncs a private
exclusive observation journal after each completed workflow's owned cleanup.
A failed checkpoint prevents final success publication. Completed rows survive
later cooperative interruption; a truncated final row is not an observation.
This is evidence retention, never automatic resume. Journal writes occur between
iterations: they are included in runner elapsed time and excluded from operation
and workflow timing. Startup-only measurements have their separate report scope.

Dev9's unfinished long physical/full-data invocations were cooperatively stopped
for candidate supersession. Their available observations and cleanup/residue
status remain historical; unreported in-memory physical iterations cannot be
reconstructed or credited. All affected lanes required new dev10 execution;
the completed candidate-bound results are recorded in §12.


## 12. Completed dev10 local qualification

[Current candidate evidence](resource-qualification-evidence.json) records 2,703
passing source tests and 2,703 passing installed-wheel tests, 270 passing
supplement observations (45 configurations, warm-up plus five each), and 25
accepted synthetic matrix cells with 150 reconciled observation-journal rows.
The external and corrected auto lanes completed D = 2,204,344,800 bytes with
actual B = 536,870,912 in every operation. Strict memory produced six clean typed
refusals with no algorithmic spill. Fixed-context artifact comparisons match
across all 42 balanced and 12 short-read observations. Actual host/fault/device/
descriptor/backpressure witnesses and every failed attempt remain recorded.

`P16-T06` is complete on the frozen candidate: all 30,042,389 supplied pairs were
mixed, fully verified and restored through both modes with bounded independent
field/order/EOF comparisons. All actual budgets were 2 GiB and D exceeded 4B.
Exactly 96 ingress opens read 1,691,881,688 physical bytes with zero seeks; extra
oracle passes are labeled separately. Originals were unchanged; owned validation
outputs were removed and workers reaped. Public records contain aggregate facts;
private paths, names and fingerprints remain access-controlled. The 7h54m40s
correctness run is not a private-data throughput comparison.

Storage provenance observes shared ext4/NVMe nonrotational backing for originals,
ordinary scratch and output; the distinct-device lane used RAM-backed tmpfs,
not a second physical disk. No cache clearing or exclusive-device timing claim
is made. Fresh-script isolated-bootstrap failures are retained independently of
the corrected exact-archive invocation. Calibration remains scoped to observed
builds and measurements; it does not assert a universal RSS bound.

The physical default-cap cell passed its full predeclared warm-up plus five
measured workflows. Each reached two bundles and a 4,983,858,306-byte maximum
data artifact below the 5,000,000,000-byte cap. Final reports, all six journal
rows, host controls, worker exits and owned cleanup were reconciled. Across the
25 matrix cells, 126 observations completed full workflows and 24 produced the
expected typed resource refusals; refusals have no successful throughput claim.

`P16-T01` through `P16-T06` are complete within the recorded Linux x86-64 /
CPython 3.11.7 candidate/build/host envelopes. No Plan 16 implementation or local
execution obligation remains. The preserved dev9 failures and interrupted runs,
and the corrected dev10 bootstrap failures, remain separately attributable.
Plan 17's remaining platforms and Plan 10 release closure stay open. These later
documentation results do not modify or rebuild the frozen qualification archives.

### Completed performance observations and their scope

All figures below use five measured complete workflows after one identified
warm-up. Every workflow includes mix, full verification, both recovery modes,
independent correctness comparison and owned cleanup. Raw phase timings,
per-artifact I/O, resource samples, host settings and each observation remain in
the hash-indexed matrix evidence; aggregate medians do not replace those records.

For the 100,000-pair short-read fixture (87,800,000 normalized bytes), CPU 1's
median workflow was 86.264 s (85.925–87.118 s), while CPU 4's was 110.310 s
(109.797–110.917 s). This workload showed no parallel speedup on the recorded
host. The D > 4B external lane's median was 180.983 s (178.735–187.876 s), and
the corresponding auto lane's was 184.088 s (183.328–185.336 s). Strict-memory
refusals are acceptance observations, never successful throughput measurements.
The physical-cap lane's median was 9,789.337 s
(9,779.149–10,163.106 s); all five measured workflows passed.

These are cache-reuse, shared-host observations with explicitly partitioned CPUs;
other qualification processes overlapped on shared storage. They are not cold-
cache results, exclusive-device comparisons, cross-platform promises, or evidence
of a universal parallel speedup. Integrated stage clocks are inclusive wall time;
CPU and artifact-class I/O have operation scope, not per-stage attribution.
