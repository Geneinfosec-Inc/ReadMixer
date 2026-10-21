# Python implementation progress

Updated: 2026-09-30. Current implementation: `2.0.0.dev10`.
Plans 11/12, Plan 13 worker infrastructure, and Plan 14 production integration are
implemented locally. Plan 15's bounded local acceptance is implemented. Plan 16
is complete for the frozen dev10 local qualification scope: all 25 synthetic
matrix cells and the full supplied-data task pass. Plan 17's remaining supported
platforms and Plan 10 release closure remain open. Plan 17 now has executable
[release tooling and a host/evidence handoff](../../tools/release/README.md);
[release-evidence.json](release-evidence.json) records exact new attempts. Historical dev2–dev8
evidence is preserved unchanged. The historical
[post-dev5 handoff](POST_DEV5_WORKER_ACCEPTANCE.md) is superseded by the current
inventory; its remaining infrastructure work is implemented.

Public `mix`, complete `verify`, and multi-sample `recover` now execute the new
paired workflow. Indexed and explicit sequential recovery restore complete
selected samples without original input files or the ephemeral mixing key.
Read-only shallow/deep `inspect` remains available. These are implemented
operations, not stubs; they are **not a release-qualified implementation of every
plan requirement**. Plans 11/12, Plan 13 infrastructure and Plan 14 are locally implemented;
Plan 15 has local acceptance and Plan 16 has completed local resource/performance
qualification. Remaining platform/release gates belong to Plans 17/10. The Rust
tree and supplied originals have not been changed.

This document separates functional implementation, concrete local tests, and
unmet completion gates. [regression-coverage.json](regression-coverage.json)
maps every `AUD-01..AUD-24` obligation to exact collected pytest node IDs and
states what remains. Every audit mapping is partial: a passing subset does not
close its entire scenario/platform matrix.

## Implemented behavior by plan

### 00 — Audit and transfer decisions

The paired replacement follows the inspected audit and its deliberate
compatibility break. The predecessor's no-op shuffle is not an oracle or a
supported alternate engine. Audit traceability now includes shuffle, formats,
verification, recovery, resource faults, and CLI workflows rather than only
input/storage foundations. The original source snapshot remains historical
provenance; it is not a fingerprint of this candidate.

### 01 — Packaging, interfaces, and compatibility

The `src/readmixer_py/` package, `readmixer-py`, module invocation, and old-location
forwarder share one typed command/API contract. Frozen configurations, explicit
byte units, lazy imports, typed causal errors, and bounded aggregate CLI reports
are implemented. Help/version avoid native-library imports. Legacy flags and
unsupported pool formats fail explicitly. Reports distinguish pool verification,
verification against supplied originals, and each recovery I/O scope.

Real command tests exercise mix, verify, and both recovery modes. Failure never
prints a successful operation report. SIGINT/SIGTERM retain their cancellation
status; an interruption after durable commit preserves and identifies the
committed marker. Ordinary postcommit cleanup faults return committed results
with bounded cleanup warnings. Current implementation modules are `mixing.py`,
`verification.py`, and `recovery.py`, avoiding collisions with the lazy public
function names.

### 02 — Paired input and lossless parsing

Anchored discovery and strict sample sheets produce canonical sample identities
and reject unsafe paths, duplicate physical files, ambiguous naming, recognizable
unsupported layouts, and observed file replacement. A complete TSV round trip
with identical leaf filenames in distinct sample directories confirms separate
source IDs and exact sample-specific content through verification against
originals and both recovery modes. The bounded byte parser
validates complete paired records, mate roles/counts, gzip members/trailers,
record limits, and actual normalized-byte limits. It preserves opaque header and
plus metadata, normalizes CRLF once, accepts valid missing final LF, and preserves
standalone CR payload during canonical rereading.

Mix parses each admitted sample once in a serial forward pass, accumulating
counts and normalized digests while scattering complete pairs. Production-path
spies exercise plain/gzip/mixed mates across multiple output bundles and assert
one decoding open/decoder per original, no seeks, and exact physical/decoded
byte counts and digests, without rereading source data to build the oracle. It
requires two nonempty samples; empty samples remain represented in successful larger pools.
Deep inspection can successfully report a valid but mix-ineligible catalog.

### 03 — Resource admission and storage

Shared byte and descriptor leases cover parser/frame buffers, sort and terminal
workspaces, protected storage, metadata, and compression tasks/results. Detected
host/cgroup ceilings clamp configured RAM; runtime pressure checkpoints limit
new admissions without changing canonical topology. Disk reservations query the
actual anchored filesystem, group same-device growth, and reserve write/spill
overlap before mutation. Query failure and counter overflow fail closed.

Memory/external/auto storage, authenticated paged ledgers, bounded multi-pass
external sorting, and exact identity coverage are integrated. RAM-only mode
refuses algorithmic disk spill. Auto spill authenticates durable ciphertext
before releasing its RAM source. Sort-run and recursive-frontier ownership
survive interrupted ledger handoff/deletion. Cleanup reserves reader capacity
in advance, including cancellation with otherwise saturated resources.

Resident metadata now shares one enforced `min(64 MiB, floor(B/16))` ceiling
inside aggregate RAM admission. Atomic acquisition, resizing, splitting, transfer
and release update RAM, metadata and descriptors together. Conservative charges
cover the accounting registry itself, catalogs, disk/store/ledger registries,
identity coverage, pool metadata, output descriptors, selectors, reports and
serialization overlap. Blanket multiples of complete metadata allowances have
been removed. Catalog record aliases retain their owner after source close;
pool and emitter codec close preserve retained metadata until disposal/return.

Strict memory charges resident registry pages; auto/external storage releases
resident source charges only after authenticated handoff. Registry-purpose
writers use bounded 64 KiB buffers. Existing sample/file/encoded-document limits
remain independent and can be reached only when shared admission also permits.
Transaction ownership now uses authenticated pages under this same ceiling.
At most 64 active/unregistered handles and a separate completion marker remain
resident; the encoded metadata and overall 100,000-file caps remain independent.

These are admission envelopes and bounded checkpoints, not measured proof that
every supported build stays below a universal process-group RSS ceiling.

### 04 — Global pair randomization and sharding

The canonical serial shuffler implements checked PYPAIR1 frames, the exact keyed
BLAKE2b identity transcript, 64 root buckets, recursive power-of-two refinement,
and canonical numeric leaf order. Terminals enforce pair-count, packed-arena,
and complete working-set limits. Native NumPy lexsort orders bulk ChaCha20
128-bit ranks; complete batches retry on collisions, with eight-attempt failure
instead of a biased tie breaker. Full-width AES identity tags and exact coverage
preserve every pair occurrence, including duplicate original header text.

Production admission selects LPT parser workers and complete 64-writer scatter
sets where resources permit. Whole-set batch rotation bounds producer retention;
encrypted or raw handoffs retain ownership until acknowledgment. The coordinator
keeps a paged inventory and canonical depth-first traversal. The bounded sorter
scatter remains the low-resource fallback. Fixed T, identity ordering and
bucket/rank transcripts do not depend on concurrency or batch boundaries.

Independent fixed-key order/rank vectors, recursive refinement, boundary/depth
faults, collision retries, and all storage backends are tested. A separate tiny
exhaustive oracle compares the production rank permutation with an independent
Fisher–Yates choice tree, including conditional collision rejection and final
retry distributions. The prescribed
32-root/4,096-pair distribution regression and separate 100,000-pair predecessor
chunk-boundary regression exercise the actual shuffler. They detect gross
locality defects; they are not anonymity or security proofs.

### 05 — Encrypted scratch and safe publication

Fresh OS operation roots/run IDs, explicit HKDF contexts/domains, AES identity
tags, and exact RMPYTMP1 authenticated segments have independent vectors.
Algorithmic disk scratch uses ChaCha20-Poly1305 with bounded decoding and checked
completion seals. Expected segment identity, inode, size, authentication, and
logical counters are checked; detached references and ledger roots retain
explicit ownership. No production seed option or serialized root key exists.

Final-format partials are private plaintext files in a private destination-side
stage. Closed artifacts are independently size/hash checked and synced before
no-replace hard links. Artifacts precede the final manifest/receipt; directory
sync separates marker visibility from durable commit. Actual write/link/fsync,
replacement, cancellation, and cleanup faults test inode-aware rollback,
indeterminate durability, and foreign-file preservation. Hostile FIFO replacements
are rejected without blocking. After durable commit, cleanup never removes the
valid output set, including when interruption propagates.

Transaction ownership now uses authenticated inventory/link pages in the shared
operation store. Active handles, pending handoffs and cleanup cursors have bounded
shared-metadata charges. Data scratch closes before publication; ownership pages
survive until commit/rollback cleanup. Manifest/receipt representations retain
their independent resident and encoded limits.

### 06 — Bundles, keys, indexes, and manifests

Every pool bundle contains the specified six artifacts: two BGZF FASTQs, two
standard `.gzi` files, a private BGZF mapping CSV, and a private physical-order
index. The exact twelve mapping fields retain full original metadata as canonical
hex. Independent struct fixtures freeze the 64-byte index header and 40-byte
entry. Generated secured identifiers contain run ID/full pair tag/mate role;
secured plus lines are bare `+`.

BGZF implements bounded deterministic block framing, CRC/ISIZE/EOF checks,
level-zero fallback, crossing-record reads, and canonical virtual offsets.
Synchronized greedy bundle admission checks all lanes and their sidecars/index;
a singleton that cannot fit fails. The default physical cap is exactly
5,000,000,000 bytes and applies to metadata/markers as well as FASTQs.

Strict bounded manifests reject duplicate keys, invalid UTF-8/nonfinite values,
boolean integers, unknown fields/versions, unsafe paths, inconsistent source
identities/totals, and impossible inventories. Encoded and parsed metadata have
separate admission checks. Empty-source facts, generated artifact roles/names,
dense sample/bundle domains, and `6 * bundles + 1` totals are validated.

Independent gzip/DEFLATE and wire oracles, hostile format mutations, physical
cap inspection, and local HTSlib `bgzip`/pysam interoperability tests are present.
Fixed inputs/root/run ID/topology/build produce identical six-artifact bytes
across storage modes and admitted CPU budgets 1, 2, and 4; manifests are compared
semantically without their deliberately different execution metadata.

### 07 — Complete verification

Full verification checks every declared artifact's consumed bytes/hash and all
BGZF streams, sidecars, index entries, key rows, secured headers/roles, lengths,
and physical joins. An independent greedy-boundary calculation rejects producer
rollover mistakes. Protected external sorts prove the exact original ordinal
domains and global tag uniqueness; reconstructed per-source normalized digests
and counts must match the manifest. No producer placement table is trusted.

Optional original-input verification parses a separately admitted catalog and
checks exact normalized source content with an explicit report scope. It never
silently downgrades to pool-only validation. Tests mutate structure and identities
while recomputing outer hashes, so count/hash agreement alone cannot bless a
broken bijection. Unsigned hashes establish consistency with the supplied pool,
not trusted provenance against wholesale malicious replacement.

### 08 — Multi-sample recovery

Selections resolve before destination creation and follow canonical sample order.
Indexed mode traverses each index once for the union, validates complete required
indexes/sidecars, and gathers selected records without hashing unread FASTQ/key
payloads. Its scope is `selected_records`. Explicit sequential mode consumes
complete bundle streams, never resolves/stats/opens the bypassed index, and
records `complete_bundle_streams` with `explicitly_bypassed` index validation.
There is no silent fallback from indexed mode.

Both modes feed the same protected sort/reconstruction path. Every selected
original ordinal and complete normalized digest must pass before one receipt
commits the entire request. Original metadata determines fresh synchronized
recovery part boundaries under the same or a smaller cap. Selected empty samples
receive valid empty mate BGZF files and sidecars. Fresh recovery IDs allow repeated
successful recovery into the same destination.

Tests cover exact opaque/CR normalization, absent originals, union versus separate
recoveries, many bundles, empty samples, cap/selection admission, late selected
sample digest failure, source-close failure before commit, and compression failure.
Cleanup attempts every active mate writer and releases metadata while preserving
the first error. Failed gathering explicitly closes its scanner even when a caller
retains the exception traceback; live descriptors and leases are released.
Borrowed frames/views are dropped before output-stage transitions or error return.
Source descriptors and compression work close before publication.

### 09 — Parallel execution and performance

One invocation-wide compression service now serves all mix lanes or both recovery
mates. CPU admission intersects requested workers, logical CPUs, affinity, and
visible cgroup quotas; memory admission can lower compression concurrency. The
coordinator counts as one CPU-heavy slot. CPU budget one uses synchronous calls;
higher budgets use bounded threads with at most two queued/completed blocks per
compression slot. Completed results retain their leases until ordered delivery.
Failure/cancellation stops callbacks, joins the service, and preserves the first
safe error before transaction cleanup.

Dev7 integrates one invocation-owned scheduler into public mix/verify/recover.
Workers parse snapshot-bound sources once, scatter complete batches and rank
canonical terminals. Closed sort policies serve mixing, verification and both
recovery modes without serialized callables. Coordinator-owned coverage, tags,
normalized hashes and marker-last publication retain their original semantics.
Reports record the admitted process/compression partition. CPU one and lower
resource envelopes use synchronous fallbacks; requested workers are an upper
bound. Exact local six-artifact equivalence includes real spawned production
jobs. Resource/performance and supported-host qualification remain open.

### 10 — Acceptance, documentation, and release

The suite now includes complete public command round trips, independent expected
normalized bytes, exact format/crypto fixtures, real fixed-root shuffle regressions,
self-consistent corruption cases, resource/ownership faults, and local HTSlib
checks. Audit traceability uses executable collected node IDs. Development helpers
provide bounded private-data checks and synthetic fixture generation.

CI configuration targets Linux/macOS and Python 3.11/3.14, but a configured lane
is not a passing support-matrix result. Historical
[foundation-evidence.json](foundation-evidence.json) and
[storage-evidence.json](storage-evidence.json) describe dev0/dev1 only. Candidate
source/archive hashes, dependency versions, and installation results are
recorded in [pipeline-evidence.json](pipeline-evidence.json): **1,122 source tests
and 1,122 installed-wheel tests passed**, including outside-checkout import
checks. Wheel/sdist contents and a wheel rebuilt from the sdist were checked.
The installed-wheel environment reused system-site dependencies; fully isolated
locked release environments remain a Plan 17 requirement. A tiny five-pair
installed-candidate benchmark completed one warm-up and five measured runs.
These results establish their stated local scopes, not release qualification.

That evidence file and its archive hashes remain unchanged. Dev2 fingerprints
describe the preserved historical candidate. Historical shared-metadata changes and
their separate dev3 source/installed-package results are recorded in
[metadata-evidence.json](metadata-evidence.json); old passing results are not
reattributed to this candidate.

## Local validation and supplied paired gzip inputs

Synthetic tests exercise the public serial workflow and bounded compression,
including exact output equivalence at admitted CPU budgets 1/2/4. The two local
independent HTSlib tests pass using installed `bgzip` and pysam 0.23.3: produced
streams decompress correctly, generated `.gzi` bytes agree with HTSlib, and
virtual-offset seeks recover the expected bytes. This local evidence does not
qualify other operating systems, dependency builds, or a future release candidate.

The supplied files were used in three distinct validations:

- **Historical dev1 complete read-only ingress scan:** 48 nonempty samples, 30,042,389 pairs,
  60,084,778 reads, 1,691,881,688 compressed bytes, and 10,983,150,366 normalized
  bytes. Mate checks passed with a configured 512 MiB allowance. About 255 seconds
  was observed for the serial scan; this is neither a benchmark comparison nor
  a peak-RSS measurement.
- **Bounded encrypted-storage prefix:** up to 128 pairs per sample, totaling
  6,144 pairs and 2,154,240 normalized bytes, passed PYPAIR1 encoding, external
  encrypted storage, authenticated rereading, and independent identity/content
  checks. Scratch cleanup completed. Replay with
  [check_storage_inputs.py](../../scripts/check_storage_inputs.py).
- **Bounded complete-workflow prefix:** the same 6,144-pair/12,288-read scope
  produced a two-bundle, 13-artifact pool, passed complete verification of that
  prefix pool, and passed indexed and sequential recovery. Independent stdlib
  gzip hashes, byte counts, and newline counts matched every selected prefix.
  Both recoveries together produced 96 parts and 386 artifacts; all temporary
  fixtures, pools, and restored files were cleaned. The run admitted three
  compression slots under a four-CPU upper bound. Replay with
  [check_roundtrip_inputs.py](../../scripts/check_roundtrip_inputs.py).

The complete 30,042,389-pair dataset has **not** been mixed and recovered. Prefix
validation does not check unconsumed source suffixes or their final gzip trailers;
the separate complete ingress scan supplies that input-only evidence. Source
files remain unchanged. Public evidence includes aggregate counts and booleans,
not source names, original records, mapping contents, private digests, or keys.

## Historical dev3 validation scope

The preserved dev3 increment has **1,233 source and 1,233 installed-wheel passes**
in [metadata-evidence.json](metadata-evidence.json). New tests check
shared-category exhaustion, atomic handoffs, retained catalog/page/report
lifetimes, serialization overlap and complete workflows at 256/512 MiB in all
storage modes. The existing artifact determinism and HTSlib tests remain required.

The supplied-data prefix was rerun: all 48 samples, 6,144 pairs, 12,288 reads and
2,154,240 normalized bytes passed mix, complete prefix-pool verification and both
recoveries. Independent gzip hashes/counts matched, originals were unchanged and
private temporary outputs were removed. This remains a prefix run, not the full
30,042,389-pair Plan 16 task. No new representative performance result is claimed.

A local package check does not satisfy Plan 17's isolated hash-locked four-cell
platform matrix. The current candidate remains a development build. Detailed
remaining work and evidence limits follow below.

## Dev4 ownership and acceptance increment

[OutputOwnershipJournal](../../src/readmixer_py/ownership.py) stores exact private
`RMPYOWN1` records in two authenticated ledgers: registered artifacts and
successful final links. Creation-order registration and reverse-order publication
make ordinal validation bounded. Exclusive staging creation rejects duplicate
names; no transaction-owned growing list or name set remains. Journal pending
boundaries are the ownership commit point even when append does not return.

Mix and recovery share one store/identity namespace with their ownership pages.
Pair/sort/codec owners close before final links; journal keys/pages remain until
commit or rollback cleanup. Cleanup authenticates a page before deleting its
external files, retains bounded progress and refund tokens across interruptions,
and preserves undecodable or foreign residue. Terminal abandonment releases RAM
and descriptors without guessing paths. Ordinary postcommit ownership cleanup
failures keep valid final files and return bounded warnings. Hard termination
can leave private plaintext staging and encrypted scratch; resume is unsupported.

The historical dev4 source and installed-wheel suites each passed **1,352 tests**, including **119 new cases** since
dev3. New local tests cover many inventory/link pages in all storage modes, actual
public workflows crossing pages, interrupted registration/link/refund boundaries,
closed-unregistered handle limits and durable-output preservation. Plan 15 adds
independent union-gather I/O/cache assertions and self-consistent secured-record
mutations. Plan 16A adds reproducible v2 fixture profiles, fresh process/import/CLI
startup lanes and scoped child CPU/thread observations. Those dev4 results and
candidate fingerprints belong to [ownership-evidence.json](ownership-evidence.json),
not the preserved historical dev2/dev3 records. These checks do not qualify a
release or complete Plans 15/16/17.

## Dev5 private worker and independent oracle increment

Plan 13 now has a private `StageScheduler`, `StageAdmission`, fixed binary protocol,
nonblocking framed socket transport and lightweight spawn bootstrap. Jobs have a
single explicit identity-sort policy; no arbitrary function or pickle application
message crosses the channel. Control frames are capped at 64 KiB and raw chunks
at 1 MiB. At most two submitted/completed jobs per worker hold full admitted RAM,
metadata, transport and result windows. CPU one uses the same policy synchronously.

The sender lends its unique source lease until the worker accepts the complete
input. Result length/digest and task/grant state are checked before final ACK;
returned result leases survive scheduler close. This is an internal memory-batch
contract, not the planned `MemoryRef`/external-segment capability API. Workers
receive no root key or publication authority. Registered worker/helper RSS,
descriptors and READY thread counts enter aggregate excess-only admission.

Native-thread environment limits apply before spawned interpreter imports and
are restored in the parent. The launch lock covers ReadMixer launches only.
The interpreter-owned resource-tracker helper is accounted while the scheduler
runs and may remain afterward; all task workers must be reaped. Global shutdown
uses ten seconds for cooperative stop, five after terminate, then kill and a
bounded five-second reap check. Failed reaping retains ownership/reservations
for retry. Versioned worker/channel/sort charges still require Plan 16 calibration.

Plan 15's independent bounded serial oracle constructs explicit identity order,
tags and all six artifact bytes independently of producer helpers. Fourteen
cases cover two wire goldens and tiny/rollover workflows across three storage
modes and compression levels zero/six; complete verification and both recoveries
check the expected contents. Forced refinement/collisions retain their separate
component oracles. Private worker tests do not prove production parallel byte
identity, full workflow signal safety or platform qualification.

The dev5 source suite passes **1,582 tests**, including **230 new cases** since
dev4. Installed-wheel checks and the bounded supplied-input round trip are
recorded with their exact scope in the candidate evidence.

Dev5 validation, exact candidate fingerprints and preserved historical hashes
are recorded separately in [spawn-evidence.json](spawn-evidence.json). At that historical candidate, the public pipeline was serial with shared
compression threads and Plan 14 was unimplemented. Dev7 supersedes that boundary;
the evidence and hashes above continue to describe dev5 only.

## Historical dev6 Plan 13 infrastructure boundary

The private scheduler now accepts explicit memory references or bounded segment
batches. Memory mode keeps raw admitted transfers. External and auto modes use
exact derived-key file grants, one coordinator disk budget, bounded per-write
credits, full returned-segment authentication and a separate paged task-ownership
ledger. Returned segment results survive scheduler shutdown with caller ownership;
undelivered or failed output stays owned through reaping and retryable cleanup.
Foreign replacements and corrupt journals cannot authorize guessed deletions.

Valid READY RAM/descriptor shortages now trigger reaping and a smaller partition,
retaining measured high-water coefficients before jobs/compression start. Other
startup/protocol/cancellation/cleanup errors remain fatal. Worker-local subleases
bound copies, sorting, codecs, metadata and descriptors within the global grant.
Periodic service checkpoints observe registered-process resource pressure.

The dev6 source suite passes **1,789 tests**, with **203 new cases** since the
post-dev5 1,586-node collection (207 since the historical dev5 candidate).
Installed-candidate execution is recorded separately in its evidence file.

The [Plan 13 current section](13-spawn-workers-and-bounded-storage-handoffs.md#10-dev6-protected-worker-infrastructure)
links every mechanism to its local tests. Candidate-specific source/installed
results and fingerprints belong to
[worker-handoff-evidence.json](worker-handoff-evidence.json), not historical dev5
or the subsequent regression-only handoff. No new large supplied-data or
representative benchmark result is inferred from private worker tests.

**Historical dev6 stopping point (superseded by dev7):** the closed production-independent identity-sort worker stage
supports memory/external/auto handoffs. Public mix/verify/recover still execute
data stages on the coordinator with compression threads. Plan 14 must integrate
LPT/direct-scatter/refinement/terminal/rendering and supported external-sort jobs,
including their stage-specific derived key scopes. Plan 15 must extend actual
workflow/publication failures. Plans 16/17 must qualify resources, performance,
installed supported hosts and the full 30,042,389-pair supplied-data run. Therefore
Plan 13's cross-plan completion checklist and Plan 10 release gate remain open.

## Dev7 Plan 14 production integration

The production scheduler now owns deterministic LPT source jobs, exact normalized
batch credits, complete 64-writer producer grants and canonical native terminal
jobs. Root keys and publication authority stay in the coordinator. Bounded raw
or authenticated encrypted handoffs cover every scatter bucket; returned ownership
is recorded in pages before sender release. Admission includes manager metadata,
64 potential output descriptors, parser carry, transport overlap and one shared
flush workspace. Reduced partitions/batches and the serial sorter fallback are
selected before source decoding. Producer buffers close before emission begins.

Closed sort policies support stable run generation and bounded merge groups for
mixing, complete verification and both union recovery modes. Larger records/groups
use the existing admitted coordinator merge. Terminal completion may be delayed
without changing leaf order; one next refinement may overlap earlier terminals.
Memory terminal results use checked rank-ordered packed frames, with coordinator
little-endian offsets and exact identity coverage; redundant permutation arrays
are not transmitted. This explicit representation decision is recorded in Plan 14.

Bulk AES tags reuse a coordinator context in windows of at most 4,096 identities
inside a retained terminal. Greedy six-lane bundle emission and verification/recovery normalized
hashing remain on the coordinator. Source reports carry worker `PairedReader`
hashes computed during original-order parsing; scatter results independently
validate routing, exact source identity coverage, lengths, counts and seals.
Complete verification independently reconstructs the normalized hashes.

Local source and outside-checkout installed-wheel results, dependency versions,
archive/source fingerprints, and bounded supplied-input validation are recorded
in [parallel-pipeline-evidence.json](parallel-pipeline-evidence.json). They apply
only to dev7. Historical dev2's 1,122 source/installed passes, prefix round trips
and tiny benchmark, and all later historical evidence remain unchanged.

**Historical dev7 handoff:** Plan 14 runtime and bounded local acceptance were
implemented. Broader workflow fault/boundary coverage was assigned to Plan 15; measured
resource envelopes, stage/startup performance and the full 30,042,389-pair workload
are Plan 16; locked Linux x86-64/macOS arm64 CPython 3.11/3.14 candidate lanes and
release reconciliation are Plan 17. No representative speedup, universal RSS
ceiling, full supplied-data round trip or release qualification is claimed.

## Historical dev8 Plan 15 acceptance

Version `2.0.0.dev8` completes the bounded local Plan 15 matrix with **337 new
cases**. The implementation fixes a pool cleanup defect found by reached-stage
source-close injection: metadata and registry leases are now released even when
source closure fails, and secondary cleanup errors preserve the admission or
workflow error that caused the failure.

The seven new test modules cover independent serial-versus-worker artifact bytes,
physical/encoded/represented metadata and inventory boundaries, every index/key
field and hostile physical addresses, repaired outer hashes with biological body
mutations, original occurrence domains and verification scopes, relocated original
comparisons, and actual resource/publication failures. Exceptions remain live
while descriptor, lease, worker, source and foreign-file checks run.

Real SIGINT/SIGTERM tests acknowledge nine workflow stages before signaling. A
broken worker channel preserves the causal error; SIGKILL cases distinguish
pre-marker, marker-visible/pre-sync and durable commit while inspecting private
residue. The kill harness owns a single-process group and makes no cleanup or
power-loss promise. Existing bounded oldest-result/termination tests remain part
of the suite. Native checkpoints retain a completed native result before its
handoff rather than claiming interruption inside a particular C instruction.

Candidate-specific source/installed-console outcomes, exact nodes, source/archive
hashes and bounded supplied-input results belong to
[workflow-acceptance-evidence.json](workflow-acceptance-evidence.json). The suite
runs outside the checkout against the exact installed wheel; CLI tests select
its absolute console executable. Guarded signal helpers are included in the
sdist. Local packaging reuses installed dependencies and does not substitute
for Plan 17's isolated locked candidate matrix.

**Historical dev8 stopping point (superseded below):** Plans 11/12, Plan 13 worker infrastructure, Plan 14
production integration and Plan 15 bounded local acceptance are implemented.
Plan 16's measurement tooling, constrained/large workflows, full 30,042,389-pair
supplied-data task and representative benchmarks remain open. Plan 17 owns the
immutable locked candidate, supported-host execution and evidence reconciliation;
Plan 10 remains the sole release-completion gate. Package publication is separate.

## Follow-on ownership and handoff

The follow-ons have seven linked implementation plans. Their scenarios remain
distinct from execution results. Plans 11/12 have local implementations;
Plan 13 infrastructure and Plan 14 production integration are locally implemented;
Plan 15 has bounded local acceptance. Plan 16's implementation, regressions,
full supplied-data task and all required local qualification cells are complete;
Plan 17's remaining platform gates remain open.

- [11 — Shared metadata budget and lifetimes](11-shared-metadata-budget-and-lifetimes.md):
  implemented shared accounting and retained-object ownership; extends Plan 03.
  Later owners must use this interface; remaining platform envelopes belong to 17.
- [12 — Paged output ownership and publication](12-paged-output-ownership-and-publication.md):
  implemented locally in dev4: authenticated inventory/link pages, bounded active
  handles, one operation store, interrupted handoffs and retryable cleanup;
  extends 03/05. Local scale qualification passed in 16; remaining platforms belong to 17.
- [13 — Spawn workers and bounded storage handoffs](13-spawn-workers-and-bounded-storage-handoffs.md):
  worker infrastructure now includes memory receipts/subleases, encrypted
  capabilities, task-ledger/disk-credit handoffs, auto spill and startup retry.
  Production integration is implemented in Plan 14; local measured acceptance
  passed in 16, and remaining platforms belong to 17. Extends 03/05/09.
- [14 — Deterministic parallel pipeline](14-deterministic-parallel-pipeline.md):
  locally implemented LPT parsing, direct scatter, canonical terminal queues,
  bulk tags, closed-policy sorting and shared compression; extends 02/04/06–09.
  Measurement/platform evidence is owned by Plans 16/17.
- [15 — Workflow boundaries and failure acceptance](15-workflow-boundaries-and-failure-acceptance.md):
  locally implemented: independent serial/worker artifact oracles, reachable
  lane and metadata/inventory boundaries, full public hostile-field matrices,
  original-input comparisons, indexed scope/I/O checks, reached-stage resource
  and publication faults, real SIGINT/SIGTERM, broken IPC and SIGKILL residue.
  Exact local results and limitations are in the dev8 evidence; extends 01/05–10.
  Supported-platform qualification belongs to 17, large physical caps to 16.
- [16 — Resource scale and performance qualification](16-resource-scale-and-performance-qualification.md):
  dev10 implements versioned fixtures, independent component/workflow measurements,
  artifact-class I/O, backpressure, native/process observations and real host
  controls. Scale, physical-cap, full private supplied-data validation and all
  required local measurements pass on the frozen candidate. Extends 03/09/10.
- [17 — Platform packaging and release closure](17-platform-packaging-and-release-closure.md):
  dev10 local Linux CPython 3.11 candidate is isolated and hash-locked; the other
  supported-host cells and final exact-candidate reconciliation remain open;
  extends 01/10 and all parent platform gates.

Plans 11–14 provide the locally implemented production foundation; Plan 15
provides its bounded local acceptance. Plan 16's local qualification is complete
on the frozen dev10 candidate. Next complete Plan 17's remaining supported-host
lanes and 17B reconciliation. A fix creates a new candidate and reruns affected
checks.
The [planning index](README.md#execution-order-and-candidate-milestones) records
this split explicitly. Plan 10 remains the sole release-completion gate.

The full supplied-data run is a separate requested local task, with aggregate-only
public evidence. It does not make private inputs a public CI prerequisite.
Preserve input files, validate all selected samples, and run the two recovery
modes sequentially to bound retained output space. Dev10 completed the full task;
historical prefix evidence remains separately attributable.

## Remaining implementation and acceptance gates

1. **Plan 17 — exact candidate and supported hosts.** Dev10 immutable wheel/sdist
   and isolated hash-locked Linux CPython 3.11 acceptance are recorded. Complete
   outside-checkout installed-package, interoperability, filesystem, real-signal/
   fault and resource lanes for the remaining Linux x86-64/macOS arm64 and
   CPython 3.11/3.14 combinations.
   Local Linux CPython 3.11.7 acceptance and configured CI do not establish the
   other hosts. Changed source/archive/dependency hashes invalidate affected
   qualification and require a new candidate.
2. **Plan 10 — release completion.** Reconcile all correctness, fault, platform,
   resource, performance, privacy, documentation and audit evidence after the
   required lanes pass. This is the sole release gate. Publishing a package is
   a separate action.

Plan 15's local implementation and regression obligations are covered by the
scenario index in its document and exact executed nodes in dev8 evidence. Its
remaining platform repetitions belong to Plan 17; no universal RSS ceiling,
representative speedup or power-loss durability experiment is claimed.

The format intentionally does not support legacy salvage, Rust interchange,
single-end/interleaved/multilane input, a database, incomplete-run resumption, or
recovery of original gzip container bytes. Final recovery material is sensitive
plaintext protected by file permissions, not an encrypted keystore or sample
access-control boundary. Memory mode does not promise locked/unswappable or
forensically erased memory. Anchored identity checks do not turn mutable inputs
into immutable filesystem snapshots.

## Dev9 Plan 16 candidate preparation

The Plan 16 §9 tools implement complete measurement and execution paths for
P16-T01–T06. The frozen candidate preparation does not assert successful scale,
physical-cap, constrained-host, native-workspace or full supplied-data results.
See [Plan 16](16-resource-scale-and-performance-qualification.md#9-dev9-qualification-preparation-and-exact-execution-boundary)
for ownership and commands/tool anchors. Source/wheel suites and measurements
must be recorded against dev9's exact archives and isolated dependency lock.
The dev8 2,322 source and 2,322 installed-wheel passes remain dev8 evidence.
Plan 17's multi-platform qualification and Plan 10 release closure remain open.


## Historical post-freeze dev9 qualification execution

[Dev9 resource qualification evidence](resource-qualification-dev9-evidence.json)
identifies the immutable dev9 wheel/sdist, fully isolated hash-locked Linux
CPython 3.11 environment, completed observations and still-running cells. This
section is a later documentation snapshot than the archived preparation text.
All **2,537 outside-checkout installed tests passed**. The first eight-CPU source
run had four fixture-setup RAM-admission failures (2,533 passed); its diagnostics
remain preserved; an unchanged four-CPU replay passed all **2,537 tests**
and is recorded separately.
Component/fresh-process measurements and independent six-artifact supplemental
checks completed. The final historical record retains 23 accepted matrix cells,
two rejected cells and interrupted physical/full-data attempts. Successful input
inventory is not successful full recovery; dev10 must repeat affected lanes.


## Dev10 measurement-driven correction and renewed qualification

Dev9's required auto-scale lane exposed a real implementation gap: its six mixes
completed, but verification exhausted RAM while accumulating an unsealed merge
output. Sorter outputs already destined for encrypted disk now select that writer
policy at creation, with the same operation ownership and admitted workspaces.
A 128 MiB sorted-run regression completes under a 96 MiB budget; strict memory
still refuses without algorithmic disk spill. Shared storage policy is unchanged.

Native frame/header scans preserve borrowed bytes and validation contracts while
reducing measured scan costs. The scanner/serial-fixture preparation passed 2,683
source tests before the additional journal and auto-sort fixes; this is preparation
evidence, not a dev10 installed-candidate suite. Candidate-bound results belong in
[resource-qualification-evidence.json](resource-qualification-evidence.json).

Completed workflow observations now have an exclusive private fsynced journal.
Its rows survive later interruption; only a complete final report qualifies a
cell, and checkpoint failures prevent publication of success. No resume is added.
The physical workload uses a 64 GiB host limit; full supplied-data validation uses
a constant 32 GiB host limit, each with an unchanged 2 GiB application budget.

All dev9 archives and completed/failed observations remain separately attributable.
Its unfinished physical/full-data runs were administratively interrupted for this
candidate correction, without claiming their completion. Dev10 repeated its
source/installed tests, workload matrix, component/native measurements and full
30,042,389-pair validation; the final results follow below. Plan 17's remaining platforms and Plan 10 release
closure remain open after local Plan 16 work.


## Frozen dev10 execution evidence

The immutable dev10 wheel and sdist have isolated hash-locked Linux x86-64 /
CPython 3.11 installations. All **2,703 source tests and 2,703 outside-checkout
installed-wheel tests passed**, with no failures or skips. The separate sdist
rebuild has identical wheel-member and installed package bytes; ZIP timestamps
prevent an archive-byte reproducibility claim. Original archives remain the
qualification candidate.

Component, startup, calibration, fresh-process, independent artifact-reference
and compression-window lanes have **270 passing observations across 45 measured
configurations**, each with one warm-up and five measured runs. The initial
isolated fresh-script bootstrap failures are retained separately; adding only the
verified archived scripts directory to that invocation fixed them without changing
candidate bytes. No observed calibration excess or universal RSS claim is made.

Current archive hashes, commands, test nodes, observations and remaining gates
are in
[resource-qualification-evidence.json](resource-qualification-evidence.json).


### Full supplied-data validation and synthetic matrix

The existing installed-candidate runs continued during the usage interruption;
no completed workload was restarted. All **48 samples / 30,042,389 pairs /
60,084,778 reads** completed mix, full pool verification, indexed recovery and
sequential recovery. Both bounded independent comparisons consumed all original
and restored streams to EOF and matched every normalized field and ordinal.
Originals are unchanged, workers were reaped and the owned workspace was removed.
The 10,983,150,366 normalized bytes exceed four times each actual 2 GiB operation
budget. The full task took 28,480.23 seconds under a constant 32 GiB host cgroup;
its 12,686,389,248-byte cgroup peak includes file cache. All memory-event counters
were zero. Aggregate evidence and private provenance have separate permissions.

All **25 synthetic matrix cells pass**: 21 bounded/controlled-host cells, three
external/auto/strict-memory scale cells, and the physical default-cap cell.
All 150 observations reconcile: 126 full workflows and 24 expected typed resource
refusals, with one warm-up and five measured observations per configuration. The external and auto lanes
each completed six full workflows with 2,204,344,800 normalized bytes and actual
512 MiB budgets in every phase. Strict memory refused cleanly without disk spill.
All 42 balanced and 12 short-read comparison observations have identical six-
artifact bytes within their fixed contexts. The observation journals match final
reports, and host controls and failed-attempt retention have been reconciled.

**Plan 16 is complete locally.** All six physical default-cap workflows passed,
including final cleanup/report acceptance. Each produced two bundles and reached
a 4,983,858,306-byte data artifact below the 5,000,000,000-byte cap. Every required
local scenario P16-T01–T06 now has candidate-bound accepted evidence. No Plan 16
implementation or local execution work remains; observed resource envelopes are
specific to the recorded build/workload/host and are not universal RSS guarantees.
Plan 17's remaining platform matrix and Plan 10 release closure stay open.

The short-read CPU-four median was 110.310 s versus CPU-one's 86.264 s; this host
showed no parallel speedup for that fixture. Cache reuse and overlapping host I/O
are explicit in [Plan 16](16-resource-scale-and-performance-qualification.md).
Historical records and frozen candidate hashes are unchanged; this completion
inventory is a later documentation snapshot, not a rebuilt package.


## Plan 17 external qualification implementation (2026-09-30)

The frozen dev10 runtime, tests, archive bytes and completed Plan 16 record were
preserved. New tools under [tools/release](../../tools/release/README.md) add
candidate transport, native wheel locks, isolated source/installed test execution,
explicit platform applicability, controlled resource launch, encrypted private
transport and evidence reconciliation. They fingerprint their own later snapshots
and never represent a rebuild of the dirty checkout as the tested candidate.

A fresh Linux x86-64 CPython 3.11.7 lane passed all 2,703 source and 2,703 installed
tests, both independent HTSlib oracles, six dependency-free entry probes, private
filesystem checks and all-member sdist-wheel content equivalence. Linux CPython 3.14.7 completed with 2,701 source passes / 2 failures and
2,700 installed passes / 3 failures. Two failures assume a CPython-specific exact
reference count; the installed suite also encountered admission refusal before a
boundary fixture's intended checkpoint. Both prepared regression patches and
focused controls are retained separately, unapplied to the candidate. The boundary
failure did not reproduce in isolated controls, so full-suite closure remains open.
[release-evidence.json](release-evidence.json) identifies every attempt and result.
Failed interpreter preparation is preserved separately from executed regressions.
The external tools passed 274 tests; 150 bounded native Python 3.14 supplement
observations passed with explicit larger-workload/remaining-probe limitations.

Remaining work starts with reviewed regression corrections and a new frozen
candidate, then affected full-suite repetition and actual host execution/review:
both macOS arm64
ordinary cells; all still-unqualified native-build resource/calibration/supplement
lanes; portable macOS replacement regressions; reconciliation of scoped existing
Linux 3.11 Plan 16 evidence; and final parent-scenario/audit/documentation review.
The known Linux 3.14 failures remain blocking in the current execution index.
General new-candidate freeze/build automation and orchestration for 108 remaining
oracle plus 12 compression observations are not provided by the completed subset
of tools; the guide identifies the required protocol and next preparation steps.
No Plan 17 or Plan 10 completion claim has been made. The operator guide gives
commands and explains exactly which files to return so these gates can be assessed.
