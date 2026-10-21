# Plan 14 — Deterministic parallel pipeline

Status: **Runtime integration implemented locally in `2.0.0.dev7`.** The public
mix/verify/recover paths now use the invocation-owned spawn scheduler when complete
resource admission permits. Local acceptance and exact candidate results are
recorded in [IMPLEMENTATION.md](IMPLEMENTATION.md) and
[parallel-pipeline-evidence.json](parallel-pipeline-evidence.json). Remaining
workflow fault breadth, measured scale/performance, and supported-host release
qualification belong to Plans 15–17; Plan 10 remains the release-completion gate.

Prerequisite chain: [11](11-shared-metadata-budget-and-lifetimes.md) →
[12](12-paged-output-ownership-and-publication.md) →
[13](13-spawn-workers-and-bounded-storage-handoffs.md) → this plan.
Retain algorithm and resource contracts from [03](03-resource-admission-and-storage.md),
[04](04-global-pair-randomization-and-sharding.md), and
[09](09-parallel-execution-and-performance.md). [15](15-workflow-boundaries-and-failure-acceptance.md)
owns remaining workflow failures; [16](16-resource-scale-and-performance-qualification.md)
owns measurement; [17](17-platform-packaging-and-release-closure.md) owns closure.

## 1. Serial baseline and preserved contracts

[mixing.py](../../src/readmixer_py/mixing.py) builds one catalog, parses sources
once, feeds [SerialShuffler](../../src/readmixer_py/shuffle.py), and emits the
six-artifact bundles on the coordinator. Serial scatter groups keyed bucket
assignments using `ExternalSorter`, then opens one pair-segment writer at a time.
This trades additional protected I/O for a smaller simultaneous working set.

The serial implementation already fixes bucket transcripts, rank layout/retries,
refinement limits, canonical identity ordering, and exact coverage. Preserve it
as the synchronous reference and low-resource fallback. Keep pool schema 1,
shuffle algorithm 1, execution profile 1 and all CLI options unchanged. There
is no production seed, alternative permutation, or Rust artifact compatibility.

Under fixed input bytes/catalog, test root/run ID, resolved B/T, codec/build,
compression level and artifact cap, all six files in every bundle must match
exactly across execution strategies. Compare manifests after excluding execution
metadata; do not equate timings/reports, scratch names, ciphertext, or ownership.

## 2. Pipeline stages and source scheduling

Integrate Plan 13's scheduler behind private stage interfaces in the existing
mix/shuffle path. The coordinator owns the catalog, global normalized-byte
counter, source-report inventory, source mutation checks, and final completion.
Each child receives only assigned source snapshots and capabilities; catalog
objects and open input streams do not cross process boundaries.

Choose the admitted parser count before assignment. Apply deterministic LPT:
sort samples by descending `r1.snapshot.size + r2.snapshot.size`, breaking ties
by canonical sample index. Assign each next sample to the producer with the
smallest accumulated physical-byte weight, breaking ties by producer ordinal.
This is a scheduling estimate only, never a decoded-size or RAM estimate.
Within each producer, parse assigned samples in ascending canonical index.

Charge the bounded assignment representation to Plan 11's shared metadata
budget. Send assignments incrementally or through admitted ledger pages; do not
materialize a job/result matrix proportional to samples times workers. Reuse
the same producer identity across its bounded batches. Worker identities affect
scratch ownership only and never enter bucket/rank/tag derivation.

A worker opens each admitted mate exactly once through no-follow anchored input
validation, verifies its snapshot before/after the one forward decoding pass,
and runs the existing framing/mate/normalization rules. Ordinary gzip remains
sequential per source. Reports contain counts, normalized digests and lengths;
the coordinator validates exactly one complete report per canonical sample.

For a configured `max_total_uncompressed`, obtain coordinator byte credits
before accepting normalized pairs into scatter. Allocate credits in bounded
batches without inventing an expansion estimate; reconcile unused credits at
batch/sample/task end. Reclaim unused outstanding credits before rejecting a
request that could fit; a reserved but unused credit is not actual input content.
Credits prevent parallel workers from each accepting the full
ceiling independently. Without a ceiling, use checked aggregate u64 counters.
Parsing carry may contain the next pair before admission, but rejected bytes
never become admitted scatter content. Require two nonempty samples globally.

Seal scatter inputs and validate all source counts before terminal emission.
Close source descriptors before publication. Worker failures invalidate pending
sample reports; do not salvage partial samples or silently repeat decoding.

## 3. Direct scatter, whole-set batches, and fallback

Add a direct scatter strategy with the common 64-root-bucket namespace. Admit
the complete writer set before a producer starts: 64 logical buffers/contexts,
64 potential spill-capable handles, two inputs, controls, frame carry, metadata,
batch retention and transport overlap. Strict memory charges logical writers
and actual handles; it reserves no nonexistent scratch disk capacity.

Current [SegmentWriter](../../src/readmixer_py/storage.py) reserves an 8 MiB
external workspace per writer in addition to its 1 MiB buffer. Do not multiply
that transient workspace by 64 when a single worker flushes synchronously.
Implement one exclusively borrowed flush/AEAD/codec workspace per worker and
retain all 64 writer buffers/handles with their actual small native state.
Refuse overlapping flushes using the same workspace. Preserve independent
segment keys, block counters, seals and ownership; sharing a workspace does
not share an encryption stream. Plan 16 must measure the replacement envelope.

Use admitted whole-set batches with an encoded payload target up to 64 MiB,
and at least one configured maximum frame. Before a pair would exceed the
target, seal all nonempty writers, hand off or spill their segments, and open
fresh identities/generations for the next batch. Abort empty writers without
creating artificial data segments. Finish a final partial batch at source/task
end. Batches may be smaller when admission requires it; their boundaries never
change bucket assignment or canonical topology.

Whole-set rotation is required in auto mode: current storage can spill only
sealed eligible segments. A large source must not accumulate its entire input
in 64 unsealed memory segments. Never use per-record LRU writer eviction as
the normal replacement for complete-set admission.

Try complete-set admission while reducing parser concurrency. If no direct
set fits, select the existing serial sorter-scatter strategy in the configured
storage mode. This is the approved low-resource exception to Plans 03/09's
direct-writer prescription, not permission to weaken admission. Strict memory
still fails before disk scratch; auto/external retain authenticated scratch.
Do not restart a partly decoded source merely to change strategy. Select before
ingress; later pressure spills eligible sealed data, pauses admission, or fails.

Preserve a private injected strategy seam for tests; add no CLI strategy option.
Record the actual strategy in bounded development/resource telemetry, not in
public artifact bytes or an unversioned new manifest field.

## 4. Canonical merge, refinement, and terminals

Workers return compact segment statistics: path, producer/batch identity,
frame count, encoded bytes, maximum frame, and the protected reference. Stream
variable inventories into paged storage and accept ownership using Plan 13.
Cross-check seals and checked aggregate totals against the source reports.

For each bucket, merge segment heads by `(sample_index, original_pair_index)`.
Reject nonincreasing identities, duplicates, count/byte mismatches and invalid
frames. Use the existing admitted merge fan-in, at most 32, with bounded head
buffers and paged run inventories. Never open one descriptor per discovered
segment or collect the entire bucket's locators in a Python list.

Refine oversized buckets using unchanged numeric path order, exact fanout
formula, depth 32 and fixed T. Parent/child coexistence, workers and frontiers
retain their leases until authenticated replacement and cleanup. Discover and
schedule leaves in canonical depth-first numeric order using paged frontiers.
Task submission order can differ from completion order; emitted leaf order
cannot differ. Never shrink terminals because more workers were requested.

A terminal job binds path, pair count, arena bytes, maximum frame, canonical
source references and all eight derived rank-attempt keys. It authenticates
input, reconstructs the canonical identity-ordered packed arena, and runs the
existing bulk rank and NumPy lexsort logic. Whole-batch collision retries retain
the exact attempt semantics; workers never break ties using source identity.

For external results, return an authenticated segment of rank-ordered complete
frames, with exact totals; retain source ownership until handoff is accepted.
Implementation decision in dev7: strict memory also returns admitted,
rank-ordered complete packed frames through the raw transfer/ACK protocol.
The worker constructs checked little-endian offsets and the native rank
permutation locally; the coordinator reconstructs checked little-endian offsets
from the returned framing and proves exact identity coverage before publication.
Offset/permutation arrays do not cross IPC. This uses the same logical result
representation in both backends, avoids transmitting redundant native arrays,
and preserves the fixed terminal envelope and exact artifact order. Auto may choose either backing without changing logical order.

The coordinator drains only the next canonical terminal, assigns dense secured
ordinals, and updates exact identity coverage before handing frames to emission.
Late results retain their completion leases and block further submission once
the item/byte window fills. Reference or arena retention must remain visible to
the budget until its final borrowed frame/view has been consumed and dropped.

## 5. Bulk tags, bounded rendering, and shared sorting

[OperationKeys.tag](../../src/readmixer_py/crypto.py) currently constructs an AES
context per pair. Add a coordinator-owned reusable AES-256 ECB tag generator
with `encrypt_into`-style bounded batches of the exact existing 16-byte identity
blocks. Use windows of at most 4,096 pairs: 64 KiB plaintext identities and
64 KiB ciphertext, plus explicitly charged hex/rendering workspace. Preserve
the existing individual-tag helper and independent golden vectors as oracles.

A window stays within one retained terminal lease; it cannot retain borrowed
views from an already released arena or prefetch another unleased terminal.
Construct numeric identities first, encrypt in bulk, then consume records and
tags in canonical output order. Flush a partial window at terminal boundaries.
Changing tag batch size must not change tag values or output bytes.

Keep final rendering and greedy pair admission in
[BundleEmitter](../../src/readmixer_py/artifacts/bundles.py). Feed mate pieces
through bounded BGZF buffers; do not assemble a list of secured FASTQ/key records.
Compute the complete six-lane footprint before accepting each pair; regenerate
mapping rows after rollover because decimal offsets change. The compression
service receives immutable bounded blocks, never a memoryview retaining a whole
terminal. Release all borrowed views before the terminal lease is retired.

Extend [ExternalSorter](../../src/readmixer_py/sorting.py) with planned closed
production `SortPolicy` values: bucket frames compare `(-bucket, sample, ordinal)`
using the admitted path/fanout/key; original frames compare `(sample, ordinal)`;
24-byte tag observations compare their first 16 bytes; 12-byte big-endian
coverage identities compare their complete bytes. Frame policies retain the
configured PYPAIR1 maximum. Keep equal-key records in source order within runs
and canonical input-run order across merges; duplicate-identity/tag rejection
remains the consuming validator's responsibility. Keep trusted local callables
only for synchronous/internal tests; never pickle a callable into a worker.

Use the shared scheduler for bounded run generation and independent merge
groups. Run ordinals and merge grouping derive from canonical source/run order,
never completion order; merge fan-in remains resource-admitted and at most 32.
Workers return protected references and checked totals, not Python record graphs.
This path serves shuffle, complete verification, and multi-sample recovery.

Verification/recovery keep one required union scan/gather and coordinator-owned
scope, exact coverage, normalized hashing and completion. Do not rescan the pool
per selected sample or combine ordinary SHA-256 chunk digests as if composable.
Recovered records drain in original identity order through the same bounded
rendering/shared compression service. Add further process rendering only in a
separate measured optimization; it is not required to close this integration.

## 6. Acceptance and completion

Retain [test_shuffle.py](../../tests/test_shuffle.py)'s independent permutation,
rank, refinement and distribution fixtures; retain
[test_parallel_pipeline.py](../../tests/test_parallel_pipeline.py)'s existing
compression equivalence and [test_recovery.py](../../tests/test_recovery.py)'s
single union-gather assertions. Extend them with real spawned production jobs.

- P14-T01: unequal/empty samples and equal physical-size ties produce fixed LPT
  assignment; child reader instrumentation proves one forward mate pass and
  exact globally credited normalized limits. Maps to P09-T03, PY04-MIX-001,
  P03-T06 and AUD-11/AUD-21/AUD-24.
- P14-T02: full direct-writer admission, shared flush exclusion, whole-set batch
  rotation, huge single sources, low-descriptor fallback and strict-memory
  failures preserve ownership and bounds. Maps to P03-T03/T04/T05, P09-T04,
  P05-T03 and AUD-10/AUD-23.
- P14-T03: compare all six files exactly for 1/2/4 CPUs, delayed/reversed terminal
  completions, both scatter strategies, different batch boundaries, and memory,
  external and forced-auto-spill storage at fixed B/T. Compare manifests without
  execution. Maps to PY04-DET-001, P09-T02 and AUD-01/AUD-02.
- P14-T04: identity-sorted merging, duplicate/missing frames, corrupted seals,
  exact/over-limit refinement, depth exhaustion, and all rank-collision attempts
  fail or match the independent oracle. Maps to PY04-RNK-001/RNK-002/REF-001,
  PY04-FAIL-001, P05-T02 and AUD-01/AUD-12.
- P14-T05: block the earliest terminal while later work completes; observe all
  arena, transport, merge and completion leases and final release after borrowed
  views disappear. Maps to P03-T03, P09-T04 and AUD-10/AUD-22/AUD-23.
- P14-T06: bulk tags match independent AES vectors and per-pair output at windows
  1/4,096 and terminal boundaries; rollover/key digit boundaries and compression
  backpressure preserve exact files. Maps to PY04-TAG-001, P09-T02/T06 and
  AUD-08/AUD-15/AUD-16.
- P14-T07: all production sort policies match independent key/original-byte
  oracles; full verification and both recovery modes retain their stated scope,
  exact digests and one union gather. Maps to P07-001/003/007, P08-001/002/003,
  P09-T03 and AUD-06/AUD-18.
- P14-T08: worker failure or cancellation during parser, refinement, native sort,
  result handoff, merge and emission stops publication and reaps writers under
  Plan 13 deadlines. Maps to P09-T05/T06, P05-T06, PY04-FAIL-001 and AUD-12/AUD-14.

- [x] Real process execution passes reference and fixed-context artifact checks.
- [x] Every storage/strategy transition preserves one-pass input and fixed topology.
- [x] Tag/rendering batches hold no unleased arenas or unbounded record graphs.
- [x] Plans 15–17 contain the remaining failure, resource and platform evidence;
  passing this plan alone is not a performance or release qualification claim.

## 7. Dev7 implementation decisions and local acceptance

- [parser_stage.py](../../src/readmixer_py/parser_stage.py) supplies closed,
  snapshot-bound capabilities, deterministic LPT producer assignments and exact
  normalized batch credits. A source is read once. Credits are requested for the
  actual completed parser batch before scatter accepts it; there are no idle
  normalized reservations to reclaim. A bounded next-pair carry remains admitted.
  One forward decoding pass is distinct from the no-read descriptor opens used
  for post-read pathname/snapshot validation.
- [worker_scatter.py](../../src/readmixer_py/worker_scatter.py) owns 64 logical
  writers per active producer, one shared flush workspace and complete-set
  rotation. [scatter_stage.py](../../src/readmixer_py/scatter_stage.py) requests
  capabilities incrementally, reconciles actual disk growth and authenticates
  every returned bucket before acknowledging ownership. Empty grant retirement
  is authenticated and cleanup remains coordinator-owned. Memory uses admitted
  raw transfers; auto/external use encrypted grants after bounded batch assembly.
- The coordinator grants a monotonic generation per producer across source
  jobs. It checks bucket routing, unique complete source ordinal ranges, counts,
  lengths and maximum frames. Source SHA-256 values come from the worker's
  existing `PairedReader` in original byte order. They are not composed from
  bucket or chunk digests; complete verification independently recomputes them.
  The scatter batch header has no unused raw-stream digest: returned segment
  seals or raw-transfer digests authenticate the actual result representation.
- [direct_scatter.py](../../src/readmixer_py/direct_scatter.py) stores references
  in an authenticated paged inventory. Its 64 fixed metadata scans avoid a
  locator list proportional to data; payloads feed a bounded identity sorter.
  Registry/IPC/manager costs participate in partition selection. Before ingress,
  admission reduces process count and, if needed, batch size. The coordinator
  direct strategy or sorter fallback remains available with the same fixed T.
  `finish_ingress` seals and releases producer buffers before rendering starts.
- Canonical terminal queues retain at most the admitted task window. One next
  refinement may run while preceding terminal workers finish; it cannot emit
  ahead of those leaves. The authenticated parent/child frontiers remain owned
  until the canonical cleanup prefix advances. Larger sort records/merge groups
  use the admitted coordinator fallback; ordinary worker runs/groups are capped
  at 512 KiB. Native terminal jobs have a separate complete fixed-T allowance.
- `BulkTagger` reuses one AES context and at most 4,096 identities per terminal
  window. Secured ordinals, exact coverage, tags, greedy bundle admission,
  normalized verification/recovery hashing and final publication stay with the
  coordinator. One compression service shares the fixed CPU partition.

Executable local scenarios are in
[test_parallel_parser.py](../../tests/test_parallel_parser.py),
[test_parser_protocol.py](../../tests/test_parser_protocol.py),
[test_direct_scatter.py](../../tests/test_direct_scatter.py),
[test_worker_scatter.py](../../tests/test_worker_scatter.py),
[test_scatter_protocol.py](../../tests/test_scatter_protocol.py),
[test_parallel_terminals.py](../../tests/test_parallel_terminals.py),
[test_bulk_tags.py](../../tests/test_bulk_tags.py),
[test_parallel_sort_policies.py](../../tests/test_parallel_sort_policies.py),
[test_parallel_pipeline.py](../../tests/test_parallel_pipeline.py), and
[test_parallel_workflow_faults.py](../../tests/test_parallel_workflow_faults.py).
They retain the parent independent serial/wire oracles. Local passes are not
platform qualification, proof of throughput improvement, or the full supplied-data run.
