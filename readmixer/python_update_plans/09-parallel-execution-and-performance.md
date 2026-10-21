# Plan 09 — Parallel Execution and Performance

Status: Planned. Performance targets, test cases, and qualification procedures
here do not imply measured throughput or completed implementation.

Prerequisites: [Plan 03](03-resource-admission-and-storage.md) provides admission,
[Plan 04](04-global-pair-randomization-and-sharding.md) fixes ordering, and
[Plan 05](05-encrypted-scratch-and-safe-publication.md) provides storage/lifecycle.
Implement the artifact, verification, and recovery contracts in Plans 06–08
before treating optimized execution as release-ready.

## 1. Existing bottlenecks and transfer boundary

Python reopens each source and decodes its prefix for every contiguous chunk
([scramble.py:289](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:289)).
For N records in m equal chunks that consumes N*(m+1)/2 records, besides the
initial count pass at
[scramble.py:93](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:93).
The parent materializes all task results
([scramble.py:404](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:404)),
and every chunk opens keys for every input, even absent sources
([scramble.py:251](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:251)).
Random tags issue seven entropy requests per read
([util.py:68](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/util.py:68)).
These costs are avoidable, but faster execution of that no-op shuffle is not a
correctness or throughput baseline for a real global permutation.

Transfer deterministic task identities, bounded ordered completion, and clean
worker shutdown from Rust [scatter.rs](../../crates/readmixer-core/src/scatter.rs),
[paired_pipeline.rs](../../crates/readmixer-core/src/paired_pipeline.rs),
[bgzf_output.rs](../../crates/readmixer-core/src/bgzf_output.rs), and
[recovery_output.rs](../../crates/readmixer-core/src/recovery_output.rs).
Use [plan.rs](../../crates/readmixer-core/src/plan.rs) for admission invariants.
Do not port scoped-thread assumptions, Rust allocator sizes, or throughput
claims to the Python interpreter.

## 2. Coordinator and CPU ownership

Implement execution.py with StageScheduler, WorkerJob, WorkerResult,
CancellationState, and CompressionService. The coordinator owns final paths,
the transaction, global leases, stage transitions, and successful completion.
Workers cannot publish manifests or independently declare the run successful.

Use multiprocessing.get_context("spawn") explicitly on Linux and macOS.
Keep module import and the CLI main guard safe for spawned interpreters.
Do not inherit live input/output descriptors, buffered cipher objects, or
uncontrolled NumPy state through fork. A worker receives only its assigned
ephemeral keys in bounded in-memory messages; never persist these messages.

Resolve one CPU budget from logical CPUs, affinity where available, cgroup
quota where applicable, and the optional --workers upper bound. Reserve
at least one runnable slot for a positive fractional quota. Memory and
descriptor admission may lower concurrency further. Record the effective
limit and reason; a worker count is not a promise to saturate every stage.

All CPU-heavy processes and compression threads draw tokens from that one
budget. Count a coordinator performing parsing/rendering as active work.
Use one shared compression service per invocation, not one thread pool per
stream or worker. Do not multiply process count by compression thread count.
With one effective worker, run synchronously without background CPU work.
The first process implementation uses the fixed invocation partition and
execution-profile-1 reporting rules in
[Plan 13](13-spawn-workers-and-bounded-storage-handoffs.md); it does not add
independently measured stage maxima into a misleading concurrency total.

Set native BLAS/OpenMP thread limits to one in child bootstrap before NumPy
imports, and avoid unnecessary linear-algebra dependencies in sort paths.
The release benchmark must observe native thread counts; environment settings
are not evidence that a wheel obeyed them. Unexpected threads trigger review
of the supported build's CPU and memory envelopes.

## 3. Stage schedule

1. Build the bounded catalog once. For mix, decode each sample's two streams
   in lockstep once; accumulate counts and normalized hashes during that pass.
   A separate user-requested inspect operation may decode independently, but
   mix does not perform a count pass followed by repeated prefix scans.
2. Assign complete samples to admitted parser/scatter workers. Identity comes
   from the canonical catalog and pair ordinal, never task completion order.
   For direct scatter, admit complete 64-writer sets, all retained buffers,
   a shared transient flush workspace, and input/control descriptors under
   Plan 03. Lower concurrency to fit; use the canonical sorter-scatter fallback
   when no direct set can be admitted. Rotate entire sets at bounded batch
   boundaries as specified by [Plan 14](14-deterministic-parallel-pipeline.md).
   No per-record writer eviction or input prefix replay is introduced.
3. Seal input scatter segments, validate end counts, then refine over-budget
   buckets. Use a bounded/paged frontier and depth-first scheduling where useful.
   Canonical parent/child path order is independent of scheduling.
4. Load admitted terminals, restore canonical input identity order, generate
   rank rows in bulk, and run NumPy lexsort under Plan 04. Return bounded
   segment references or a charged in-memory result, not Python record graphs.
5. The coordinator consumes terminals in canonical order and emits complete
   pairs into Plan 06 bundles. Batch AES blocks and record rendering within
   leases. Admit compression blocks using fixed uncompressed boundaries.
6. Plans 07/08 gather observations or selected samples once per required
   verification/gather pass; external sorting and reconstruction use the same
   admitted scheduler. Do not create one whole-output scan per selected sample.

Worker count, completion order, and memory/external/auto backing cannot change
bucket assignment, canonical terminal limits, ranks, tags, or bundle layout.
Byte-for-byte comparisons cover each bundle's six artifact files under fixed
inputs, root, public run identifier, resolved B/T/topology, codec/build,
compression level, and artifact cap. Compare manifests semantically after
excluding execution metadata, which records worker/storage choices. Reports,
timings, and resource observations naturally differ. Changing permutation or
encoding inputs can legitimately change bundle bytes.

Ordinary gzip decoding remains sequential within each source. Parallelism
comes from independent sample pairs and later stages; do not promise random
access or parallel decompression of an arbitrary gzip stream. BGZF indexed
access belongs to the artifact/recovery format, not the old input chunk reader.

## 4. Bounded IPC and result ordering

WorkerJob carries an operation identifier, canonical task identity, small
configuration, lease identity, and input references. Its schema and encoded
size are bounded. Do not pickle entire catalogs, arenas, or lists of records
for every job. WorkerResult carries status, expected counts/digests, and a
bounded reference; variable-size registry information streams into paged storage.

External references name authenticated segments under the private scratch
root and include expected identities. Workers reopen only validated references.
MemoryRef identifies an owner, lease, generation, and byte length; it is not a
filesystem pathname. The first implementation transfers memory data through
bounded raw-byte channels in chunks of at most 1 MiB. Reserve sender retention,
receiver destination, transport buffers, and copy overlap before transfer.
Release sender ownership only after receiver acknowledgment or coordinated
cancellation. Never let ordinary multiprocessing queues invisibly buffer arenas.

If a complete memory transfer cannot be admitted, lower concurrency or fail
in strict memory mode. Auto may spill through Plan 05 before sending an external
reference. Do not introduce plaintext disk staging or unaccounted shared-memory
objects as an IPC workaround. A later shared-memory adapter requires explicit
lifetime, accounting, crash-cleanup, and confidentiality qualification.

Maintain at most two submitted jobs per admitted worker and a matching
byte-limited completion window. A completed out-of-order terminal keeps its
lease. Stop submitting when the earliest missing task prevents ordered drain;
never grow a dictionary until all tasks finish. Deliberate writer closure at a
stage/batch boundary seals the segment; reopening creates a fresh identity/key
generation, not a reset nonce counter. Do not use per-record eviction as the
normal substitute for complete writer-set admission.

Compression jobs contain one bounded block and its stream/block ordinal.
Encoded BGZF blocks enter a globally admitted completion window of at most
two blocks per compression slot. Each stream drains in block order before
recording compressed offsets or .gzi entries. The same pool serves R1, R2,
keys, and recovery streams without separate unbounded reorder queues.

## 5. Failure and cancellation protocol

Define one coordinator-owned first-error record. It contains a typed safe
failure, stage/task identity, and bounded diagnostic context, without original
headers, sequences, quality bytes, keys, or serialized job payloads.
Subsequent worker and cleanup errors attach to it; they do not replace it.

Workers check cancellation between bounded input blocks, frame batches,
terminal stages, and compression jobs. Native work on a bounded block may finish
before returning; do not promise instantaneous interruption inside lexsort/zlib.
An unexpected worker exit, broken pipe, malformed result, or failed
authentication cancels work. The first release has no automatic per-task
execution timeout: slow admitted work alone is not evidence of worker failure.
Explicit cancellation still enforces the shutdown deadlines below.

On error or SIGINT/SIGTERM: stop new admissions and submissions, signal workers,
close/drain channels without waiting for a blocked producer indefinitely, and
join cooperatively. Use a 10-second shutdown grace period, then terminate,
wait up to another 5 seconds, kill remaining workers, and reap them. Only after
writers are stopped may Plan 05 remove owned scratch/partials. Preserve exit
status and primary error. Completion callbacks cannot publish after cancellation.

On successful completion, drain work, verify all source seals and output
invariants, join workers, and commit the last completion marker. Operational
logging after a committed transaction must not trigger destructive rollback.
SIGKILL residue follows Plan 05; this design has no automatic resume.

## 6. Correct reference and benchmark protocol

First implement an independent, small-data serial reference for the same
canonical catalog, transcript, resolved topology, tag construction, and output
contract. It may use slower Python containers within an explicit fixture limit.
Compare its six bundle files byte-for-byte and its manifest semantically,
excluding execution metadata, against the optimized implementation before using
it as a benchmark. Do not require equal reports, timings, or resource observations.
For larger cases, compare optimized --workers 1 against admitted parallel modes
and verify every run. Do not extrapolate a memory-limited reference to inputs
it cannot admit, or use historical Python's unchanged-order output as an oracle.

Benchmark only generated, nonclinical synthetic fixtures with reproducible
generators. Include unequal sample sizes, plain/gzip input, varied record/header
lengths, high and low compressibility, many small files, long valid records,
forced refinement, auto spill, and many-sample recovery. Tiny tests must include
the same code paths without requiring production-sized datasets.

Separate parser, frame codec, bucket hashing, rank generation, lexsort, tag
rendering, scratch AEAD/codec, BGZF compression, indexed gather, sort merge,
and end-to-end measurements. Report wall time, CPU time, normalized bytes/sec,
physical bytes/sec, peak aggregate RSS, scratch peak bytes, bytes read/written,
descriptor peak, thread/process counts, and stage stalls/backpressure.
Record fixture dimensions, Python/dependency/zlib versions, OS, CPU budget,
storage medium, cache treatment, B/T, workers, and compression settings.

Use at least three timed repetitions after one explicitly identified warm-up
for stable local comparisons; publish medians and ranges, not an invented
precision claim. Keep cold-cache and warm-cache results separate. Include
startup costs in end-to-end numbers and identify any excluded setup work.
Measure correctness and resource limits in the same configuration as speed.
No throughput number or speedup claim is accepted before measurements exist.

Optimization order: remove redundant decoding and full-dataset maps; bulk
crypto/rank/array operations; bound I/O buffers; overlap admitted independent
work; then tune compression. Add each optimization only with the reference
equivalence defined above and a measured benefit on relevant synthetic cases.
Do not enlarge dependency
or runtime complexity merely to imitate Rust's implementation.

## 7. Tests and completion

- P09-T01: spawn-safe CLI/library execution on Linux/macOS; --workers 1 has
  one CPU-heavy execution path; affinity/quota/native-thread limits are honored.
- P09-T02: under the fixed inputs/root/run ID/topology/codec/build/level/cap
  context above, compare all six bundle files exactly across workers 1, 2, 4,
  delayed completions, and memory/external/forced-auto-spill backends. Compare
  manifests semantically excluding execution; allow reports/timings to differ.
- P09-T03: gzip sources are decoded sequentially once during mix; instrumented
  readers detect reopen/prefix replay and per-selected-sample recovery rescans.
- P09-T04: blocked earliest tasks keep queue/reorder/IPC/native buffers within
  leases; many samples and shards do not create unbounded job/result catalogs.
- P09-T05: worker exceptions/exits, broken channels, cancellation during each
  stage, and forced termination produce no false manifest/receipt or live writer.
- P09-T06: one shared compression pool preserves exact bytes/offsets; injected
  compression failures stop publication and preserve the originating error.
- P09-T07: benchmark records contain versions, validation results, resource
  peaks, and repeat statistics; historical no-op shuffle is never the oracle.

- [ ] Reference and optimized bundle bytes agree under fixed context; manifests
  agree semantically excluding execution, without equating runtime reports.
- [ ] CPU, descriptor, memory, and completion-window admission is integrated.
- [ ] Cross-platform cancellation and determinism matrices pass.
- [ ] Reproducible measurements support every published performance claim.
