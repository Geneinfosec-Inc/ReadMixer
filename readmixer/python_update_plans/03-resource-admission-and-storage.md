# Plan 03 — Resource Admission and Storage

Status: Planned. This document specifies future Python work; it does not claim
that the implementation, resource measurements, or acceptance tests exist.

Prerequisites: [Plan 01](01-packaging-interfaces-and-compatibility.md) fixes
configuration and failures; [Plan 02](02-paired-input-and-lossless-parsing.md)
fixes catalogs, normalized records, and pair identity. Coordinate storage with
[Plan 05](05-encrypted-scratch-and-safe-publication.md) before implementing it.

## 1. Problem and transfer decisions

The Python memory estimate is advisory and assumes 1,200 bytes per read
([scramble.py:128](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:128)).
Its merge retains one dictionary entry per record
([scramble.py:450](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:450));
the minimum chunk floor can exceed the requested allocation
([util.py:269](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/util.py:269)).
Disk estimates use fixed compressed-input multipliers and a fictitious 10 GB
fallback after a failed query
([util.py:179](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/util.py:179),
[util.py:102](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/util.py:102)).
These mechanisms cannot remain the admission authority.

Transfer explicit budgets, additive working sets, and storage-independent
logical behavior from Rust [budget.rs](../../crates/readmixer-core/src/budget.rs),
[plan.rs](../../crates/readmixer-core/src/plan.rs), and
[memory_store.rs](../../crates/readmixer-core/src/memory_store.rs).
Use the failure cases in
[paired_resource_tests.rs](../../crates/readmixer-core/src/paired_resource_tests.rs)
as requirements. Do not copy Rust allocation sizes, mlock behavior, allocator
assumptions, or zeroization guarantees into CPython claims.

## 2. Configuration and fixed limits

Implement resources.py and storage.py under src/readmixer_py. ResourceLimits
contains validated integers, never floating-point byte quantities.

- --max-ram accepts a positive decimal integer optionally followed by B, KB,
  MB, GB, KiB, MiB, or GiB; a bare integer means bytes. Decimal and binary
  suffixes retain distinct factors. Reject signs, fractions, overflow, and
  unrecognized suffixes.
- --workers is an optional positive integer upper bound; absence selects auto.
- --storage is auto, external, or memory; auto is the default.
- --temp-dir defaults to the destination for mix and recover; verify may use
  the platform temporary directory. Resolve it before allocating scratch.
- The default maximum normalized input mate is 1 MiB. An explicitly raised
  limit must fit the Plan 04 frame's u32 length and single-frame admission.
- The artifact cap is an integer in 65,564..5,000,000,000 bytes; mix defaults
  to 5,000,000,000. Recovery inherits the pool cap unless explicitly given a
  lower or equal cap. Compression levels are integers 0..9, default 6.
- The catalog admits at most 10,000 samples, 20,000 physical inputs, and 16 MiB
  of encoded catalog data. Charge its actual Python representation separately.
- Final-artifact inventory has at most 100,000 entries across all files, not
  100,000 bundles. A stricter format limit wins.
- Each manifest or recovery receipt is bounded by the minimum of 64 MiB, the
  artifact cap, and its admitted metadata lease. Reject oversized metadata.

These limits also apply to hostile manifests before count-dependent allocation.
Names, paths, arrays, completion registries, index directories, and reporting
objects need both count and byte limits; a limit on frame payload is insufficient.

--max-total-uncompressed is an optional explicit byte-size ceiling. For mix,
count actual normalized input mate bytes once, including normalized LF bytes;
absence means checked u64 counters without an invented input-size estimate.
For verify, the user ceiling counts reconstructed normalized original mate
bytes once across the complete pool, matching mix. Optional --originals parsing
has independent actual source counters but does not double-charge that semantic
ceiling. Recovery counts selected normalized original output mate bytes once.
Both operations separately enforce manifest/schema bounds on actual per-stream
decoded lengths and counts. Repeated physical reads are telemetry, not repeated
semantic charges. Reject before exceeding any applicable counter.
CSV, index, and secured FASTQ lengths use their own schema-derived bounds:
replacement headers can be longer than the original input headers.

## 3. Resolve one run budget

ResourcePlan records requested and effective ceilings, detection sources,
canonical terminal slice, descriptor allowance, and stage-specific leases.
Resolve this once before work, and record the resolved values in private
run metadata without secrets or original read identifiers.

1. Read psutil available memory and coordinator RSS. On Linux, intersect
   available headroom with applicable cgroup v2 or v1 memory limit minus
   current usage. Treat unlimited cgroups explicitly; do not parse them as
   enormous finite integers. On macOS use available memory without inventing
   a cgroup constraint.
2. Reserve min(2 GiB, 20% of available headroom) for unrelated activity.
   The detected aggregate allowance is existing coordinator RSS plus the
   remaining headroom. Intersect it with an explicit --max-ram value.
3. Set B to that effective aggregate ReadMixer allowance. Detect and report an
   explicit request above the effective ceiling; use the lower value.
4. Charge coordinator state, imported-library/native baseline, worker baseline,
   catalogs, active registries, buffers, and currently leased work. Fail if
   even one minimal execution path cannot be admitted.
5. Recheck external pressure at bounded stage boundaries. Stop admitting new
   work, spill eligible auto blocks, or cancel with ResourceError; do not
   silently change the already resolved shuffle topology.

The accounting model is an admission policy, not a hard operating-system RSS
cap. Sum process RSS conservatively for diagnostics, recognizing shared-page
double counting. tracemalloc alone cannot measure NumPy, zlib, or crypto memory.
Qualification must report aggregate parent/child RSS and versioned build data.

## 4. Canonical terminal admission

Use the following initial versioned envelope on every supported build:

    T = min(128 MiB, floor(B / 4))
    C_initial = 32 MiB
    W = arena_bytes + 128 * pair_count + C_initial

The terminal must satisfy pair_count <= 32,768, arena_bytes <= 64 MiB,
and W <= T. arena_bytes includes every encoded PYPAIR1 frame header and body,
not just sequence data. A frame is at most 40 + 2*M encoded bytes when M is
the configured normalized input-mate limit.

The 128-byte coefficient and C_initial are conservative starting envelopes,
not measured facts or proof of a hard RSS guarantee. Account explicitly for
rank rows, offsets, lexsort output and workspace, crypto buffers, decoder carry,
and native contexts; qualify or revise the versioned envelope before release.
Do not count the same temporary twice when deriving a measured replacement.

T depends only on B, never worker count, storage backend, or transient pressure.
[Plan 04](04-global-pair-randomization-and-sharding.md) refines oversized shards.
Lower concurrent worker admission to fit leases; never shrink terminal limits
because additional workers were requested. If T <= C_initial, or one pair
cannot fit a legal terminal, reject with ResourceError before final publication.
Depth exhaustion is a typed shuffle failure, not an oversized-terminal escape.

## 5. Lease interfaces and bounded metadata

Budget.acquire(category, bytes, descriptors=0) returns a context-managed Lease
or ResourceError. Leases support transfer between owners and explicit release;
ownership transfer cannot momentarily debit neither owner. Use checked integer
addition/multiplication before allocating arrays or reserving native buffers.

Stage admission reserves the complete simultaneous working set: parent,
workers, input carry, native codec state, scratch blocks, IPC, compression,
completed results awaiting order, metadata, and cache. Retaining a completed
result retains its lease. Size each queue in both items and bytes.

Resolve active descriptors to at most 1,024 and at most the soft descriptor
limit minus existing descriptors and a 32-descriptor reserve. If measurement
is unavailable, use the portable limit and a conservative explicit baseline;
if the remainder cannot serve one operation, fail before spawning workers.
Direct spill-capable scatter reserves the complete 64 bucket-writer set,
two inputs, and control handles per active worker, together with all retained
buffers and the shared transient flush workspace. Strict memory charges the
64 logical writers/buffers but only its actual input/control descriptors.
Lower direct-scatter concurrency to fit. When even one complete direct set
cannot be admitted, the bounded sorter-scatter path specified by
[Plan 14](14-deterministic-parallel-pipeline.md) is the supported fallback; it
preserves the canonical topology/order and charges its actual sort resources.
Whole-set rotation occurs at admitted batch boundaries. Neither path uses
per-record writer eviction as a substitute for its complete resource admission.

Resident segment/catalog/ownership registries have a shared ceiling of
min(64 MiB, floor(B/16)), further restricted by available leases.
[Plan 11](11-shared-metadata-budget-and-lifetimes.md) specifies the accounting
and retained-object lifetime changes implemented locally in dev3 after the dev2
baseline.
Catalogs must fit their admitted resident representation; do not silently drop
entries.
Auto/external modes page growing segment, sort-run, and ownership ledgers
through authenticated encrypted storage. Strict memory retains admitted pages
or fails. A descriptor per discovered shard or an unbounded frontier list is
forbidden, including during recursive refinement and cleanup.

Each paged ledger has one admitted trusted LedgerRoot in RAM, including its
identity, last-page reference, page/record counts, byte total, and aggregate
digest. Cap live roots at 1,024 and charge them to the same metadata allowance.
Plan 05 binds pages to ledger identity and ordinal and chains their expected
references; never recursively store a ledger's own trusted root in itself.

ExternalSorter(codec, key, lease) creates bounded sorted runs and performs
merges with fan-in <= 32 and <= descriptor and buffer allowances. Compare keys
without materializing payload copies. Its run registry is paged; no merge keeps
all record locators in a Python dictionary. Recovery, verification, and tag
uniqueness checking use this same admitted sorter.

IdentityCoverage uses checked per-sample offsets and an admitted compact bitset
when it fits. Otherwise auto/external streams fixed 12-byte identities
(sample u32BE, original ordinal u64BE) into the protected sorter and checks
exactly the expected complete per-sample ranges. Reject duplicates, gaps, and
out-of-range identities. Strict memory must admit its bitset or RAM-only sort
and retained observations, or fail; it cannot mmap a plaintext disk bitmap.
Charge counts, offsets, observations, and the final coverage pass. A bitset is
linear in pair count and is not a constant-memory guarantee.

## 6. Storage modes and transitions

SegmentStore exposes new_segment, append, seal, open_verified, and drop.
SegmentRef is a small opaque reference with expected identity/counts/digest,
not a list of records. [Plan 05](05-encrypted-scratch-and-safe-publication.md)
owns authentication, segment bytes, and lifecycle.

- memory keeps admitted segment blocks and ledger pages in process-owned RAM.
  It must fail before spilling; named shared-memory or mmap mechanisms with an
  uncontrolled file-backed lifetime are not an implicit escape.
- external writes every algorithmic disk block using authenticated encryption.
- auto starts with memory blocks, spills sealed eligible blocks when admission
  requires it, and retains the same logical byte stream and terminal order.
  Reserve overlap for encryption and migration before starting a spill.

Never evict a block still leased by a reader. Mark a spill durable and verified
before releasing its in-memory source. A failed spill retains ownership and
causes cancellation; it cannot publish an incomplete reference. Strict memory
does not reserve nonexistent scratch-disk capacity, but output-space admission
still applies. Final-format private partials are publication staging, as
distinguished from algorithmic scratch in Plan 05.

## 7. Disk admission and execution steps

Track actual logical scratch bytes and physical files on each filesystem,
including parent/child overlap while refining or merging. Group paths by
filesystem identity so destinations and scratch on one device share one pool.
Do not multiply compressed input size by a guessed expansion ratio.

Before each disk growth, check filesystem free space against the admitted
increment and existing outstanding reservations. Keep a safety reserve of
max(64 MiB, 1% of the admitted increment); reject a failed free-space query.
Reservations coordinate this process only; handle ENOSPC after a successful
check because other processes can consume space. Reserve final-output and
private partials on their actual filesystem. Release scratch charges only after
successful deletion, including intermediate merge/refinement parents.

Implementation order: validate limits; resolve B/T and descriptors; build the
bounded catalog; implement leases; add memory storage; add protected spill;
add paged ledgers and sorter; integrate all stages; then qualify the envelopes.
ResourceError carries safe category/request/available figures and preserves the
primary error when cleanup also fails. No record bytes or keys enter diagnostics.

## 8. Tests and completion

- P03-T01: size syntax, zero/overflow, cgroup variants, macOS fallback, and
  explicit request clamping with injected resource observations.
- P03-T02: terminal admission at exact boundaries and one byte over; changing
  worker count leaves T and logical order unchanged.
- P03-T03: parent/worker/native/IPC/reorder leases cannot oversubscribe; release
  and transfer accounting survive cancellation and injected allocation failure.
  Inject external memory-pressure changes during work and exercise the next
  bounded admission checkpoint without altering canonical terminal limits.
- P03-T04: auto spill matches memory/external logical output; memory fails
  before a disk scratch write; failed spill retains its source ownership.
- P03-T05: many tiny shards/runs, maximum catalogs, and many artifacts keep
  metadata/descriptor counts bounded; no per-record mapping appears.
- P03-T06: decoded-count limits, long records, hostile manifest lengths,
  ENOSPC, query failure, same-filesystem aggregation, and output overlap.
- P03-T07: qualification measures aggregate RSS on synthetic workloads and
  records Python/NumPy/zlib/PyCryptodome/platform versions and deviations.

- [ ] All storage, parser, sorter, and worker allocations have assigned owners.
- [ ] Boundary/fault tests pass with no successful over-budget admission.
- [ ] The versioned envelope is qualified; remaining RSS limitations are stated.
- [ ] CLI/resource documentation agrees with resolved limits and storage behavior.
