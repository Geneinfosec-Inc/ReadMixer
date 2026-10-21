# Plan 04 — Global pair randomization and sharding

Status: **Planned**. This is an implementation specification for a new Python
algorithm, not a report of shipped functionality or release-qualified performance.

## Purpose and prerequisites

Produce one cryptographically randomized permutation of the union of all admitted
sample pairs. Preserve both mates, original metadata and natural sample counts.
The supported swap operation changes whole-pair positions; mate orientation is
never swapped. Avoid dataset-sized Python record lists, full-input rescans and
per-output-read dictionaries.

- [Plan 02](02-paired-input-and-lossless-parsing.md) owns the canonical catalog,
  validated borrowed pair views and the two-nonempty-sample requirement.
- [Plan 03](03-resource-admission-and-storage.md) owns resource leases, admission,
  segment storage, metadata limits and the terminal working-set calculation.
- [Plan 05](05-encrypted-scratch-and-safe-publication.md) is the sole authority for
  root-key generation, exact HKDF transcripts, path encoding and scratch security.
- [Plan 06](06-bundles-keys-indexes-and-manifests.md) consumes ordered pairs and owns
  bundle layout, final headers, public wire formats and private mapping rows.
- [Plan 09](09-parallel-execution-and-performance.md) owns bounded worker execution;
  [Plan 10](10-acceptance-documentation-and-release.md) owns release evidence.

## Audited defects and implemented Rust transfer anchors

The predecessor's [permutation:241](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:241)
shuffles a temporary `perm.tolist()` and discards it, then applies the unchanged
array at line 306. Fixing that statement alone would still leave independent
consecutive chunks: [ordered pool results:388](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:388)
feed [concatenation:463](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:463).
The [read-to-part dictionary:449](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:449)
also retains an entry for every output read. These behaviors are not compatibility
requirements for the new paired-only format.

Transfer Rust's [atomic paired scatter](../../crates/readmixer-core/src/paired_pipeline.rs#L1026),
[numeric-identity bucket assignment](../../crates/readmixer-core/src/shard.rs#L337),
[canonical segment merge](../../crates/readmixer-core/src/paired_pipeline.rs#L1565),
[recursive capacity refinement](../../crates/readmixer-core/src/paired_pipeline.rs#L2166),
and [ordered terminal handoff](../../crates/readmixer-core/src/paired_pipeline.rs#L1740).
Its [terminal fit accounting](../../crates/readmixer-core/src/paired_pipeline.rs#L2400)
and [exact identity bitmap](../../crates/readmixer-core/src/paired_pipeline.rs#L2563)
are resource/correctness references, not constant-memory claims.

Python intentionally replaces Rust's
[Fisher–Yates terminal loop](../../crates/readmixer-core/src/shuffle.rs#L22)
with bulk cryptographic ranks and native NumPy sorting. It also uses a new KDF,
BLAKE2b bucket function and private frame codec. No Rust artifact-byte or RNG
compatibility is promised. Rust's source activation and unfinished release gate
are recorded separately in [the Rust plan index](../plans/README.md).

## Private atomic frame codec: PYPAIR1

PYPAIR1 is the internal plaintext payload codec inside protected scratch blocks.
It is not a public pool/key format. The enclosing storage envelope identifies its
payload type under Plan 05; do not add an undocumented per-frame magic prefix.

The fixed header is 48 bytes, in this exact order:

1. `frame_bytes: u32LE`, counting the entire header and body.
2. `sample_index: u32LE`.
3. `original_pair_index: u64LE`.
4. Eight `u32LE` line-payload lengths: R1 header, sequence, plus, quality, followed
   by R2 header, sequence, plus, quality.

The body concatenates those eight exact byte strings in the same order, without
line terminators. Headers retain `@`; plus strings retain `+`. Canonical record
rendering restores the four LF terminators per mate. Sequence/quality lengths
must match within each mate, and all Plan 02 validations remain mandatory.

For normalized per-mate limit M, the maximum frame is `40 + 2*M` bytes: the body
is at most `2*M - 8` because its eight LF bytes are implicit. Reject a configured
limit whose maximum frame exceeds u32 or a single-frame resource limit. Use
checked sums for every length and reject inconsistent total lengths, trailing
body bytes, invalid identities or truncated fields before allocating from them.

`encode_pair_into(view, destination)` copies one valid pair into a charged packed
buffer. `decode_pair_view(frame)` returns borrowed slices after validation. Neither
interface creates persistent Python objects for every record in the dataset.
Appending a pair to storage is atomic at the frame boundary; no partially admitted
pair may reach another stage. A logical frame may span Plan 05's strictly bounded
1 MiB decoded authenticated blocks, but never spans segment identities or terminal
boundaries. Authenticate every contributing block and admit the carry buffer before
reconstructing and yielding a complete frame. There is no oversized-block escape.

## Canonical identity, keys and bucket selection

Canonical identity is the dense u32 sample index and its original u64 pair ordinal.
Use a fresh 32-byte operating-system root for every production run. There is no
public seed option. Test fixtures may inject fixed roots only through the isolated
test seam in Plan 05. Never log or serialize root or derived cryptographic keys.

Use Plan 05's exact `Derive` contract. The relevant derived roots are
`root/shuffle`, `root/rank` and `root/tag`. Obtain a parent-path bucket key through
`shuffle/bucket`; obtain a terminal rank key through `shuffle/rank` with the
canonical path, pair count and attempt number. Do not duplicate the HKDF encoding
here or replace it with ambiguous string concatenation.

For each pair, the bucket-message bytes are exactly `sample_index:u32BE` followed
by `original_pair_index:u64BE`. Compute keyed BLAKE2b with `digest_size=8` using
the parent-path bucket key. Interpret the eight digest bytes as u64LE and mask
with `fanout - 1`. Do not truncate a differently sized BLAKE2b digest, use Python's
process-randomized `hash()`, or hash sequence/header/quality values for routing.
Python provides keyed BLAKE2b in [hashlib](https://docs.python.org/3/library/hashlib.html#blake2).

The virtual assignment root has an empty path, encoded by Plan 05 as depth zero.
Its fanout is exactly 64. A first-level bucket has path `(bucket,)` and depth one.
Recursive children append one component in `0..fanout-1`. Depth means component
count; the maximum is 32, allowing at most 31 refinements after initial scatter.
Every recursive fanout is a power of two between 2 and 64. A depth-limit failure
is a typed error; do not fall back to truncation, sequential output or a biased
overflow policy.

## Scatter and recursive refinement

Assign whole sample pairs to read workers using deterministic size-based LPT with
canonical sample index as the tie breaker. Within each worker, read assigned
samples in canonical order, opening each mate once. Each pair enters the common
64-bucket namespace; there are no sample-specific bucket partitions.

Workers write private protected segments and return compact segment statistics:
path, owner, frame count, encoded-byte total, maximum frame and sample reports.
Segment payloads are not pickled into task/result queues. Logical output ordering
must not depend on which worker finishes first.

For each bucket, merge segment heads by `(sample_index, original_pair_index)`.
Reject nonincreasing identities and reported-count/byte mismatches. Use a bounded
heap when multiple sources are live; reserve its buffers and descriptors. A
physical worker number may identify temporary ownership but is never an input
to bucket, rank or tag derivation. Scratch segment identities and ciphertext may
vary with ownership under Plan 05 without changing the six bundle artifact files.

A terminal must satisfy all three conditions:

- At most 32,768 pairs.
- At most 64 MiB of encoded PYPAIR1 payload.
- The entire charged terminal working set fits the canonical lease in Plan 03.

Plan 03 owns the `F + 128*n + C` admission envelope and its versioned default
lease/overhead values. F is encoded frame bytes and n is pair count. Payload is
not allowed to consume the complete lease. Canonical capacity does not shrink
when fewer workers are requested or admitted; reduce concurrency or fail instead.

For an oversized bucket with canonical lease T and overhead C from Plan 03, require
`T > C`. Set the estimated child requirement to the maximum of 2,
`ceil(n/32768)`, `ceil(F/(64 MiB))`, and `ceil((F + 128*n)/(T - C))` using checked
integer ceiling division. Round upward to a power of two and clamp to 2–64.
Merge canonical input identities and route under the new path key.
Only complete pair frames move. Do not sort by sample, repair sample balance,
assign quotas or move excess pairs to whichever child happens to have space.

The estimate is not a fit guarantee: inspect actual child statistics and repeat
until every surviving leaf fits or a limit fails. Admission includes retained
frontiers, path tables, empty segments, concurrent parent/child blocks and cleanup
ownership. Release empty buckets and obsolete parents through the store contract.
Sort the final leaves by numeric lexicographic path, independent of scheduling.

## Terminal rank-sort algorithm

1. Read and authenticate the terminal into one preallocated contiguous arena in
   canonical identity order. Allocate a contiguous u64 offsets array of length
   `n+1`, whose final sentinel is F. Check frame/count/identity statistics again.
2. For attempt 0, derive the rank key through Plan 05 using terminal path, n and
   the attempt byte. Initialize ChaCha20 at stream offset zero with the specified
   12 zero nonce bytes. That nonce is scoped to the distinct derived rank key;
   scratch encryption uses separate keys and nonce ownership.
3. Produce exactly `16*n` keystream bytes into bounded native-backed storage.
   The first `8*n` bytes form the contiguous high-limb row, and the next `8*n`
   bytes form the contiguous low-limb row. Interpret every word explicitly as
   little-endian unsigned u64. Rank i is `(high[i], low[i])`; this is not an
   interleaved sixteen-byte-per-pair layout.
4. Obtain indices with `numpy.lexsort((low, high))`. The high limb is primary
   because lexsort's last key is primary. Use numeric arrays only: no object
   arrays, floating ranks, `.tolist()`, Python integer rank lists or `numpy.random`.
5. Compare adjacent full 128-bit ranks in sorted order in bounded windows. Carry
   the last rank across window boundaries. A repeated high limb alone is valid;
   both limbs must match to constitute a collision.
6. If any collision exists, discard the entire rank batch and permutation, advance
   the attempt and regenerate all ranks. Attempts are exactly 0 through 7. After
   eight colliding batches, raise `RankCollisionError` and publish nothing. Never
   break ties by input order, sample identity or a deterministic extra field.
7. On success, release rank storage and iterate permutation indices to borrow
   complete frames from the original arena. Copy only into the charged bounded
   handoff/writer buffer, not a second shuffled arena. Check cancellation before
   and after the native sort and throughout emission.

The implementation uses [NumPy's lexsort contract](https://numpy.org/doc/stable/reference/generated/numpy.lexsort.html)
and [PyCryptodome's bulk ChaCha20 interface](https://www.pycryptodome.org/src/cipher/chacha20).
Sorting requires additional workspace; strided or byte-swapped keys may require
copies. Charge supported-build native workspace and Python overhead under Plan 03,
including collision-check gather arrays. No measured speedup is asserted here.

## Ordered handoff, tags and exact coverage

Visit final terminal paths in canonical order and number emitted pairs with one
dense global secure-pair ordinal. An `OrderedPair` handoff contains that ordinal,
the original identity and a borrowed complete frame. Both mates inherit the same
pair position. Downstream serialization must consume a view before its owner is
released; workers may return protected segment descriptors instead of live views.

Generate the pair tag using AES-256 on the exact unique 128-bit input
`b'RMP1' || sample_index:u32BE || original_pair_index:u64BE`, with Plan 05's
separate `root/tag` key. Encode the complete ciphertext as 32 lowercase hex digits.
This PRP gives distinct tags for distinct canonical identities without a global
tag set. It does not detect duplicate identities or authenticate payloads. Both
mates share the tag; Plan 06 adds its run identifier and `/1` or `/2` header role.
Recovery uses lossless private mapping rows, not decryption of ephemeral tags.

Use Plan 03's admitted exact identity coverage structure to reject duplicates,
out-of-range identities and omissions. Its bitmap option costs approximately
one bit per original pair plus sample offsets; do not call it constant memory.
Assert emitted pairs equal validated input pairs and emitted mates equal twice
that count with checked arithmetic. Part/bundle mapping is handled by Plan 06,
not by an unbounded dictionary keyed by every read.

## Distribution and determinism contract

For ideal independent uniform ranks, conditioning on all n ranks being distinct
makes every terminal permutation equally likely. Whole-batch collision rejection
preserves that symmetry. Independent uniform bucket assignment, uniform ordering
inside every bucket and canonical bucket concatenation form the global random
bucket construction. The actual PRF/CSPRNG supplies computational pseudorandomness;
do not claim literal independent unlimited entropy from a finite root.

Natural sample counts remain unchanged; local same-sample streaks and single-sample
terminal buckets are valid. Do not require alternating samples or equal shares.
Exact order is deterministic for a fixed root, canonical inputs and canonical
topology across workers, scheduling, storage paths and batching. Sample renaming,
frame-size changes or different topology may change order. An observed adaptive
tree is not a condition under which every global ordering must remain equiprobable.

With the same public test run ID, format/compression settings and supported build,
compare exact bytes of all six artifact files per bundle across worker/backing
variants. Compare manifests semantically after excluding their `execution` section:
its recorded worker and storage observations intentionally differ. Public report
timings and resource observations are outside exact-byte reproducibility.

## Implementation, tests and completion

Implement the private codec first, then serial scatter/refinement/rank-sort,
ordered coverage and tags, then Plan 09's parallel execution. All errors propagate
through the causal cancellation/cleanup path in Plans 05 and 09. No partial pair,
collision fallback or worker completion after cancellation may publish output.

- `PY04-FRM-001`: binary golden frames, zero-length sequence/quality, u32/u64 bounds,
  inconsistent lengths and maximum-record-derived frame bounds are checked.
- `PY04-BKT-001`: exact BLAKE2/KDF/path vectors, all legal fanouts and boundary
  depths; source identity is the sole per-pair routing input.
- `PY04-RNK-001`: known stream-to-high/low vectors, endian correctness, key order,
  equal-high/different-low ranks and independent stream chunk sizes are tested.
- `PY04-RNK-002`: injected collisions inside/across check windows regenerate the
  entire batch; attempt seven may succeed; eight failures publish nothing.
- `PY04-MIX-001`: unequal synthetic sample sizes preserve every identity and both
  mates across shards; examples cross predecessor chunk boundaries.
- `PY04-DET-001`: fixed canonical topology gives identical pair order under worker
  counts, scheduling, segment ownership, buffering and storage variants. Under
  fixed public test run ID/build/format settings, all six bundle files match
  exactly; manifests match semantically after excluding `execution`.
- `PY04-REF-001`: exact/one-over payload, pair-count and lease limits refine or fail
  correctly; empty buckets, maximum depth and metadata exhaustion are covered.
- `PY04-TAG-001`: fixed AES input/key vectors, identity boundary values, mate tag
  agreement and distinct canonical identities validate the injective construction.
- `PY04-ORC-001`: an independent small Fisher–Yates oracle checks structural and
  distribution properties, not equality to rank-sort's fixed-key permutation.
  Tiny exhaustive ideal-rank cases and seeded statistical fixtures supplement
  exact bijection assertions; perfect sample alternation is never an oracle.
- `PY04-FAIL-001`: errors after real segment writes, corrupted frames, duplicate
  identities and cancellation surrounding native sort leave no published run.

- [ ] PYPAIR1, KDF usage and resource accounting agree with Plans 02, 03 and 05.
- [ ] All `PY04-*` cases pass on the supported Linux/macOS dependency matrix.
- [ ] Constrained-memory tests cover Python objects and native sort allocations.
- [ ] Exact recovery and complete verification pass through Plans 07 and 08.
- [ ] Plan 10 records performance measurements and release evidence separately.
