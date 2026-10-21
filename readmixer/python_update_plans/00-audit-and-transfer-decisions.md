# Plan 00 — Audit and transfer decisions

Status: Planned. Source inspection is recorded below; remediation and release
qualification remain future work.

Prerequisite: access to the two source trees identified in
[source-snapshot.json](source-snapshot.json). This plan owns the defect register
and design-transfer rationale. It does not authorize changes to the Rust
implementation or execution on private datasets.

## Inspected baseline and evidence limits

The Python baseline is HEAD
`6454d21b4566d34e7dbcfadedb6a6f3e412f5beb`, with the executable source inspected
under `readmixer/`. The Rust baseline is HEAD
`a886706254786b8af4980c5e5afbe990658e89dc` plus the substantial working-tree
changes fingerprinted in the snapshot. Source line references below describe
that snapshot; verify them again if either tree changes.

The audit covered the Python CLI, complete scrambling and utility modules,
tests, dependency files, and relevant documentation. Rust inspection followed
catalog construction, byte parsing, pair framing, sharding/refinement, resource
admission, BGZF output, private mappings, publication, verification, recovery,
tests, and development/release plans. Fingerprinting adjacent files does not
mean every function was executed or independently proven.

Rust's [provenance record](../provenance/readmixer-rewrite.json) identifies a
complete rewrite with separate Git ancestry. The flawed Python predecessor is
not a byte-compatibility oracle. Rust's current paired-auto path and v3 recovery
are source-activated, but [Rust Plan 12](../plans/12-paired-mixing-validation-and-release-closure.md)
still withholds final release qualification.

Planning used source inspection and small symbolic/in-memory checks. The future
test IDs below are requirements, not a claim that the new implementation or its
acceptance suite already exists. No real reads or source sample mappings belong
in this directory.

## Confirmed Python defect register

Severity indicates implementation priority: P0 is a correctness/data-loss or
misleading-success blocker; P1 prevents safe reliable use at scale; P2 concerns
supportability and evidence. It is not an externally assigned vulnerability
score.

### PY-01 — The apparent shuffle is a no-op (P0)

At [scramble.py:241](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:241),
`perm` is a NumPy array, but `crypto_random.shuffle(perm.tolist())` shuffles a
new list and discards it. The original identity permutation is used at line 306.
Changing the random-number source would not fix this discarded-copy defect.
Owner: [04](04-global-pair-randomization-and-sharding.md), test `AUD-01`.

### PY-02 — Chunk concatenation is not global randomization (P0)

[Contiguous chunk boundaries and ordered collection](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:388)
feed [concatenation](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:463).
Even repairing PY-01 leaves records confined to their original chunks. There is
no global external permutation despite the function's external-sort name.
Owner: [04](04-global-pair-randomization-and-sharding.md), `AUD-02`.

### PY-03 — There is no atomic paired-sample model (P0)

[Input accounting](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:201)
assigns independent file/read ranges, and
[processing](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:277)
creates tags for individual reads. Neither catalog nor frame binds R1 to R2.
Giving two separate shuffles the same seed is not a replacement for validated
pair identities. Owners: [02](02-paired-input-and-lossless-parsing.md), 04, 06;
`AUD-03`.

### PY-04 — Parsing can accept malformed or silently truncated records (P0)

[Counting](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:217)
checks only divisibility of line count by four. The
[generator](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:348)
does not validate `@`, `+`, equal sequence/quality lengths, or mate identifiers;
it silently stops on a partial terminal record. It also decodes all input as
UTF-8 instead of preserving accepted bytes. Owner: 02; `AUD-04`.

### PY-05 — Missing final newlines can corrupt and drop records (P0)

The parser accepts a final non-newline quality string. The
[worker writes it unchanged](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:312),
so a following header can become part of that quality line. The
[four-line merge](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:473)
does not reject a trailing incomplete group. A symbolic two-record check
demonstrated seven merged lines and inconsistent output/key counts. Owners:
02, [06](06-bundles-keys-indexes-and-manifests.md); `AUD-05`.

### PY-06 — Original metadata is irreversibly truncated (P0)

[Descriptions longer than 100 characters](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:321)
are replaced with first/last fragments before writing keys. This is lossy
truncation, not compression. Original header restoration is impossible from
those rows alone. Owners: 02, 06, [08](08-multi-sample-recovery.md); `AUD-06`.
The new release must not advertise a tool that recovers information already lost
by an old pool.

### PY-07 — Original plus lines remain in mixed output (P1)

The [worker copies the original plus line](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:315).
It can repeat the original identifier or other source metadata. Store it
losslessly in private mappings and emit a bare plus marker in the pool.
Owners: 02, 06; `AUD-07`.

### PY-08 — The primary FASTQ identifier is repeated (P1)

[Headers use `@ReadMixerSecured {tag}`](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:312).
Readers using the first whitespace-delimited token see the same identifier for
every record. Owners: 04, 06; `AUD-08`.

### PY-09 — Repeated basenames conflate sample mappings (P0)

[Chunk key paths/dictionaries](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:257),
[aggregation](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:384),
and [final key names](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:531)
use basenames. Two directories containing the same filename share ambiguous
dictionary identities and truncating output paths. The exact lost rows depend
on execution/buffering; the ambiguity is unconditional. Owners: 02, 06;
`AUD-09`.

### PY-10 — Memory use is not bounded by the advertised budget (P1)

The [whole-run mapping](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:450)
stores an entry per output read. `pool.map` retains every result, and the
parent builds all chunk arguments and result metadata. The
[fixed 1200-byte estimate](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:128)
only produces a warning when exceeded. The
[10,000-read floor](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/util.py:269)
can exceed a caller's tiny RAM budget, and automatic selection has a 1 GiB floor
even when available memory is lower. Owners:
[03](03-resource-admission-and-storage.md), [09](09-parallel-execution-and-performance.md);
`AUD-10`.

### PY-11 — Chunk processing repeatedly decompresses input prefixes (P1)

After an initial full counting pass, each
[chunk reopens input and skips earlier reads](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:289).
For one input of N reads split into m equal chunks, chunk processing decodes
`N * (m + 1) / 2` reads in total, of which `N * (m - 1) / 2` are discarded
prefix reads; the initial count adds another N reads. One hundred chunks can
therefore decode about 50.5 input equivalents during processing. This is a
structural cost, not a measured speedup prediction. Owners: 02, 04, 09;
`AUD-11`.

### PY-12 — Failure can be reported as completion (P0)

[Top-level handling](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:170)
logs errors without rethrowing; other branches return early. The
[CLI](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/readmixer.py:35)
then prints completion. Compression also
[swallows failure](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:603).
Owners: [01](01-packaging-interfaces-and-compatibility.md),
[05](05-encrypted-scratch-and-safe-publication.md); `AUD-12`.

### PY-13 — Private-directory creation does not protect every file (P1)

`mkdtemp` at [line 79](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:79)
already creates a private temporary directory; do not misreport it as public.
Its payloads are nevertheless plaintext, and final keys and FASTQs are opened
with ordinary ambient-umask permissions. There is no authenticated temporary
format. Owner: 05; `AUD-13`.

### PY-14 — Output publication and compression are not transactional (P0)

Final FASTQs and keys use
[direct truncating opens](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:459).
Compression [deletes originals incrementally](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:583)
before the whole operation is committed. There is no marker-last protocol,
artifact inventory, exclusive publication, or identity-aware rollback.
Owner: 05; `AUD-14`.

### PY-15 — Short random tags have no uniqueness mechanism (P1)

[Seven calls to four-base generation](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/util.py:68)
produce 28 bases, or 56 random bits, per tag. Only half the bits drawn by each
`randbits(16)` call are used. No collision check exists. Under independent
uniform draws, the birthday probability is approximately 6.7% at 100 million
tags and 99.9% at one billion. Duplicate tags do not necessarily lose data if a
separate correct positional map exists, but the claimed unique label is not
guaranteed. Owners: 04, 06; `AUD-15`.

### PY-16 — The file-size promise is neither decimal nor universal (P1)

[The splitter](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:444)
uses `5 * 1024**3`, permits an oversized first record, and caps only the plain
FASTQ. Keys and compressed physical artifacts have no corresponding guarantee.
Owners: 06, 08; `AUD-16`.

### PY-17 — Disk checks use guesses and can inspect the wrong filesystem (P1)

[Fixed expansion ratios](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/util.py:179)
cannot guarantee space. A failed capacity query
[invents 10 GiB of free space](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/util.py:96).
The [calling path](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:105)
may check the input directory while default output goes to the working directory
and scratch goes elsewhere. Owner: 03; `AUD-17`.

### PY-18 — Keys are not a recovery or verification implementation (P0)

The complete [CLI](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/readmixer.py)
and `process` module expose scrambling only. No function joins mappings to
output, reconstructs samples, proves exact coverage, or verifies physical
digests. A CSV being present does not establish recoverability. Owners: 01,
[07](07-complete-verification.md), 08; `AUD-18`.

### PY-19 — Tests miss the central correctness contract (P1)

The [integration test](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/tests/test_readmixer.py:197)
uses one small input and checks existence, line counts, and a header prefix.
It does not detect the no-op shuffle, global locality, mate separation, lossy
metadata, partial publication, or failed recovery. Owner:
[10](10-acceptance-documentation-and-release.md); `AUD-19`.

### PY-20 — Packaging and documentation overstate the execution contract (P2)

[The script's argument surface](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/readmixer.py:22)
depends on working-directory imports and exposes units inconsistent with their
implementation. Runtime dependency lists include optional charting packages.
README/security claims need to follow demonstrated behavior rather than the
presence of cryptographic packages or removed dependency advisories. Owners:
01, 10; `AUD-20`.

### PY-21 — Empty-file counting can exit with success status (P0)

[Both counting helpers](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/util.py:39)
refer to an unassigned `i` for an empty file, then bare `except` calls `exit()`
without a failure code. An empty decoded gzip stream has the same issue.
Owners: 01, 02; `AUD-21`.

### PY-22 — Resource monitoring is not continuous process-group enforcement (P1)

The [pipeline samples memory before and after processing](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:377),
while [the utility watches host-wide usage](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/util.py:207).
This is not process-group admission, continuous limit enforcement, or the claimed
graceful degradation. Owners: 03, 09; `AUD-22`.

### PY-23 — Every chunk creates metadata for every input (P1)

[Key initialization](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:251)
opens a file per input even when that input contributes no record to the chunk.
The parent retains a result entry for every such chunk/file combination. This
multiplies file descriptors, empty files, and metadata by chunk count and input
count. Owners: 03, 06, 09; `AUD-23`.

### PY-24 — Discovery cannot establish safe paired input identity (P1)

[Discovery](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:182)
accepts any `.gz` filename but lacks `.fq`, paired roles, codec magic validation,
physical-file duplicate checks, and a bounded explicit catalog. An extension is
not proof of FASTQ content or sample identity. Owner: 02; `AUD-24`.

## Transfer decisions and Python adaptations

Transfer these Rust designs as requirements, then validate their Python
implementation independently:

- [paired_catalog.rs](../../crates/readmixer-core/src/paired_catalog.rs),
  [fastq.rs](../../crates/readmixer-core/src/fastq.rs), and
  [paired_frame.rs](../../crates/readmixer-core/src/paired_frame.rs): canonical
  samples, lockstep mate checks, byte parsing, and indivisible pair frames.
- [shard.rs](../../crates/readmixer-core/src/shard.rs) and
  [paired_pipeline.rs](../../crates/readmixer-core/src/paired_pipeline.rs): one
  keyed shard namespace, bounded recursive refinement, canonical traversal,
  and preservation of natural counts. Python uses bulk cryptographic rank-sort
  instead of a Python-level Fisher–Yates loop in the production path.
- [budget.rs](../../crates/readmixer-core/src/budget.rs),
  [plan.rs](../../crates/readmixer-core/src/plan.rs), and
  [memory_store.rs](../../crates/readmixer-core/src/memory_store.rs): explicit
  admission, accounting for metadata, and memory/external storage. Python must
  add interpreter, IPC, and native-array workspace charges; Rust allocation or
  memory-lock guarantees do not transfer automatically.
- [bgzf.rs](../../crates/readmixer-core/src/bgzf.rs),
  [paired_bgzf.rs](../../crates/readmixer-core/src/paired_bgzf.rs),
  [paired_format.rs](../../crates/readmixer-core/src/paired_format.rs), and
  [paired_output.rs](../../crates/readmixer-core/src/paired_output.rs): bounded
  compressed members, synchronized artifact caps, complete private mappings,
  and one shared compression budget.
- [output.rs](../../crates/readmixer-core/src/output.rs) and
  [workers.rs](../../crates/readmixer-core/src/workers.rs): no-overwrite
  publication, marker-last completion, causal failures, cancellation, and
  identity-aware cleanup. These are per-file atomic operations plus a commit
  marker, not one globally atomic multi-file rename.
- [verify_v3.rs](../../crates/readmixer-core/src/verify_v3.rs),
  [recovery_engine.rs](../../crates/readmixer-core/src/recovery_engine.rs), and
  [recovery_output.rs](../../crates/readmixer-core/src/recovery_output.rs):
  cross-artifact joins, original-order recovery, and explicit assurance scopes.
  Python changes the index to physical order and gathers a multi-sample union
  once rather than repeatedly running Rust's single-sample operation.

Do not copy Rust's wire schema, BLAKE3 dependency, optional authenticated
plaintext scratch mode, single-sample public recovery restriction, independent
record compatibility engine, or unqualified throughput claims. The new Python
schema uses SHA-256 digests and standard keyed BLAKE2b, with existing native
dependencies for bulk arrays and cryptographic primitives. It retains encrypted
on-disk scratch in every supported execution mode that spills.

Do not transfer [Rust Plan 14](../plans/14-adding-database-to-readmixer.md) as an
implemented feature. Rust Plans 12/13 are useful release-discipline references,
not acceptance evidence that Python can inherit.

## Implementation handoff, interfaces, and regression evidence

Plan 00 adds no runtime API. It provides stable `PY-01..PY-24` defect IDs and
`AUD-01..AUD-24` regression obligations consumed by Plan 10. A later change must
update this register if it invalidates a finding or reveals another failure.
Keep differences between static reasoning, a symbolic reproduction, a passing
unit test, and a completed release benchmark explicit in evidence records.

Each `AUD-NN` test must exercise the corresponding failure mechanism or its
replacement contract; a file-exists assertion is insufficient. Structural
shuffle tests must expose forbidden chunk locality without claiming that every
random shuffle must change every position. Tiny uniformity checks use controlled
randomness; statistical failures use predeclared thresholds and fixed fixtures.

For old behavior that can be reproduced safely, capture a minimal symbolic
fixture or a source-extracted in-memory probe in the future Python test suite.
Do not keep the broken engine in production merely to retain those tests. Use
independent expected identities/bytes as the new implementation's oracle.

## Completion criteria and remaining limits

- [ ] Every defect has a remediation owner and executable regression in Plan 10.
- [ ] Changed source fingerprints have been reviewed before dependent work.
- [ ] The implemented Python contracts satisfy the accepted scope without
  reviving old chunk-local or independent-mate behavior.
- [ ] Documentation distinguishes Rust design references from measured Python
  evidence and records intentional compatibility breaks.
- [ ] No claim is made that lost legacy metadata can be reconstructed.

The audit does not establish biological anonymity, malicious-replacement
authenticity, secret zeroization in CPython, hard RSS enforcement on every OS,
or a particular speedup. Those would require different guarantees and evidence.
