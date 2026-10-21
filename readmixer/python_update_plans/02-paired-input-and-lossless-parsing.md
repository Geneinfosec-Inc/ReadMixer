# Plan 02 — Paired input and lossless parsing

Status: **Planned**. This document specifies future Python work; it does not report
implemented behavior or release qualification.

## Purpose and prerequisites

Replace independent-file discovery and repeated text scans with a canonical
paired-sample catalog and one-pass, bounded, byte-oriented decoding. A mixing run
must contain at least two nonempty samples, each represented by exactly one R1/R2
file pair. Preserve every admitted record byte except the documented line-ending
normalization. Read positions are shuffled later as whole pairs.

- Implement the configuration/error boundaries from [Plan 01](01-packaging-interfaces-and-compatibility.md).
- Obtain catalog, parser, descriptor and decoded-byte admission from [Plan 03](03-resource-admission-and-storage.md).
- Send validated pair views to [Plan 04](04-global-pair-randomization-and-sharding.md).
- Use the protected storage/publication boundary in [Plan 05](05-encrypted-scratch-and-safe-publication.md).
- [Plan 06](06-bundles-keys-indexes-and-manifests.md) owns final headers and public mapping formats.
- This first implementation is paired-only on Linux/macOS, Python 3.11 or later.
  Legacy pools, Rust wire compatibility, lane concatenation and interleaved input
  are outside this plan.

## Audited predecessor defects and Rust transfer anchors

The predecessor's [discovery:182](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:182)
accepts every `.gz`, scans only one directory level below the root, and has no
sample or mate model. Its [counting pass:201](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:201)
checks line-count divisibility rather than FASTQ structure. Each
[chunk reader:289](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:289)
reopens inputs and skips their preceding reads. Its
[record reader:337](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:337)
decodes UTF-8 and silently stops on incomplete records. The
[description transformation:321](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:321)
irreversibly truncates long descriptions, while basename-indexed dictionaries at
[key creation:251](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:251)
can conflate unrelated files.

Transfer the Rust concepts of [physical-file uniqueness](../../crates/readmixer-core/src/paired_catalog.rs#L182),
[canonical grouping](../../crates/readmixer-core/src/paired_catalog.rs#L283),
[strict byte framing](../../crates/readmixer-core/src/fastq.rs#L63),
[mate normalization](../../crates/readmixer-core/src/paired_frame.rs#L20), and
[lockstep scatter](../../crates/readmixer-core/src/paired_pipeline.rs#L957).
The two-stage nonempty requirement is implemented at
[catalog admission](../../crates/readmixer-core/src/pipeline.rs#L586) and
[decoded-count admission](../../crates/readmixer-core/src/pipeline.rs#L777).
These are source anchors, not evidence that the new Python contracts below exist.
Recursive discovery and sheet-relative roots intentionally differ from Rust.

## Input-source and catalog contract

1. `mix --input DIR` and `mix --samples TSV` are mutually exclusive and exactly
   one is required. For automatic discovery, DIR is the input root. For a sample
   sheet, the sheet's parent directory is the input root; every declared mate
   resolves relative to that directory. Do not use the process working directory.
2. Automatic discovery visits the root and descendant directories recursively.
   Never follow directory or leaf symlinks. Reject a selected input root that is
   itself a symlink. Reject a discovered symlink rather than silently omitting an
   apparent mate. Detect repeated directory identities before descent.
3. Recognize lowercase `.fastq` and `.fq`, with optional `.gz`. Ignore other
   ordinary files. A recognized FASTQ filename that cannot form an admitted pair
   is an error, not an independent-record fallback.
4. Supported paired stems are `ID_R1`/`ID_R2`, `ID_R1_001`/`ID_R2_001`, and
   `ID_1`/`ID_2`. Group by physical parent directory and ID; require one mate of
   each role from the same naming family. Never pair orphans across directories.
5. Sample IDs use the schema's bounded ASCII grammar:
   `[A-Za-z0-9][A-Za-z0-9._-]{0,127}`. They are therefore valid UTF-8. Sort IDs by
   their exact UTF-8 bytes, case-sensitively, without Unicode normalization or
   locale collation. Reject duplicate IDs across the entire catalog.
6. Reject recognizable multilane names, including sample IDs ending `_Lddd` and
   filenames whose `_Lddd` segment immediately precedes a recognized mate suffix.
   Do not reinterpret separate lanes as separate samples. Explicit sample sheets
   do not waive this rule. Arbitrary custom naming cannot prove lane provenance;
   one declared file per role remains the enforced contract.
7. The TSV header is exactly `sample_id\tr1\tr2`. Require UTF-8 without a BOM,
   exactly three nonempty fields per row, and no quoting, comments or blank rows.
   Accept LF or CRLF and a complete final row without LF. Reject duplicate IDs.
8. TSV filenames may use custom stems/extensions. Declared paths must be relative,
   nonempty, NUL-free, and contain no `..` component. Reject absolute paths,
   directories, symlink components and leaf symlinks. Require the opened object to
   remain within the sheet-parent input root. Do not expand `~` or environment
   variables. The sheet itself must be a regular nonsymlink file.
9. Reject reuse of any physical input file, including hard links, across roles or
   samples. Use `(st_dev, st_ino)` for identity on supported platforms; basenames
   are descriptive metadata only. For mix, validate resolved paths before any
   creation: neither the output destination nor the configured temporary directory
   may equal or lie beneath the admitted input root. Ancestors of the input root
   are allowed only when the newly created private staging/scratch child lies
   outside the input tree. Recheck that child against the anchored input root
   before writing; generated outputs must never replace an input. These creation
   rules do not apply to inspect, which writes nothing.
10. Assign dense sample indices in canonical ID order, starting at zero. Enforce
    at most 10,000 samples, 20,000 physical mate files and a 16 MiB encoded catalog,
    together with actual retained/transient metadata admission from Plan 03.
    Limit each TSV line to 16 KiB before decoding or splitting it.

Discovery must admit each new directory/input/catalog entry before retaining its
metadata. Use an iterative traversal with a bounded frontier; do not accumulate
an unbounded list of all directory entries or rely on Python recursion depth.
Filesystem enumeration and TSV row order must produce the same final catalog.
Diagnostics identify an input by admitted numeric identity or a sanitized path;
sample IDs, paths and record metadata remain private run information.

## Opening and compression policy

Inspect the already opened descriptor's initial bytes. Gzip magic selects gzip
decoding; otherwise select plain bytes, with the parser validating the content.
A `.gz` filename without gzip magic is an error. A gzip stream under another
explicit filename is accepted based on magic. Empty plain files are valid empty
mates; an empty file advertised as gzip is an invalid gzip stream.

The two mates may use different codecs. Accept valid concatenated gzip members,
including BGZF as an input gzip member sequence. Reject truncated members,
checksum failures and nonmember trailing junk; do not treat corruption as EOF.
Maintain compressed-byte, decoded-byte and record counters during the single
scan. Apply Plan 03's per-record and actual aggregate decoded-size guards.
Do not infer decoded size from compressed size or perform a counting pass.

Open with the supported no-follow descriptor strategy from Plan 05, verify
regular-file identity against the catalog, and keep that descriptor for decoding.
Compare size/identity/change metadata at completion to detect ordinary concurrent
modification. This is not an immutable filesystem snapshot guarantee against a
writer that can modify the same file object while restoring its metadata.
An observed change fails the run; callers must supply quiescent inputs.

## Four-line byte grammar and normalization

The rules below describe `ingress` mode. Reader construction must explicitly
select `ingress` or `canonical_lf`; never infer the mode from bytes. Plans 06–08
use `canonical_lf` for produced artifacts and restored streams: remove only LF,
preserve every CR byte, and require all four line terminators. This distinction
preserves an admitted final quality payload ending in standalone CR at input EOF
when canonical rendering appends LF.

- A record has exactly four physical lines: full header, sequence, full plus
  line, and quality. Wrapped/multiline FASTQ is not supported.
- The header starts with byte `@`; the plus line starts with byte `+`.
  Retain those marker bytes and all following metadata bytes exactly.
- Sequence and quality are opaque byte strings and must have equal lengths.
  Do not decode, uppercase, alphabet-check, trim spaces or alter their contents.
  Both may be empty when the corresponding physical lines are present.
- Remove an observed LF terminator and, only when immediately preceding that
  LF, one CR. A standalone CR is payload. Canonical rendering adds one LF after
  each of the four payload lines. No other normalization is permitted.
- Accept a complete nonempty fourth line terminated by EOF instead of LF. EOF
  before any fourth-line bytes is truncation: an empty final quality line must
  have its physical LF. EOF at the start of the next header means clean end.
- The default `max_record_bytes` is 1 MiB for the entire normalized single-mate
  record: four payload lengths plus four LF bytes, including header/plus markers.
  An exact-limit record succeeds; a record one normalized byte larger fails.
- Read in bounded chunks with an overflow probe. A valid raw CRLF record needs
  at most `max_record_bytes + 4` bytes; reserve at most one further probe byte.
  Do not call unbounded `readline()` and check the limit after allocation.
- Keep one reusable backing allocation and four line ranges per mate. Any copied
  destination frame is charged separately. Raising the record limit requires
  Plan 03 admission and Plan 04's unsigned-32-bit frame-length checks.

Canonical record equality means equality after this LF normalization. It does
not mean preserving original compression, gzip metadata or original CRLF bytes.
Compute per-mate canonical digests while parsing so later recovery can prove
that full headers, plus metadata, sequences, qualities and order were retained.

## Mate identity and lockstep admission

Remove the leading `@` only from the borrowed view used for identifier parsing.
The first ASCII-whitespace-delimited token is the identity token. Its base is
that token with a terminal `/1` or `/2` removed, if present; reject an empty base.
Recognize a role from that suffix, the final metadata token ending `/1` or `/2`,
the final metadata token exactly `1` or `2`, and the first metadata token beginning
`1:` or `2:`. All recognized markers in one header must agree.

At every ordinal, require equal base identifiers. Recognized R1 markers must
declare R1 and recognized R2 markers must declare R2. Marker-free matching bases
are accepted. Consequently, swapping two otherwise indistinguishable marker-free
mate files cannot be detected; do not promise otherwise. Do not reorder records
to repair mismatches or search ahead for a matching mate.

Original pair ordinals begin at zero and advance once after both records validate.
Canonical identity is `(sample_index: u32, original_pair_index: u64)`; guard every
increment and serialization. Duplicate header text at different ordinals is not
itself a duplicate canonical identity. Reject one-sided EOF and any malformed
mate before admitting that pair to a shard.

Empty samples remain in the catalog and final private manifest with zero counts.
For mix, reject fewer than two catalog samples before scratch creation. After
decoding, reject fewer than two samples with positive validated pair counts,
before shuffle or publication. Inspect reports these counts without requiring
mix eligibility. R1 and R2 never count as separate samples.

## Proposed Python interfaces and failure ownership

- `build_catalog(input_source, admission) -> SampleCatalog` performs read-only
  discovery and canonicalization. `SampleInput` owns numeric identity, private ID,
  mate paths, inspected codecs and physical-file snapshots.
- `RecordFramer(stream, line_mode, limits).next_record(cancel) -> RecordView | None`
  requires an explicit `ingress` or `canonical_lf` mode and exposes borrowed byte
  ranges and normalized length. A view expires on the next read from that framer.
- `PairedReader.next_pair(cancel) -> PairView | None` owns both framers and returns
  two valid views with canonical identity. It never yields half a pair.
- `encode_pair_into(pair, destination)` copies a validated pair into Plan 04's
  packed frame. Consumers may not retain expired views or build a dataset-sized
  list of record objects.
- `SampleReadReport` carries counts, byte totals, maximum admitted frame size and
  separate canonical mate digests; it carries no raw record text in diagnostics.

Raise typed catalog, path, codec, FASTQ, pairing, input-change or resource errors.
Attach safe sample/mate/ordinal context without printing raw headers, sequences,
qualities or plus metadata. Cancellation is polled inside long record reads and
between pairs. A read failure cancels sibling work; Plan 09 joins workers, while
Plan 05 cleans unpublished artifacts and retains the originating error.

## Implementation steps

1. Add catalog/identity value types and bounded configuration validation.
2. Implement descriptor-safe recursive discovery and strict TSV admission.
3. Implement codec inspection and one-pass bounded byte framing.
4. Implement pure byte identifier normalization and the lockstep reader.
5. Connect reports, canonical digests, nonempty checks and packed-frame handoff.
6. Integrate cancellation/failure cleanup; replace predecessor entry points only
   through Plan 01's new paired API. Reject legacy pooled artifacts; ordinary
   original FASTQ support remains.

## Acceptance tests and completion

- `PY02-CAT-001`: all naming families, nested directories, shuffled enumeration
  and TSV row orders canonicalize identically; mixed families/orphans fail.
- `PY02-CAT-002`: duplicate IDs, hard links, reused roles, symlink directories,
  symlink leaves, traversal and recognizable lanes fail before publication.
- `PY02-CAT-003`: custom TSV filenames, sheet-relative containment, grammar limits,
  exact/overlong catalog rows and resource ceilings have explicit outcomes.
- `PY02-FQ-001`: opaque non-UTF-8 bytes, long header/plus metadata, CRLF and missing
  final LF recover to exact canonical bytes; standalone CR remains payload.
  Include final quality ending in CR at ingress EOF and its LF-only canonical
  reread; auto-detection or repeat CRLF normalization must fail this regression.
- `PY02-FQ-002`: empty sequence/quality, absent fourth line, bad markers, unequal
  lengths, wrapped records and exact/one-over record bounds exercise the grammar.
- `PY02-PAIR-001`: recognized marker combinations, conflicting roles, announced
  mate swaps, mismatched bases and one-sided EOF fail with private-safe diagnostics.
- `PY02-PAIR-002`: two nonempty samples succeed; one nonempty plus empty samples
  fails; empty samples remain catalogued in successful larger runs.
- `PY02-IO-001`: mixed mate codecs, gzip members, CRC failures, trailing junk,
  descriptor replacement and cancellation during one long record are covered.
- `PY02-IO-002`: instrumented readers prove one forward decoding pass per mate;
  no count pass, prefix rescanning or retained expired views occurs.

- [ ] Interfaces and grammar reviewed against Plans 01, 03, 04 and 06.
- [ ] Every `PY02-*` test passes on supported Linux and macOS builds.
- [ ] Failure tests assert nonzero CLI status and no published run manifest.
- [ ] Parser/cumulative allocation limits have constrained-memory evidence.
- [ ] Plan 10 records the evidence; status remains Planned until implemented.
