# Plan 06 — Bundles, keys, indexes, and manifests

Status: **Planned**. This document specifies future Python behavior; it does not
describe an implemented Python format or authorize runtime changes.

Prerequisites: [Plan 01](01-packaging-interfaces-and-compatibility.md),
[Plan 02](02-paired-input-and-lossless-parsing.md),
[Plan 03](03-resource-admission-and-storage.md),
[Plan 04](04-global-pair-randomization-and-sharding.md), and
[Plan 05](05-encrypted-scratch-and-safe-publication.md).

Consumers: [complete verification](07-complete-verification.md) and
[multi-sample recovery](08-multi-sample-recovery.md). Changes to this protocol
must update those plans and their independent test readers together.

## 1. Evidence and scope

The Python predecessor writes six-column, unversioned keys, truncates long
descriptions, and copies original plus lines into supposedly secured output:
[scramble.py:262](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:262),
[scramble.py:315](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:315),
[scramble.py:320](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:320).
Basename-keyed state aliases distinct input files, and a read-sized dictionary
connects records to final parts:
[scramble.py:256](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:256),
[scramble.py:449](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:449).
Its 5 GiB split applies before optional recompression and leaves keys uncapped:
[scramble.py:444](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:444),
[scramble.py:565](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:565).

Transfer Rust's synchronized six-artifact bundles, exact footprint admission,
and completion-marker publication, using
[paired_format.rs:20](../../crates/readmixer-core/src/paired_format.rs#L20),
[paired_layout.rs:491](../../crates/readmixer-core/src/paired_layout.rs#L491),
[paired_manifest.rs:225](../../crates/readmixer-core/src/paired_manifest.rs#L225), and
[output.rs:616](../../crates/readmixer-core/src/output.rs#L616) as design evidence.
Rust's source activation is not final release qualification; see
[Plan 12](../plans/12-paired-mixing-validation-and-release-closure.md#L3).
Inspected-source provenance is recorded in [source-snapshot.json](source-snapshot.json).

This is a new Python-only format. It neither reads legacy Python pools nor
claims Rust v2/v3 interchange. General original FASTQ inputs remain supported
under Plan 02. No persisted mixing key is necessary to read a completed pool.

## 2. Modules and ownership

Implement under the proposed `src/readmixer_py/artifacts/` package:

- `bgzf.py`: bounded member encoder, hostile member reader, virtual offsets,
  and standard `.gzi` encoding/validation.
- `key.py`: canonical key sizing, encoding, and bounded parsing.
- `index.py`: Python mapping-index header/entry encoding and hostile reading.
- `manifest.py`: bounded strict JSON admission and streaming serialization.
- `bundles.py`: pair footprints, synchronized rollover, bundle emission, and
  final artifact descriptors; it consumes the ordered pair stream from Plan 04.

Planned internal interfaces are `pair_footprint(pair, tag, candidate_offsets)`,
`BundleEmitter.append(pair, secure_pair_index)`, `BundleEmitter.finish()`,
`BgzfWriter.write(bytes)`, `BgzfWriter.finish()`,
`BgzfReader.read_record_at(virtual_offset, limits)`, and
`build_index(bundle, owned_partials, lease)`.
They return typed descriptors/errors, not unbounded lists of per-pair objects.
Public orchestration stays behind Plan 01's `mix(MixConfig) -> MixReport`.

Use NumPy/PyCryptodome/psutil only as assigned by the earlier plans; the BGZF
codec uses standard-library zlib for DEFLATE, not a new compression algorithm.
HTSlib `bgzip` is an independent test oracle, not a runtime dependency.

## 3. Stable identities and artifact inventory

The pool format discriminator is `"format": "readmixer-python-pool"`, with
`schema_version: 1`, `normalization_version: 1`, and
`shuffle.algorithm_version: 1`. Unknown formats or versions are unsupported.
The run ID is 16 independently random public bytes, rendered as 32 lowercase
hexadecimal characters. It is an identifier, never a cryptographic root key.

Sample indices are the catalog's dense `u32` values. Original pair ordinals are
zero-based `u64` values within a sample. Secured pair ordinals are a dense
zero-based `u64` domain across the entire physical bundle sequence. Require
checked representability even though Python integers do not overflow.

For bundle `b`, use the prefix `pool.<run_id_hex32>.b<bundle_index:06d>`:

- `<prefix>.R1.fastq.gz` and `<prefix>.R1.fastq.gz.gzi`.
- `<prefix>.R2.fastq.gz` and `<prefix>.R2.fastq.gz.gzi`.
- `<prefix>.key.csv.gz` and `<prefix>.index.rmpyidx`.

The sole completion manifest is `pool.<run_id_hex32>.manifest.json`.
Bundle indices are dense; each bundle contains at least one complete pair.
Six files belong to every bundle, with no key `.gzi`. Every generated file is
mode `0600`; Plan 05 owns the private staging directory and publication rules.
Names never contain source paths, sample identifiers, or original headers.

The default final-artifact inventory ceiling is **100,000 files total**,
including the manifest, not 100,000 bundles. Thus `6 * bundles + 1` must fit
the configured inventory ceiling and the resource-admitted registry capacity.
Catalog limits are at most 10,000 samples / 20,000 physical inputs and 16 MiB
encoded metadata, further restricted by Plan 03's memory admission.

## 4. Secured FASTQ and canonical key CSV

Use the collision-free AES-256 identity PRP specified by Plan 05. Its canonical
16-byte input is `b'RMP1' || sample_index_u32_BE || original_pair_index_u64_BE`.
Its dedicated KDF key is separate from shuffle and scratch keys. The entire
16-byte ciphertext becomes the 32-character lowercase hexadecimal pair tag;
never truncate it or substitute an unchecked random-tag generator.

Emit headers `@ReadMixer.<run_id_hex32>.<tag_hex32>/1` and `/2`, unchanged
sequence/quality bytes, a bare `+`, and LF after every line. A pair receives one
tag and one physical ordinal in both mate files. The normalized input-record
limit does not constrain the secured record directly: derive its maximum as
the admitted sequence and quality lengths plus the exact new header and
framing. A larger replacement header must not reject an otherwise valid input.

The exact ASCII key header, followed by one LF, is:

```text
sample_index,original_pair_index,secure_pair_index,pair_tag,r1_uncompressed_offset,r1_record_bytes,r2_uncompressed_offset,r2_record_bytes,r1_header_hex,r1_plus_hex,r2_header_hex,r2_plus_hex
```

Every row has exactly these twelve comma-separated fields and one terminating
LF. Integer fields use canonical unsigned decimal: `0` or a nonzero digit
followed by digits; no sign, whitespace, exponent, leading zeros, or bools.
The tag is exactly 32 lowercase hex characters. Metadata uses even-length
lowercase hex and is decoded only after its encoded and decoded bounds pass.
No CSV quoting is needed or accepted; commas and other admitted opaque metadata
bytes are hex. Reject decoded LF in header/plus fields, since an embedded LF
cannot belong to an admitted four-line record and would inject restored lines.

`r1_header_hex` / `r2_header_hex` preserve complete original headers including
`@`; plus fields include `+`. All exclude their normalized LF. Preserve
standalone CR bytes exactly as Plan 02 requires; do not apply generic `strip`,
Unicode decoding, newline translation, or description truncation.
Produced pool/recovery streams use the explicit `canonical_lf` reader mode:
remove only LF, never a preceding CR, and require all four LF terminators.
Only original-input ingress applies CRLF normalization. This preserves a lone
CR at original final-quality EOF when normalized rendering appends its LF.
Offsets name starts in the corresponding secured mate's uncompressed bundle
stream. Byte lengths include all secured FASTQ framing. Key rows follow the
same pair order as both mate streams. Bounds for key rows are derived from
the admitted metadata sizes, decimal field widths, tag, commas, and LF—not
from the assumption that a CSV row fits one original FASTQ record limit.

## 5. BGZF and standard sidecars

The default compression level is 6. A nonterminal data member has exactly
65,280 uncompressed bytes; the last data member has at most that many.
Each complete member is at most 65,536 physical bytes. Use canonical metadata
without original filename, comment, timestamp, or platform-dependent extras.
If primary compression would exceed the member bound, retry that payload at
level zero and check the bound again; an impossible result is an invariant error.
Write exactly one canonical 28-byte BGZF EOF member, including for empty streams.

Read members with explicit bounds on header, stored member, decoded output,
and decompression work. Validate the BGZF extra field and size, raw DEFLATE
termination, CRC32, ISIZE, member ordering, final EOF, and no trailing bytes.
Canonical pool readers reject noncanonical pool streams rather than repairing
them. Generic gzip input handling remains Plan 02's separate responsibility.

The standard `.gzi` is a little-endian `u64` entry count followed by pairs of
little-endian `u64` compressed and uncompressed member starts. Omit implicit
`(0, 0)` and the EOF member. An empty or one-data-member stream has an eight-byte
sidecar with count zero. Sidecar entries must match the actual stream exactly.

A virtual offset is `(compressed_member_start << 16) | within_member_offset`.
Require a representable 48-bit compressed start and valid 16-bit payload offset.
For a record/key-row start, require `0 <= within_member_offset < decoded_member_length`
in a data member. A start exactly at a member boundary uses the next data
member's compressed start and offset zero. Reject the previous member's
decoded-length endpoint, starts in the EOF member, and starts at physical EOF;
these are not alternate encodings of a valid start.
Record and key-row starts may occur inside members; records may cross members.
Virtual offsets are structured addresses, not ordinary offsets for arithmetic.
BGZF block boundaries need not coincide across R1, R2, and key lanes.

One shared bounded compression queue serves the lanes. Plan 09 owns scheduling;
the resource ledger must charge queued, running, and completed-but-not-yet-written
payload/result bytes. Per-lane ordering is mandatory regardless of completion
order. No whole-lane byte buffer or read-sized block-location dictionary is allowed.

## 6. Frozen Python mapping-index contract

The index is uncompressed, physical-pair-ordered, and private. It is intentionally
different from Rust `RMXIDX3`; it has no sample-directory or per-sample sort.
All integers below are little-endian, with no native alignment or padding.

The 64-byte header is encoded by `<8sII16sQQQQ`, in this order:

1. Magic: exactly `b'RMPYIDX1'` (eight bytes).
2. Version: `u32`, exactly 1.
3. Entry width: `u32`, exactly 40.
4. Run ID: the exact 16 public run-ID bytes.
5. Bundle index: `u64`.
6. First secured pair ordinal for this bundle: `u64`.
7. Entry count: `u64`, equal to bundle pair count.
8. Reserved: `u64`, exactly zero.

Each 40-byte entry is `<IIQQQQ`, in this order:

1. Sample index: `u32`.
2. Reserved: `u32`, exactly zero.
3. Original pair ordinal: `u64`.
4. R1 virtual offset: `u64`.
5. R2 virtual offset: `u64`.
6. Key-row virtual offset: `u64`.

Entry position `i` derives secured ordinal `first_secure_pair_index + i`.
Require exact physical length `64 + 40 * entry_count`, bounded before reads or
allocations; reject overflow, extra bytes, truncated entries, wrong run/bundle,
nonzero reserved fields, invalid sample/ordinal ranges, and manifest disagreement.
Starts increase strictly in each lane, since every pair has one nonempty
record/key row. Full verification proves these starts equal observed boundaries.

Build the index after all three BGZF lanes and both mate `.gzi` files finish.
Make one bounded read of the completed private key partial. For each row, use
monotonic cursors through mate sidecars to convert its two uncompressed offsets
into virtual offsets; take the key-row start from the BGZF key reader itself.
Stream the index immediately. Recheck key identities, row counts, and secured
ordinals. Reopen only transaction-owned partials with validated file identity;
Plan 05 must provide that ownership-checked operation. No per-read location map,
plaintext scratch run, or retained table for every bundle is necessary.

## 7. Exact cap admission and bundle emission

`max_artifact_bytes` is an integer in `65_564..=5_000_000_000`, defaulting to
5,000,000,000 physical bytes **per file**, not 5 GiB or a per-bundle aggregate.
For cap `C`, define `safe_payload = floor((C - 28) / 65_536) * 65_280`.
At the default cap this is 4,980,407,040 uncompressed bytes per BGZF lane.

Before adding a pair, project both secured mate lengths, complete key bytes
(including the header once), `64 + 40 * pairs` index bytes, and exact `.gzi`
sizes `8 + 16 * max(data_members - 1, 0)`. All must fit independently.
Greedily append while they fit; otherwise finish the complete current bundle
and retry the pair in an empty bundle. Reject an oversized singleton; never
emit an over-cap file, split mates, or silently relax the cap.

Use streaming greedy emission: compute bounded prospective six-lane counters,
then immediately append the admitted pair in secured order. Do not add a full
prelayout/placement pass or retain per-pair placement objects. On rollover,
re-size and re-render the candidate key row with the **new** bundle-local mate
offsets. Decimal offset digit widths change; its old candidate CSV byte count
is not valid at offset zero. Retain only the current admitted pair and counters.

Admission includes total disk, artifact registry, metadata, codecs, and worker
queues. Writer-observed counts and bytes must equal the streaming counters.
Finish and sync every artifact, then check actual physical bytes and hash the
exact file contents before registering its final descriptor.

## 8. Manifest schema and commit

The single private JSON manifest contains these required sections:

- Identity: `format`, `schema_version`, `normalization_version`, `run_id`,
  and `producer` with package name and version.
- `shuffle`: `algorithm_version: 1` and `tag_algorithm: "aes256-identity-prp-v1"`.
- `limits`: `max_artifact_bytes`, `max_input_record_bytes`,
  `max_metadata_bytes`, and `max_artifacts`, recording the enforced ceilings.
- `codec`: `format: "bgzf"`, `compression_level`, `payload_bytes: 65280`,
  `max_member_bytes: 65536`, `eof_bytes: 28`, and `digest_algorithm: "sha256"`.
- `execution`: bounded resolved settings with `profile_version: 1`,
  `memory_budget_bytes` (B), `terminal_slice_bytes` (T), `storage_mode`,
  `workers_effective`, `cpu_budget`, `descriptor_limit`, and `compression_slots`.
  Use the exact meanings and admitted values from Plans 03/09; no per-pair
  arrays, secret material, or independently invented resource estimates.
- `samples`: dense entries with `sample_index`, exact `sample_id`, `pairs`,
  and `r1` / `r2` source descriptors. Each source descriptor records
  `source_file_id`, `relative_path_hex`, `input_codec`, `physical_bytes`,
  `decoded_bytes`, `normalized_bytes`, and `normalized_sha256`.
- `bundles`: dense entries with `bundle_index`, `first_secure_pair_index`,
  `pairs`, and six role descriptors named `r1`, `r1_gzi`, `r2`, `r2_gzi`,
  `key`, `index`. Each has `name`, `bytes`, and `sha256`; data lanes also have
  `uncompressed_bytes` and `records`, and sidecars/index have `entries`.
- `totals`: `samples`, `pairs`, `reads`, `bundles`, and `artifacts` including
  this manifest, plus `r1_uncompressed_bytes`, `r2_uncompressed_bytes`, and
  `key_uncompressed_bytes` for the mixed lanes.

`source_file_id = 2 * sample_index + role`, with role zero for R1 and one for
R2. `input_codec` is exactly `plain` or `gzip`; BGZF original inputs count as
gzip containers. Source role IDs and codec claims must agree with the catalog.

Normalized source digests hash the complete Plan 02 normalized original FASTQ
streams, including original header/plus bytes and all four LF terminators.
Physical artifact digests hash exact stored bytes. Every SHA-256 is 64 lowercase
hex characters. Do not persist root/subkeys, deterministic seeds, AEAD keys,
raw original headers outside the key, or any assertion of signed authenticity.
The manifest is private identifying metadata despite containing no crypto keys.

Canonical source path hex is descriptive private metadata. Pool readers do not
reopen those paths automatically. Artifact names must exactly match generated
basenames and roles; no absolute paths, traversal, separators, or aliases.
Reject duplicate JSON keys, NaN/Infinity, invalid UTF-8 JSON, unexpected fields,
boolean-as-integer values, negative counts, inconsistent widths/totals, and
unknown policies. Bound parsed-object memory as well as encoded JSON length.

Serialize with bounded UTF-8 JSON streaming and one terminal LF. Admit at most
`min(64 MiB, max_artifact_bytes, ResourcePlan metadata lease)` and fail rather
than splitting the root manifest. Do not include a self-digest or a self-size
fixed point. Artifact descriptors exclude the manifest's own digest; readers
can independently hash its exact bytes. No unbounded `json.dumps` staging copy.

Plan 05's `OutputTransaction` exclusively creates private partials, registers
closed lengths/digests, publishes files without replacement, and commits this
manifest last. A file's publication is atomic; the whole set becomes logically
complete through the marker protocol, not a filesystem-wide atomic operation.
Before durable commit, failure invokes owned rollback with cleanup errors
reported. After commit, cleanup warnings preserve the valid committed set.
Hard stops or failed cleanup may leave residue; do not promise universal marker
absence. Never report success before directory synchronization and marker commit.

Reproducibility tests fix input bytes/catalog, test root key, public run ID,
versioned content-affecting settings, resolved shuffle topology, artifact cap,
compression level, and Python/native codec build. Under those conditions, the
six bundle artifact files must be byte-identical when admitted worker/storage
choices change. Validate each manifest independently, then compare it
semantically with only `execution` excluded: that section must honestly retain
different workers, storage modes, and resolved resources. Do not erase actual
execution observations to force manifest-byte equality. Timing/resource fields
in reports are also excluded from reproducibility comparisons. If a changed
resource budget changes topology, require preservation/recovery/cap invariants
instead of identical shuffled artifact bytes.

## 9. Test IDs and completion criteria

- **P06-001:** golden CSV bytes, exact twelve fields, full long/non-UTF-8
  headers/plus lines, preserved standalone CR, canonical decimal/hex rejection.
- **P06-002:** independent `struct` fixtures prove header 64, entry 40, index
  order/run identity/reserved fields, exact length, and integer-bound rejection.
- **P06-003:** BGZF empty/one/multiple members, records spanning members,
  incompressible payload retry, CRC/ISIZE/EOF/trailing corruption, and `.gzi`
  equality against independent HTSlib `bgzip` round trips; canonical boundary
  starts use the next member at zero and reject prior-member endpoints/EOF.
- **P06-004:** minimum/default caps, cap equality/one byte beyond, each reachable
  R1/R2/key limiting lane forcing rollover, enormous original metadata, and
  rejected oversized singleton; decimal-offset digit transitions re-size
  correctly when a pair rolls to offset zero. Prove the index/sidecar bounds
  dominated by admitted lane growth, and test their direct bounds and hostile
  over-cap files. Every final artifact is physically inspected. The proof and
  scenario closure are specified in
  [Plan 15](15-workflow-boundaries-and-failure-acceptance.md).
- **P06-005:** bounded one-pass index construction agrees with independently
  observed mate/key row starts across unequal record lengths and many bundles.
- **P06-006:** manifest duplicate/NaN/bool/path/count/unknown-version mutations,
  encoded/parsed metadata limits, and total inventory accounting.
- **P06-007:** all files `0600`; no source identifiers in generated names or
  secured plus lines; precommit faults attempt rollback, report cleanup residue,
  and cannot claim success. Postcommit cleanup warnings preserve committed data.
- **P06-008:** compression completion reordering preserves lane bytes/indexes;
  queue saturation proves the shared byte limit includes completed results.
- **P06-009:** fixed-entropy/run-ID/topology/build executions produce identical
  six-file bundle bytes across admitted workers/storage; independently valid
  manifests match semantically excluding `execution`, whose differences remain.

Complete only when the frozen schema has independent golden readers, every
test above passes on Linux/macOS, Plans 07/08 consume it without writer state,
and no legacy/Rust-format compatibility or release qualification is claimed.
