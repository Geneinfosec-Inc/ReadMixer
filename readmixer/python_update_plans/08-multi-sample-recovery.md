# Plan 08 — Multi-sample recovery

Status: **Planned**. This defines new Python-native recovery for new pools only.
No recovery implementation or runtime behavior is added by this document.

Prerequisites: [Plan 01](01-packaging-interfaces-and-compatibility.md),
[Plan 02](02-paired-input-and-lossless-parsing.md),
[Plan 03](03-resource-admission-and-storage.md),
[Plan 05](05-encrypted-scratch-and-safe-publication.md),
[Plan 06](06-bundles-keys-indexes-and-manifests.md), and the independent reader
and normalized-content checks established in [Plan 07](07-complete-verification.md).

## 1. Evidence and intentional scope

The predecessor exposes only scrambling:
[readmixer.py:20](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/readmixer.py:20).
Its truncated descriptions and basename-based keys cannot support an honest
general exact-recovery promise:
[scramble.py:320](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:320),
[scramble.py:383](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:383).
This plan does not add legacy-pool import, salvage, or heuristic mate inference.

Rust's public recovery selects exactly one complete sample:
[recovery.rs:25](../../crates/readmixer-core/src/recovery.rs#L25).
Its indexed/sequential distinction, bounded pair-body sorting, normalized-digest
checks, and receipt-last publication are useful models:
[recovery_engine.rs:196](../../crates/readmixer-core/src/recovery_engine.rs#L196),
[recovery_engine.rs:476](../../crates/readmixer-core/src/recovery_engine.rs#L476),
[recovery_engine.rs:963](../../crates/readmixer-core/src/recovery_engine.rs#L963), and
[recovery_engine.rs:4184](../../crates/readmixer-core/src/recovery_engine.rs#L4184).
The Python design extends selection to a union of samples in one invocation;
it does not pretend that Rust already implements that operation.
See [source-snapshot.json](source-snapshot.json) for inspection provenance.

Recover both mates for each requested whole sample, in original pair order,
with exact Plan 02 normalized original bytes. Original FASTQs and the ephemeral
mixing key are unnecessary. Plain/conventional-gzip export, individual-mate or
range selection, merged over-cap outputs, database integration, and remote
artifact access remain outside this plan.

## 2. Public request and result contract

Implement `src/readmixer_py/recover.py` behind
`recover(RecoverConfig) -> RecoverReport`. Examples are:

```text
readmixer-py recover --manifest POOL_MANIFEST --output DESTINATION --sample SAMPLE_A
readmixer-py recover --manifest POOL_MANIFEST --output DESTINATION --sample SAMPLE_A --sample SAMPLE_B --io indexed
readmixer-py recover --manifest POOL_MANIFEST --output DESTINATION --sample SAMPLE_A --io sequential
```

`RecoverConfig` contains manifest/output paths, a nonempty sequence of exact
`sample` identifiers, `io` defaulting to `indexed`, resource/decode limits,
optional lower output-artifact cap, compression level 0..9 defaulting to 6,
scratch location/storage policy, and cancellation state. The manifest's parent
is the artifact root; there is no `--artifacts` option. The public API accepts
one or many identifiers; repeated
`--sample` flags form the CLI sequence. Reject duplicate requested identifiers
instead of silently deduplicating them. Reject any unknown identifier before
scratch/output creation. Catalog canonical sample-index order determines output
ordering independently of flag order.

Only `indexed` and `sequential` are accepted. There is no `auto` mode and no
fallback from corrupted/missing indexed artifacts. Sequential behavior is an
explicit user choice with a different recorded validation scope.

`RecoverReport` contains pool run ID, fresh recovery ID, counts, actual mode,
validation scope, ordered output descriptors, and committed batch receipt path.
It also exposes `cleanup_complete` and bounded postcommit cleanup warnings;
cleanup-only warnings retain a committed result and exit zero under Plan 05.
The CLI's `--json` view follows Plan 01's redaction policy; the full private
receipt may contain selected sample IDs and normalized digests. Never put
headers, plus lines, sequences, qualities, tags, virtual offsets, key rows,
original paths, or cryptographic keys into the receipt or ordinary diagnostics.

No requested sample can be silently skipped. A request with one invalid or
unrecoverable member fails the whole batch and does not publish a success marker
for the other members. A failed explicit original selection never becomes
"recover all" or an empty successful selection.

## 3. Admission, identities, and limits

Use Plan 06/07's bounded strict manifest admission, private-mode checks,
no-follow regular-file access, generated-basename validation, count/width
invariants, and exact pool/schema version dispatch. A recovery receipt is not
accepted as a pool manifest. Reject foreign Rust and old Python formats before
payload access. Hash the exact admitted manifest bytes for the new receipt.

Resolve every requested ID against the admitted catalog up front. Store a
bounded catalog-sized selection bitset and selected pair counts; never create
a dictionary keyed by all read ordinals or tags. Selection count and its
metadata remain inside catalog/resource limits.

The source artifacts retain their manifest-declared source cap. An optional
recovery cap applies to **new recovered outputs**, must be between 65,564 and
the source pool cap, and cannot exceed 5,000,000,000. Do not reject a valid
larger source artifact merely because the caller requested smaller recovered
parts. Independently enforce actual source file lengths against their declarations.

Admit all required paths and mode-specific resources before starting payload
work. Indexed mode requires every declared index and both sidecars of every
bundle. Sequential mode deliberately does not resolve, stat, open, hash, or
parse any index filename; it still validates the manifest schema that names it.
Preflight consumed FASTQ/key identities with the same strict path policy.

Use a fresh Plan 05 scratch context for this invocation. Scratch contains
selected biological bodies and original metadata; disk spill/merge files are
always authenticated ciphertext. No saved mixing key is needed. Strict memory
mode fails without spilling if its admitted capacity cannot contain the work.
Record buffers may cross authenticated scratch blocks, never logical segments.

The resource plan covers selected metadata, codecs, index/sidecar readers,
bounded decoded-member caches, selected pair sorter, merge frontier, output
compression queues, artifact registry, and bounded receipt serialization.
Derive worst-case restored bytes from selected pair counts and the enforced
original normalized-record bound, with framing/AEAD/merge overhead. The default
input bound is 1 MiB per normalized mate; it is a limit, not an average-size
estimate. Do not use falsely small declared source bytes to evade admission.

Use actual byte counters for allocations, decoded input, scratch, output, and
queued compression. `max_total_uncompressed`, if supplied, caps the selected
normalized original mate bytes once; enforce it against actual restored byte
totals before exceeding it. Independent schema/resource work bounds and I/O
counters cover artifact decoding, including repeated/adjacent-member decoding.
The manifest's normalized-record bound applies unless the caller supplies a
stricter bound; a caller cannot silently weaken that validation contract. A
secured record can exceed the original-record limit due to its replacement
header: use the derived secured bound, then enforce the original bound on the
fully reconstructed mate. Key-row and metadata bounds are separately derived.

## 4. Indexed union gathering

Process bundles in their declared order. Within each bundle, parse/hash the
complete `RMPYIDX1` index forward, checking its 64-byte header, 40-byte entries,
reserved fields, exact length, run/bundle/secured-range identity, sample/ordinal
ranges, and strictly increasing virtual starts in each lane.
Validate every entry, including entries for unselected samples. Maintain
bounded sample-count totals and compare them with the manifest; this is not
a replacement for full verification's per-original-identity bijection proof.

Read, hash, and structurally validate both full `.gzi` sidecars in every bundle,
including bundles containing no selected sample. Check exact count/length,
strict offsets, canonical uncompressed positions, and declared stream bounds.
Use bounded tables/cursors under a lease, not a table for the whole run.
Validate relationships to actual BGZF member starts when accessing corresponding
members. Full verification remains responsible for exhaustive stream/sidecar
correspondence for all unrelated payloads.

The physical-order index is central to efficient union selection. An entry is
selected when its sample bit is set. Its derived secured ordinal is the header's
first secured ordinal plus physical entry position. All selected R1, R2, and
key locations arrive in physical order; do not run recovery once per sample,
sort locators back into original order before reading, or repeatedly decompress
the same selected member for separate sample requests.

For each selected entry:

1. Resolve the R1/R2/key virtual offsets through hostile bounded BGZF readers.
   Prove target compressed locations are member starts rather than trusting a
   magic-looking byte sequence inside compressed payload. A monotonic member
   header cursor can skip unneeded payload while maintaining chain evidence.
   Enforce Plan 06's canonical start: offset strictly below decoded data-member
   length, boundary starts at the next data member's offset zero, and no start
   at a prior member's decoded-length endpoint, EOF member, or physical EOF.
2. Decode the key row and both complete mate records, including cross-member
   records, with canonical row/record boundary checks and bounded neighboring
   member reads when required. Reuse current decoded members across selections.
3. Require key sample/original/secured identities to match the index. Require
   both secured header tags/run IDs/roles, bare plus lines, measured mate
   lengths, and key uncompressed offsets to agree. Reconcile mate offsets with
   the corresponding sidecar mapping. Reject overlap or repeated selected
   physical record/key-row ranges.
4. Decode exact original headers/plus fields, rejecting embedded LF and invalid
   markers; restore unchanged sequence/quality bytes. Revalidate complete
   original mate shapes and pairing rules without printing their contents.
5. Append one complete reconstructed pair with its numeric sample/original
   identity to `ExternalSorter(..., key=(sample_index, original_pair_index), ...)`.
   Release per-record objects and retain only admitted caches.

Generated pool streams use **`canonical_lf`** framing: remove LF only, preserving
preceding CR bytes. Never reuse ingress CRLF normalization or UTF-8 text decoding
on these streams. This applies to data in the selected-body scratch frames too.

Hash/index results are provisional while gathering. A late index or sidecar
digest failure invalidates all provisional selected bodies and aborts the batch.
Gather all selected samples together, and compare selected entry counts with
their declared counts before accepting the gathered domain.

Indexed mode does not hash unread FASTQ/key bytes. It validates the selected
records and required boundary evidence, full indices, and full sidecar bytes.
Final normalized selected-stream digests bind the restored result to the supplied
private manifest. Do not label this `pool_complete` verification.

## 5. Explicit sequential union gathering

Sequential mode streams complete R1, R2, and key BGZF artifacts in lockstep for
every bundle, hashes all their compressed bytes, and validates both complete
sidecars against observed member starts. It never consults mapping indices.

Reuse the independent full-stream parsers from Plan 07. Validate all physical
pair ordinals, key row identities/ranges, header tags/roles, offsets, lengths,
stream counts, CRC/ISIZE/EOF, and whole-file hashes even for unselected rows.
Filter selected samples into the same encrypted complete-pair sorter used by
indexed mode. A single pass gathers the entire selected union.

No complete original-domain sort or normalized digest for unselected samples
is required by recovery. Consequently complete source-stream hashing here still
does not replace Plan 07's full bijection/tag/original-domain proof.
Corruption in a source FASTQ/key stream is fatal even if it affects only an
unselected sample. An absent or corrupt index does not affect explicit
sequential recovery because it is never accessed.

## 6. Original-order reconstruction and synchronized parts

Finish the bounded encrypted sort, respecting Plan 03's descriptor and merge
fan-in limits. Consume records in `(sample_index, original_pair_index)` order.
For each selected sample, require exactly ordinals `0..pairs-1`, with no gaps,
duplicates, extra samples, or trailing records. Counts alone are insufficient.

Hash each restored mate's exact normalized original FASTQ bytes and count their
normalized bytes while emitting them to private partials. Compare both digests,
byte totals, and pair counts with the source manifest before the batch can
commit. The source descriptor's physical gzip size and original newline choices
are not restoration promises. The report promises normalized content and order.

Output R1/R2 use the same canonical BGZF writer and standard `.gzi` as Plan 06.
Choose part boundaries from the **restored** complete pair footprints and the
recovery output cap. Roll both mates together when either cannot admit the next
pair; never reuse the mixed bundle boundaries or split a pair between parts.
Reject an oversized restored singleton even if its shorter secured form fit.
Actual completed FASTQ and sidecar lengths must fit the cap before publication.

For every selected zero-pair sample, produce one valid empty R1 BGZF, empty R2
BGZF, and their eight-byte zero-entry sidecars. Record pair count zero and empty
normalized-stream digests. This is an intentional Python behavior: Rust's
existing recovery instead produces only a receipt for an empty sample.

Allocate a fresh 16-byte public recovery ID for each invocation. The per-part
prefix is `recover.<pool_run_id_hex32>.<recovery_id_hex32>.s<sample_index:06d>.p<part_index:06d>`.
Append `.R1.fastq.gz` / `.R2.fastq.gz`, with their `.gzi` sidecars. Part indices
restart at zero within each selected sample. Numeric IDs prevent original sample
names or source filenames from becoming output paths. A fresh recovery ID
avoids collisions when the same sample is requested again in one destination.

Only the current sample/part's writers need stay open. Store completed bounded
artifact descriptors in the admitted registry, then release the handles.
Count **all** final files: four for every recovered part (including each empty
sample's part) plus one batch receipt. The default 100,000-file inventory
ceiling and actual resource-admitted capacity both apply.

## 7. Batch receipt and publication

The sole completion receipt is
`recover.<pool_run_id_hex32>.<recovery_id_hex32>.receipt.json`.
Use `format: "readmixer-python-recovery"`, `schema_version: 1`, and
`normalization_version: 1`; it is a separate document type from a pool manifest.
Required fields are:

- `pool_run_id`, `recovery_id`, and `source_manifest_sha256` over exact admitted
  source-manifest bytes, with no source-manifest pathname.
- `io_mode` (`indexed` or `sequential`), `index_validation` (`validated` or
  `explicitly_bypassed`), and `validation_scope` (`selected_records` or
  `complete_bundle_streams`) corresponding to that mode.
- `limits` with effective output artifact cap, final-artifact inventory ceiling,
  original/secured record admission facts, and metadata ceiling actually used.
- `codec` with the selected recovery compression level and the same BGZF/hash
  constants and field names as the pool manifest.
- `samples` in numeric catalog order: sample index/ID, pair/read counts,
  measured normalized R1/R2 bytes/digests, and ordered `parts` descriptors.
- Each part: part index, pair count, and four role descriptors `r1`, `r1_gzi`,
  `r2`, `r2_gzi`; each has generated basename, actual physical bytes, SHA-256,
  plus the applicable uncompressed-byte/record or sidecar-entry count.
- `totals` with selected samples, pairs, reads, parts, and exact artifact count
  including the receipt. No self-digest or self-size fixed point is required.

Serialize under `min(64 MiB, recovery output cap, metadata lease)` using bounded
UTF-8 streaming and the same strict JSON rules as the pool manifest. All receipt
digests/counts come from measured output and verified source facts; never copy
unchecked claims into a success receipt. Reopen/hash owned closed partials
through Plan 05's ownership checks as needed to bind the descriptors to files.

Prepare the public report and receipt fully before irreversible completion.
Register all closed outputs with `OutputTransaction`, finish all selected
validations, then call its publication protocol. Data and sidecars become
durable before the single receipt is published last and its directory synced.
Only then report success. Do not emit an earlier per-sample success receipt.

The transaction supplies no-replace per-file atomic publication and whole-request
logical completion through the final marker. It is not an atomic filesystem
operation over every selected sample. Before durable commit, failure/cancellation
attempts owned rollback and reports cleanup residue; a hard stop may leave an
uncommitted prefix. After commit, cleanup warnings preserve the valid output set.
Do not promise that every possible I/O/cleanup failure removes the marker.
Consumers require the valid receipt and its complete artifact set. Preserve
primary plus cleanup errors, and expose postcommit warnings separately.
If cancellation is observed after durable commit, retain the committed set and
identify its marker even when returning the normal cancellation status.

## 8. Security and failure semantics

Selective output is not selective access control. Private keys, mapping indices,
and the manifest expose other represented sample mappings; anyone holding them
can request another sample. Keep every generated recovery file mode `0600` and
keep all selected-body scratch encrypted. Neither restoration nor mixing makes
genetic sequence anonymous. An unsigned receipt does not authenticate a wholly
replaced pool/manifest/receipt set.

Unknown/duplicate selection, invalid config, unsupported format, missing artifact,
digest mismatch, malformed record/index, resource ceiling, ENOSPC, cancellation,
or compression failure before commit must fail the request through typed errors. No
best-effort partial selection, implicit sequential fallback, original-input
fallback, automatic key regeneration, or silent cap relaxation is allowed.

## 9. Test IDs and completion criteria

- **P08-001:** one/many/all selected samples with unequal counts, empty samples,
  and many bundles recover identically in indexed and sequential modes after
  original inputs are unavailable; compare exact normalized bytes and order.
- **P08-002:** one union invocation equals independent recoveries per sample;
  flag order does not affect content, and unknown/duplicate IDs fail up front.
- **P08-003:** instrumented readers prove one forward index traversal per bundle,
  shared selected-block caching across samples, bounded neighbor reads, and no
  whole unread FASTQ/key hashing in indexed mode. Check all index/sidecar digests.
- **P08-004:** missing/corrupt index fails indexed mode; explicit sequential mode
  never resolves/stats/opens that index and records the bypass in its receipt.
- **P08-005:** index/key/header/role/offset/length mismatches, overlapping selected
  ranges, noncanonical member endpoints/EOF starts, missing/duplicate original
  ordinals, and false canonical digests fail;
  no receipt or successfully committed subset survives ordinary failure.
- **P08-006:** restored metadata forces different part boundaries; smaller
  recovery caps accept larger valid source artifacts but bound every new file;
  oversized restored pairs and total inventory/receipt limits fail cleanly.
- **P08-007:** long/opaque metadata, record/member crossings, zero sequence/quality,
  and source final lone CR at EOF survive exact normalized reconstruction.
- **P08-008:** misleadingly small declared source byte totals, low memory/file
  descriptors, forced encrypted spill and multi-pass merge, ENOSPC, cancellation,
  worker failure, publication races, and cleanup failures stay bounded; failures
  before commit and warnings after commit exercise distinct ownership rules.
- **P08-009:** empty selection is an error; each selected empty sample receives
  four valid files plus its batch receipt entry; repeated successful recovery
  in the same directory uses distinct generated recovery IDs.
- **P08-010:** mode-specific receipt scope, source-manifest binding, private
  permissions, strict JSON admission, redacted CLI output, no legacy/Rust pool
  acceptance, and no mixing-key dependency are independently tested.

Complete when all tests pass, union recovery is implemented as shared gathering
instead of repeated full sample scans, both modes share one proven reconstruction
contract, physical/resource bounds hold, and receipt publication means every
requested sample has passed count, identity, and normalized-content validation.
