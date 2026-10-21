# Plan 07 — Complete verification

Status: **Planned**. This is an implementation specification, not a report that
verification or its acceptance tests already exist in the Python project.

Prerequisites: [Plan 01](01-packaging-interfaces-and-compatibility.md),
[Plan 02](02-paired-input-and-lossless-parsing.md),
[Plan 03](03-resource-admission-and-storage.md),
[Plan 05](05-encrypted-scratch-and-safe-publication.md), and the frozen
[Plan 06 format](06-bundles-keys-indexes-and-manifests.md).
Release acceptance is owned by [Plan 10](10-acceptance-documentation-and-release.md).

## 1. Evidence, purpose, and boundaries

The Python entry point exposes scrambling but no verifier:
[readmixer.py:20](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/readmixer.py:20).
Current integration assertions check artifact existence, counts, and header
prefixes, which cannot prove that records were mixed or recoverable:
[test_readmixer.py:197](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/tests/test_readmixer.py:197).
The historical shuffle bug illustrates why successful producer execution is
insufficient evidence:
[scramble.py:240](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:240).

Rust supplies useful independent-verification invariants: complete artifact
hashing/scanning at [verify_v3.rs:816](../../crates/readmixer-core/src/verify_v3.rs#L816),
mapping bijections and tag uniqueness at
[verify_v3.rs:2370](../../crates/readmixer-core/src/verify_v3.rs#L2370), and original
comparison at [verify_v3.rs:2505](../../crates/readmixer-core/src/verify_v3.rs#L2505).
Use those invariants with the new Python format, not Rust's private parser or
wire schema. Provenance belongs to [source-snapshot.json](source-snapshot.json).

This verifier consumes a completed new Python pool without trusting producer
memory, placement plans, cache contents, or a previous recovery receipt.
It proves complete pool consistency and exact normalized reconstruction.
Optional original inputs establish a stronger comparison to an external source.
It does not prove anonymity, statistical uniformity, or authenticity of an
unsigned manifest against replacement of an entire consistent pool.

## 2. Public interface and observable outcomes

Implement `src/readmixer_py/verify.py` behind
`verify(VerifyConfig) -> VerifyReport`. The CLI form is:

```text
readmixer-py verify --manifest POOL_MANIFEST
readmixer-py verify --manifest POOL_MANIFEST --originals SAMPLES_TSV
```

`VerifyConfig` carries the manifest path, optional `originals` TSV, Plan 03
resource and decode limits, scratch location/storage policy, and cancellation
state. The manifest's parent is the artifact root; no alternate `--artifacts`
option exists. Original TSV columns are exactly `sample_id`, `r1`, `r2`;
path admission and pairing rules are shared with Plan 02. Selection is not a
verification option: this command always validates the complete pool.

`VerifyReport` records run ID, exact source-manifest SHA-256, samples/pairs/reads,
bundle and artifact counts, consumed artifact bytes, and one explicit scope:
`pool_complete` or `pool_and_originals`. Preserve the distinction in text and
JSON. Plan 01's redacted CLI JSON includes no sample IDs, source paths, headers,
key rows, tags, sequences, qualities, or private per-sample digest lists.

Verification is read-only with respect to source inputs and pool artifacts.
It creates only a new private scratch context when needed, deletes its owned
scratch on success/ordinary failure, and reports success only after cleanup.
It does not modify, repair, rehash-and-rewrite, or rebless the manifest.
Typed failures and exit statuses follow Plan 01; preserve the primary error
when scratch cleanup also fails. Cancellation cannot yield a success report.

## 3. Admission before reading payloads

Read the manifest through a no-follow, regular-file descriptor; enforce its
private mode and actual bounded length before materializing JSON. Use the
Plan 06 strict JSON loader: reject duplicate keys, nonfinite numbers, unknown
fields/policies, bools where integers are required, and unsupported versions.
Enforce the actual metadata cap `min(64 MiB, artifact cap, resource lease)`
and the separately budgeted parsed-object representation.

Validate all static relationships before creating scratch or opening data:

- Exact pool format/version, normalization version, shuffle/tag policy, codec
  constants, lowercase run ID/digests, and declared integer widths.
- Dense, unique sample indices and external IDs; valid source-role identities;
  two distinct admitted source files per sample; catalog and input-count limits.
- Source normalized byte/count facts are consistent, including zero-pair
  samples; source path hex is descriptive and is not automatically reopened.
- Dense nonempty bundles; first secured ordinals form contiguous ranges;
  mate records, key records, and index entries equal each bundle's pair count.
- Sum of sample pairs equals sum of bundle pairs; reads equal twice pairs;
  aggregate byte declarations and inventory agree with role descriptors.
- Exactly six generated basenames per bundle and one manifest; names match the
  run/bundle/role grammar, are unique, and cannot escape the artifact root.
- Every declared artifact fits the pool's physical cap; the inventory contains
  at most 100,000 files and fits the actual registry lease.
- Index size is exactly `64 + 40 * pairs`; BGZF block/sidecar declarations fit
  canonical payload arithmetic and resource/decode bounds.

The new schema's producer requires two nonempty samples; validate that policy
without counting empty companions as additional mixed samples. Empty catalog
members are otherwise permitted and must retain valid empty normalized digests.

Canonicalize the artifacts root once. Open only manifest-declared regular
files with no-follow protection, validate current-owner/private-file policy,
and retain/recheck identity across any reopen. Do not glob for substitutes or
follow arbitrary artifact paths. Extra unrelated files do not become artifacts.

## 4. Resource plan and independent scratch context

Request a verifier-specific `ResourcePlan` before scanning. Charge the manifest,
catalog, descriptor registry, all simultaneous reader/member buffers, key-row
decode buffers, two sorters, sorter merge frontier, and IPC/native overhead if
parallel hashing is introduced. Start with a serial bundle scan and bounded
native compression/decompression; parallelism is not a correctness prerequisite.

Create fresh OS-random scratch-root material using Plan 05. No mixing key or
stored key seed is requested. All disk scratch is authenticated and encrypted;
strict memory mode must fail rather than spill when its lease cannot fit.
Do not use SQLite default temporary files, shell sort, pickle, or unauthenticated
unprotected merge runs. Frames may cross authenticated scratch blocks, never
logical segment identities; frame readers return only complete verified records.

Use `ExternalSorter(record_codec, key, lease)` for two independent domains:

- Reconstructed canonical pair bodies keyed by `(sample_index,
  original_pair_index)`, with identity/count information needed for dense-domain
  validation. The complete pair is the atomic record.
- Fixed-size `(pair_tag, secure_pair_index)` records for global tag uniqueness.

Plan 03 bounds initial runs, merge fan-in, active descriptors, and encrypted
run-registry/frontier storage. No dictionary, set, boolean array, or list may
scale with the total number of pairs. Catalog-sized state is separately bounded.
The physical-order index permits streaming index/key/FASTQ joins; a third
read-sized mapping table or locator sort is unnecessary.

Disk admission must cover actual worst-case scratch frames and merge coexistence
under configured record/count bounds, including authentication overhead and
filesystem allocation slack. Declared low normalized byte totals are assertions,
not permission to evade resource admission. `max_total_uncompressed`, if supplied,
counts reconstructed normalized original mate bytes once across the whole pool.
Check actual sums before exceeding that caller ceiling. Optional original-input
comparison does not charge the same logical records again. Independent schema,
resource, and actual I/O/decode counters cover each artifact pass and original
read. Full verification can need substantial scratch; failure must be explicit.

## 5. Complete bundle scan and mapping proof

For each bundle, consume its R1, R2, and key BGZF streams in lockstep and its
physical-order index as a forward stream. Hash exact compressed/index bytes as
they are consumed, using the same bytes that feed validation; handle any reopen
through identity checks. Read and hash both complete sidecars. Do not accept a
digest-only pass as equivalent to parsing the complete corresponding artifact.

The member reader independently validates all BGZF headers, member lengths,
DEFLATE termination, CRC32, ISIZE, payload sizes, final EOF, and no trailing
bytes. Derive actual member starts from those bytes and compare `.gzi` entries
exactly; do not treat the sidecar as proof of its own block boundaries.
Track row/record starting virtual offsets directly while parsing, including
records that cross member boundaries. The key has no `.gzi` dependency.

Pool data uses Plan 02's **`canonical_lf`** framing: require every final LF,
remove only LF, and preserve preceding CR bytes. Never use the ingress reader's
CRLF normalization on generated pool streams. This includes a permitted lone
CR from an original final quality line without its original final LF.

For physical pair position `i`, require all of the following:

1. Both complete mate records, one complete key row, and one index entry exist;
   their EOF positions agree exactly at the declared count.
2. The index header matches magic/version/entry width/run/bundle/count and zero
   reserved values. Derived secured ordinal equals bundle first ordinal plus
   `i`, and equals the key row's secured ordinal.
3. Index and key sample/original identities agree and are within catalog bounds.
   The original ordinal is below that sample's pair count.
4. Three index virtual offsets equal the actually observed R1/R2/key starts.
   Key mate offsets/record lengths equal measured uncompressed starts/lengths.
5. Both secured headers have the exact run ID, key tag, and proper role;
   secured plus lines are exactly `+`; sequence/quality lengths agree.
6. Key metadata decodes within bounds, original headers begin `@`, plus lines
   begin `+`, and no decoded header/plus field contains LF. Preserve other
   admitted bytes, including CR; do not repair malformed metadata.
7. Reconstructed normalized original mates obey Plan 02's record/identifier and
   mate-pair rules. Their record-size limit is distinct from the secured-size
   bound derived for the replacement header.
8. Stream the canonical complete pair into the identity sorter and its tag into
   the tag sorter; release per-record buffers before accepting more input.

At bundle EOF, verify all declared counts, exact stored/uncompressed lengths,
sidecar counts, index exact length, and SHA-256 digests. Authentication or digest
failure invalidates all provisional sorter entries and prevents success.
Reject partial trailing records, excess rows/entries, and dishonest empty streams.

## 6. Independent cap and greedy-boundary proof

Recompute the exact secured record and key footprints from observed bytes.
Reimplement the small greedy admission oracle in verifier/test logic; do not
call the producer's `BundleEmitter` or consume producer placement state.
Use the frozen cap arithmetic, actual sample-independent 40-byte index growth,
and `.gzi` accounting from Plan 06.

Every pair must fit an empty bundle. Every observed bundle must fit all lanes.
At a boundary, the first pair of the next bundle must fail admission into the
previous bundle; otherwise the producer emitted a noncanonical early rollover.
Re-size that hypothetical key row with the **previous** bundle's prospective
R1/R2 offsets: its observed row in the new bundle can have fewer decimal offset
digits. Reusing the observed row length would prove the wrong greedy boundary.
Carry only the previous bundle totals and next complete pair fields to prove this.
Last-bundle termination needs no hypothetical next pair. Actual physical lengths
must also fit the cap even when conservative admission would have allowed them.

## 7. Global identity, tag, and normalized-content proof

Merge the identity sorter in catalog sample order. For each sample, demand
exactly original ordinals `0..pairs-1`; reject a missing, duplicated, extra, or
out-of-range identity even if aggregate counts happen to match. Cross-sample
substitutions cannot be hidden by equal counts or identical biological bodies.

Reconstruct each original mate as header LF, sequence LF, plus LF, quality LF.
Update SHA-256 and exact normalized byte counts separately for each role.
Compare both values with the manifest's source descriptor when that sample
ends. Empty samples must match the empty stream digest and zero counts/bytes.
Only two sample digest states and bounded current frames need be live at once.

Merge the tag sorter and reject adjacent equal tags. Its count must equal the
pool pair count. A duplicated tag is an integrity failure even when metadata
otherwise reconstructs. This is an independent check; no ephemeral PRP key is
needed, and the verifier does not attempt to derive one from public metadata.

Dense secured ordinals were proved during physical scanning, complete original
identities during sorting, and both-mate/key/index agreement during the join.
Together these establish the required bijections without an in-memory map of
every record. Release sort segments as soon as their phase completes.

## 8. Optional authoritative-original comparison

When `--originals` is supplied, parse its TSV through the same bounded Plan 02
catalog rules. Require the exact set of manifest sample IDs, one R1/R2 source
pair each, role consistency, no extra/missing/ambiguous samples, and admitted
canonical file identities. Paths may be relocated explicitly by the sheet;
do not require the original absolute pathname or compressed container bytes.

Read supplied originals in lockstep using the ingress normalization mode.
Recompute per-sample counts, normalized mate byte totals, and normalized SHA-256
digests and compare with the complete reconstruction facts already proved.
Different gzip timestamps, compression levels, or accepted line endings do not
invalidate normalized content equality. A changed header, plus byte, sequence,
quality, pair order, role, or count does. Missing originals cannot silently
downgrade `pool_and_originals` to `pool_complete`.

Original file paths and raw identifiers do not enter ordinary diagnostics.
Report sample index, role, pair ordinal, and typed error context where useful.
This comparison is stronger than trusting a supplied manifest, but it still
does not make an unsigned pool a signed provenance record.

## 9. Failures, tests, and completion

Reject unsupported format/schema before data access. Corruption, path safety,
resource exhaustion, OS I/O, cancellation, and worker failure remain distinct
typed categories. No validation bypass or automatic repair is available.
Tests must check both the error class and the absence of a success report.

- **P07-001:** complete unequal-sample/many-bundle synthetic pool verifies with
  and without original TSV; empty samples and non-UTF-8 metadata reconstruct.
- **P07-002:** independently mutate every index header/entry field, virtual
  offset into member headers or record interiors, counts, ordering, and trailing
  bytes; recompute manifest file digests in some cases to reach structural checks.
- **P07-003:** duplicate/missing/substituted original identities, duplicated tags,
  inconsistent secured ordinals, swapped mates, and mismatched key metadata fail.
- **P07-004:** BGZF/.gzi/header/CRC/ISIZE/EOF defects, truncation and expansion
  limits, metadata hex/LF injection, and noncanonical decimal fields fail.
- **P07-005:** independently altered greedy boundaries or actual over-cap files
  fail even when manifest lengths/digests were updated consistently; decimal
  offset digit transitions exercise the independent hypothetical-row sizer.
- **P07-006:** changed/relocated originals distinguish normalized equivalence
  from true changes; TSV sample-set/role errors never downgrade validation scope.
- **P07-007:** low memory/descriptors/scratch, forced multi-pass external merges,
  lying source-byte declarations, ENOSPC, cancellation, and cleanup failures
  remain bounded and leave no plaintext scratch or false successful result.
- **P07-008:** private-mode, symlink, file-substitution, unsupported-format,
  duplicate-JSON-key, bool/NaN, and oversized-manifest adversarial cases fail.
- **P07-009:** source final quality ending in lone CR without LF survives ingress
  normalization, pool `canonical_lf` parsing, sorting, and digest verification.
- **P07-010:** independent tiny fixtures prove each bijection directly and
  confirm no producer placements or read-sized Python containers are consulted.

Complete when all test IDs pass, normal/low-resource execution agree, each
artifact is fully consumed and independently checked, optional originals have
an explicit report scope, and documentation describes the unsigned-manifest
trust boundary without claiming anonymity or release qualification.
