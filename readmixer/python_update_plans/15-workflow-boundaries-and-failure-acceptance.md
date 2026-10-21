# Plan 15 — Workflow boundaries and failure acceptance

Status: **Local implementation and bounded acceptance complete in `2.0.0.dev8`.**
[workflow-acceptance-evidence.json](workflow-acceptance-evidence.json) records
candidate-specific source and outside-checkout installed-wheel/console results,
exact scenario nodes, fingerprints and limitations. This closes local regression
work; supported-platform execution belongs to Plan 17 and physical large-cap,
resource/scale/performance qualification to Plan 16. Plan 10 remains the sole
release-completion gate. Historical dev2–dev7 evidence and archive hashes remain
unchanged. Sections 10–12 and the dev7 section retain their historical scope;
section 13 records the current completion and its independent checks.

Parents: [01](01-packaging-interfaces-and-compatibility.md),
[05](05-encrypted-scratch-and-safe-publication.md),
[06](06-bundles-keys-indexes-and-manifests.md),
[07](07-complete-verification.md), [08](08-multi-sample-recovery.md), and
[09](09-parallel-execution-and-performance.md).
Serial acceptance can start immediately. Metadata/inventory fault expectations
must follow [11](11-shared-metadata-budget-and-lifetimes.md) and
[12](12-paged-output-ownership-and-publication.md); process-specific cases depend
on [13](13-spawn-workers-and-bounded-storage-handoffs.md) and
[14](14-deterministic-parallel-pipeline.md).

## 1. Existing behavior and the remaining obligation

Historical [dev2 evidence](pipeline-evidence.json) records 1,122 source tests and
1,122 outside-checkout installed-wheel tests passing on local Linux CPython
3.11.7. It includes a real tiny five-run benchmark and bounded private-input
round trips. These are executed local results, not completion of this matrix.
The record and its source/archive fingerprints must remain historical.

Current [bundle emission](../../src/readmixer_py/artifacts/bundles.py),
[verification](../../src/readmixer_py/verification.py),
[scanning](../../src/readmixer_py/scan.py), and
[recovery](../../src/readmixer_py/recovery.py) already implement the complete
serial workflow. Their bounds and rejection paths now have the broader public-operation
coverage in section 13; these mechanisms were already implemented before this
acceptance increment, apart from the cleanup defect found by the new tests.

Useful existing regressions include
[test_pipeline.py](../../tests/test_pipeline.py),
[test_verification_faults.py](../../tests/test_verification_faults.py),
[test_pool_scan.py](../../tests/test_pool_scan.py),
[test_recovery.py](../../tests/test_recovery.py),
[test_recovery_lifetimes.py](../../tests/test_recovery_lifetimes.py), and
[test_transaction.py](../../tests/test_transaction.py).
The exact node `test_P07_005_independent_verifier_rejects_producer_early_rollover`
proves an early rollover rejection. It does not exhaust decimal-width boundaries.
`test_many_source_bundles_are_gathered_in_one_union_pass` checks artifact opens;
it does not establish selected-member reuse or bounded neighboring payload reads.

## 2. Independent complete tiny-pool reference

Add a bounded test reference for at most 128 artificial pairs. Explicit fixture
fields and numeric occurrence identities define expected normalized bytes;
production parsing, pair sizing, emission, index generation, and placement state
must not define their own expected results. Identical records at different
ordinals remain separate occurrences.

Use the already prescribed public fixture root/run identifier through private
harness seams only. Independently derive bucket/rank/tag transcripts, enumerate
canonical leaves, size prospective rows, and produce the expected secured order.
The independent cryptographic checks in [test_shuffle.py](../../tests/test_shuffle.py)
and [test_crypto.py](../../tests/test_crypto.py) are starting evidence.
The exhaustive Fisher–Yates distribution tests in
[test_shuffle_oracle.py](../../tests/test_shuffle_oracle.py) remain separate;
fixed-key rank-sort permutations need not equal Fisher–Yates permutations.

Render all six expected bundle artifacts using explicit wire structs, a small
independent canonical BGZF encoder with stdlib zlib, and independent `.gzi`
positions. Freeze the codec build/level, cap, topology, inputs, root and run ID.
Compare every artifact byte and every reconstructed occurrence, then validate
and compare manifests semantically with only `execution` excluded. A reference
must not call `BundleEmitter`, `pair_footprint`, `lane_fits`, or production key/
index encoders. Preserve independent literal goldens for the reference itself.
HTSlib interoperability is a separate oracle, not a demand that HTSlib choose
the same DEFLATE bytes. The reference is test-only, not another public engine.

## 3. Reachable cap boundaries and dominated bounds

Distinguish logical conservative admission from actual final physical lengths.
For common file cap `C >= 65564`, put `K = floor((C - 28) / 65536)` and
`S = 65280 * K`. A BGZF lane admitted with decoded bytes at most `S` has at most
`K` data members. Its standard sidecar is at most `8 + 16 * (K - 1)` bytes,
which is strictly below `C`. A sidecar cannot independently force ordinary
rollover while its corresponding lane still satisfies this admission rule.

A canonical key row has at least 65 bytes: seven decimal fields, a 32-character
tag, eleven commas and LF, and hex for two nonempty identifiers and plus markers.
The key header exceeds 64 bytes. Consequently, `64 + 40 * pairs` index bytes
are bounded by the key lane's decoded bytes, which already cannot exceed `S`.
The index cannot independently force ordinary rollover under this schema either.
Keep their explicit defensive checks; do not invent impossible valid-input cases
or weaken the common-cap contract to satisfy a test label.

Construct independent R1-limited, R2-limited, and key-limited fixtures. Vary mate
lengths asymmetrically and original metadata independently. Check exact `S` and
one-over projections, minimum/default caps, member-count transitions, oversized
singletons, and integer overflow. Use arithmetic fixtures for the default
5,000,000,000-byte cap; label any unperformed physical near-cap host run clearly.
Inspect physical sizes of every produced file, including markers and sidecars.

Drive offsets through 9/10, 99/100 and larger decimal-width boundaries. On
rollover, expected row sizes use new zero offsets. The verifier's hypothetical
append uses the previous bundle's prospective offsets, not observed next-bundle
row lengths. Force cases in which those widths change the admission decision.

Test index/GZI arithmetic and actual over-cap hostile files directly, with
consistent outer lengths/digests where needed. Test marker equality/one-over,
encoded versus parsed metadata limits, and total artifact inventory exhaustion.
Reach each intended limit independently; an unrelated earlier rejection is not
coverage. Use bounded internal ceilings where full-scale counts are unnecessary.

## 4. Self-consistent hostile pools through public operations

Parameterize mutations by the invariant broken, expected operation/scope, and
first structural check. Start from an independently valid artificial pool.
Where necessary update BGZF framing, sidecars, offsets, and manifest hashes so
outer integrity checks do not prevent reaching the intended inner validation.

Cover every mapping-index header and entry field; reserved values, ordering,
trailing bytes, record interiors, member headers, prior-member decoded endpoints,
EOF starts, and fabricated member starts inside skipped compressed payload.
Cover key ordinals, sample identities, tags, offset/length relationships,
noncanonical decimal/hex encodings, embedded LF, and original metadata markers.
Cover secured run IDs, role swaps, tags, ordinals, and non-bare secured plus lines.

Exercise missing/duplicate/substituted original domains despite equal totals,
including identical biological bodies and repeated original identifier text.
Exercise duplicate tags independently of original-domain coverage. Reject
broken joins even after outer hashes agree. Do not demand rejection of a wholly
replaced pool that remains internally consistent: unsigned hashes do not prove
historical provenance, and this plan introduces no signature format.

Run applicable cases through full verification and both recovery modes. Indexed
recovery validates its selected domain and required indexes/sidecars; sequential
recovery validates complete consumed streams but does not replace full global
bijection/tag verification. Unselected-payload cases must retain these different
expected outcomes. No indexed failure may trigger an automatic fallback.

## 5. Union cache and neighboring-read instrumentation

Build a multi-bundle pool where several selected samples share BGZF members,
records/key rows cross members, and selected regions are separated by unselected
payload. Include an empty selected sample and bundles with no selected records.

Instrument underlying compressed reads, member decompression, monotonic boundary
header traversal, cache hits/retention, index entry visits, and hash updates.
Assert one forward index traversal per bundle for the whole union, with every
index entry and both complete sidecars validated, including unselected entries.
Assert selected members are reused across samples without restarting a sample
scan, and retained decoded bytes stay within admitted cache leases.

Define expected neighboring reads from actual fixture record boundaries and
line-start evidence. A valid record can span more than two members under a
raised record bound; bound reads by required record bytes and boundary evidence,
not an invented constant of one neighboring block. Distinguish header traversal
through skipped members from decoding/hashing their payloads. Indexed mode must
not claim full hashes of unread FASTQ/key data. Sequential mode must never
resolve, stat, open, or hash the explicitly bypassed mapping index.

## 6. Stage-reached workflow resource and publication faults

Inject low memory/descriptors, pressure changes, scratch exhaustion, ENOSPC,
short writes, source-close failure, compression failure, and multi-pass merge
faults in actual mix/verify/recovery workflows. Include misleadingly small
manifest byte totals; actual decoded work and allocations remain authoritative.

Record a reached-stage witness before the injected action. Useful boundaries
include protected data already written, spill ownership acknowledged, a merge
child sealed, selected bodies provisionally gathered, an earlier sample staged,
compression drained, an artifact linked, marker linked, and directory synced.
Integrate [Plan 12](12-paged-output-ownership-and-publication.md)'s ownership
states instead of creating a second rollback model in the harness.

Check typed primary errors and bounded secondary cleanup notes, no successful
report before completion, and no successfully committed subset of a failed
ordinary recovery batch. Preserve source and foreign files, close every owned
reader/writer, release leases, and retain only explicitly reported cleanup residue.
Retain exception tracebacks during checks so borrowed buffers/descriptors cannot
hide behind garbage collection. Postcommit housekeeping warnings preserve the
valid complete set; cancellation keeps its documented exit status and marker
context. Marker visibility before its directory sync remains indeterminate.

## 7. Real signals, process exits, and crash residue

Use subprocess harnesses with explicit readiness/checkpoint acknowledgments;
fixed sleeps are not proof that the desired stage was reached. Deliver real
SIGINT/SIGTERM during parsing, native work, gather/merge, compression, publication,
and postcommit report rendering. Check CLI statuses 130/143, causal failures,
worker joining and descriptor release, and committed-marker context where due.

After Plans 13/14 exist, exercise unexpected child exit, broken IPC, blocked
oldest completion, and the specified terminate/kill/reap deadlines. Assert no
live owned writer after handled failure. Existing compression-thread injection
is useful evidence but cannot stand in for process-worker lifecycle tests.

Use a supervised, test-owned process group for SIGKILL cases. Inspect private
staging/scratch permissions and completion state after killing at acknowledged
pre-marker, marker-visible/pre-sync, and durable-commit boundaries. A hard stop
can leave owned residue; do not assert universal marker absence or cleanup.
A visible complete set and durable completion are different observations.
After durable commit, surviving artifacts must verify. Reap only harness-owned
processes; do not delete foreign files or implement automatic incomplete resume.

## 8. Planned scenario traceability

These IDs identify acceptance obligations, not pytest nodes. Section 13 connects
them to concrete files; the dev8 evidence records exact collected and executed
node IDs. The original parent and audit mappings remain unchanged below.

- **P15-T01:** tiny independent six-artifact reference and fixed-context matrix.
  Parents `P06-005/009`, `P07-010`, `P09-T02`; `AUD-01/02/18/19`.
- **P15-T02:** reachable lane limits, domination proofs, decimal transitions,
  physical cap and marker/inventory boundaries. Parents `P06-004/006`,
  `P07-005`, `P08-006`; `AUD-06/16/19/23`.
- **P15-T03:** self-consistent index/key/header/body mutation matrix.
  Parents `P07-002/003/004/008`, `P08-005`; `AUD-07/08/15/18/19`.
- **P15-T04:** union cache, neighboring reads, full required metadata scans,
  explicit scope and bypass. Parents `P08-003/004`, `P09-T03`; `AUD-11/18/19`.
- **P15-T05:** relocated/equivalent originals and sample/role/content mismatch;
  no scope downgrade. Parents `P07-006`, `API-05`; `AUD-09/18/24`.
- **P15-T06:** reached-stage resource/publication/cleanup failures in all public
  workflows. Parents `P05-T05/T06`, `P07-007`, `P08-008`;
  `AUD-10/12/13/14/17/18/19/23`.
- **P15-T07:** actual signals, child exits, broken channels and crash residue.
  Parents `API-03/07`, `P05-T06`, `P09-T05`; `AUD-12/13/14/19/22`.
- **P15-T08:** installed-command status, report redaction and source/foreign
  preservation for success, failure and cancellation. Parents `API-03/04/07`,
  `P06-007`, `P08-010`; `AUD-12/13/14/20/24`.

## 9. Completion evidence

Complete only after every planned case has a reached invariant/stage, independent
expected outcome, executed node, candidate identity, and resource/cleanup result.
Record missing mechanisms separately from missing regressions and unqualified
hosts. Ordinary local runs may close local cases; supported-platform execution
belongs to [Plan 17](17-platform-packaging-and-release-closure.md).
Do not convert a test skip, an earlier admission error, or collection success
into acceptance. This plan's completion does not complete Plan 10 or publish a
package; it contributes its bounded results to the final release decision.

## 10. Dev3 local boundary increment

The historical dev3 increment adds 25 regression cases in
`tests/test_rollover_boundaries.py`, including independent small-cap R1/R2/key limiting
fixtures, exact/one-over projections, physical artifact inspection and independent
sidecar parsing. It checks domination at every valid cap quotient through the
5,000,000,000-byte ceiling and proves rollover re-encodes decimal offsets at zero.
Real secured records have length `83 + 2*n`; fixtures use reachable neighboring
offsets (99/101, 999/1001 and 9999/10001), rather than impossible 9/10 offsets.

This covers part of `P15-T02`, not the complete scenario or this plan. The
remaining hostile-pool matrix, complete manifest/receipt/inventory exhaustion
coverage, reached-stage workflow faults, and real process signals/crash residue
remain required. Arithmetic cap domination is not a physical near-five-billion-byte
host run. The serial cache evidence in §11 and bounded complete artifact reference
in §12 are separate later increments. Existing ordinary regressions remain in place.


## 11. Dev4 serial acceptance increment

Two new test files add 18 local regression cases. Their focused run passed;
the separate dev4 integrated validation is recorded in
[ownership-evidence.json](ownership-evidence.json). These
results do not revise the historical dev2/dev3 artifacts or complete Plan 15.

[test_indexed_union_io.py](../../tests/test_indexed_union_io.py) adds six cases
for `P15-T04` (parents `P08-003/004`, `P09-T03`; `AUD-11/18/19`). The artificial
fixture has 112 pairs, three nonempty samples, an empty selected companion,
multiple bundles, and bundles containing no selected records. Explicit source
bytes define recovered expectations; stdlib gzip/zlib and literal index structs
provide the physical-member and occurrence oracle before observation begins.

- `test_fixture_has_required_union_witnesses` requires shared selected member
  occupants, selected secured records spanning more than two members, key-row
  continuations, and bundles without selected records. These conditions cannot
  pass vacuously because a fixture happened not to reach the intended layout.
- `test_indexed_union_reuses_members_and_skips_unneeded_compressed_payload`
  observes actual compressed read/seek intervals, inflation calls, cache hits,
  retained cache bytes and live leases, index visits, and real digest updates.
  It requires cross-sample reuse separately in R1, R2 and key lanes; selected
  members decode once, and reads are bounded by full selected record spans and
  predecessor-member line-start evidence. Skipped payload is distinguished from
  monotonic header/footer traversal. Every index and both sidecars are consumed
  and hashed completely, while unused bundles have no FASTQ/key payload reads.
  Indexed lane hashes remain absent; public scope and consumed-byte totals agree.
- `test_sequential_union_bypasses_even_resolution_of_missing_indexes` removes
  all indexes and guards resolution, stat and open interfaces. Recovery still
  succeeds, fully reads/hashes all five required lane/sidecar files per bundle,
  and reports `explicitly_bypassed` with `complete_bundle_streams` scope.
- `test_indexed_union_validates_required_metadata_in_unselected_bundle`, for
  index and both sidecar roles, corrupts a declared digest in an unused bundle.
  Entry and hash witnesses prove complete consumption before typed rejection;
  owned readers close and recovery leaves no published files.

[test_secured_field_mutations.py](../../tests/test_secured_field_mutations.py)
adds 12 cases for the secured-field portion of `P15-T03` (parents
`P07-002/003/004/008`, `P08-005`; `AUD-07/08/15/18/19`). The matrix crosses role,
non-bare plus, run-ID and pair-tag changes with public verification, indexed
recovery and sequential recovery. Independent stored-DEFLATE re-encoding updates
CRC and declared SHA-256 while preserving physical/decoded lengths and virtual
offsets. A join-stage witness proves rejection of the intended secured invariant
rather than an earlier outer-hash or framing failure. Recovery publishes no files.

This increment supplies local serial evidence for the union cache/scope cases;
it does not qualify future worker caches or performance. `P15-T03` still needs
the complete index/key/interior-address mutation and original-domain matrix.
The bounded `P15-T01` reference was still pending in dev4; §12 records its
separate dev5 implementation. The remaining `P15-T02` boundaries, broader
`P15-T05` original-input comparisons, `P15-T06` reached-stage resource/publication
failures, and `P15-T07/T08` real signal/crash/installed-command coverage remain open. Any
Plan 12 ownership-unit tests are separate evidence and do not replace workflow
fault injection; process-worker cases still require Plans 13/14.


## 12. Dev5 independent complete serial artifact reference

[test_serial_artifact_oracle.py](../../tests/test_serial_artifact_oracle.py)
implements the bounded local serial `P15-T01` reference (parents `P06-005/009`,
`P07-010`, `P09-T02`; `AUD-01/02/18/19`). Its 14 focused cases passed. This is a
new dev5 increment against the dev4 candidate baseline; integrated validation and
exact fingerprints are recorded separately in [spawn-evidence.json](spawn-evidence.json).
Historical dev3/dev4 evidence and archive identities are unchanged.

The reference has an explicit 128-pair ceiling and uses two 14-pair fixtures:
nine `alpha` occurrences, five `beta` occurrences, and an empty sample. Fields
are literal artificial bytes, including repeated complete records and original
identifiers at distinct ordinals, NUL/non-UTF8/CR metadata, empty reads in the
tiny fixture, concatenated gzip, plain inputs, mixed LF/CRLF and an accepted
missing final LF ending in a payload CR. File creation order deliberately differs
from canonical sample/role order. Expected normalized bytes do not use a
production parser, codec, writer, or placement helper.

The independent reference spells the HKDF transcript with stdlib HMAC, performs
keyed BLAKE2b bucket assignment, generates scalar ChaCha20 rank streams, interprets
separate little-endian high/low rows and sorts the resulting numeric ranks. AES
uses its primitive directly with an independent known-answer vector and the
literal identity transcript. The prescribed public test root/run ID is passed
only through the private production test seam. The fixed first-level topology
includes a witnessed two-pair rank reversal; actual terminal admission must fit
every independently computed leaf without refinement.

- `test_P15_T01_reference_primitives_have_independent_literal_goldens` pins
  HKDF, ChaCha20, AES-256 and complete stored-DEFLATE BGZF bytes, including the
  standard CRC32 check string and canonical empty EOF stream.
- `test_P15_T01_complete_tiny_reference_has_frozen_order_tags_and_artifact_hashes`
  pins the complete occurrence order, shared-terminal rank rows/permutation,
  pair tags and all six tiny artifact hashes. These literals protect the
  reference itself against accidental changes.
- `test_P15_T01_serial_six_artifacts_match_complete_independent_reference`
  supplies 12 cases: tiny/rollover fixtures × compression levels 0/6 × strict
  memory/external/auto storage. It independently performs greedy six-lane
  admission, re-renders bundle-local CSV offsets, compresses canonical members,
  builds standard sidecars and derives virtual offsets and literal index structs.
  Every produced artifact byte and every manifest field except `execution` must
  match. Public complete verification and both recovery modes then check every
  reconstructed field in sample/part/original-ordinal order, including empty
  outputs. Source files must remain byte-for-byte unchanged.

The rollover reference independently predicts five bundles containing
`[3, 3, 3, 3, 2]` pairs. Every R1/R2/key lane has two data members; explicit
witnesses require records/rows that cross members and index starts in nonzero
members. Physical file caps, complete inventory, private modes, counts, digests,
reported byte totals and cleanup are checked. Level-6 comparisons fix the zlib
build within each actual/reference comparison; they do not assert identical
DEFLATE bytes across builds.

This closes the implemented local serial reference obligation, not the full
fixed-context worker or supported-platform qualification. Forced recursive
refinement, injected rank collisions, worker scheduling/completion-order variants,
and cross-platform/dependency execution are outside this file. Existing component
refinement/collision and Fisher–Yates distribution oracles remain separate;
future process-worker equivalence depends on Plans 13/14. HTSlib interoperability
also remains a separate obligation. No production parallelism, resource-envelope
qualification, full Plan 15 completion or Plan 10 release completion is claimed.


### Dev7 production worker acceptance already supplied by Plan 14

Plan 14 adds real production parser, acknowledged encrypted-scatter, native
terminal and refinement failures (typed and unexpected exit), plus coordinator
emission cancellation with live pending workers. Each test proves its stage was
reached and checks source/foreign-file preservation, absent completion markers,
worker/descriptor/lease cleanup and diagnostic privacy. See
[test_parallel_workflow_faults.py](../../tests/test_parallel_workflow_faults.py)
and [parallel-pipeline-evidence.json](parallel-pipeline-evidence.json).
These cases narrow this plan's remaining work; they do not complete its broader
ENOSPC/short-write/pressure/multi-pass/publication/late-recovery and real workflow
SIGINT/SIGTERM/SIGKILL residue matrices or platform qualification.


## 13. Dev8 local completion and scenario ownership

The completed matrix combines the preserved independent checks with new public
workflow tests. Exact node IDs and source/installed results are in
[workflow-acceptance-evidence.json](workflow-acceptance-evidence.json); the audit
index is [regression-coverage.json](regression-coverage.json). A newly collected
node is counted only when the candidate's suite actually executes it.

- **P15-T01:** [test_serial_artifact_oracle.py](../../tests/test_serial_artifact_oracle.py)
  retains its independent primitive/order/tag/artifact goldens and twelve serial
  workflow configurations. [test_parallel_artifact_oracle.py](../../tests/test_parallel_artifact_oracle.py)
  adds six direct comparisons using actual admitted process workers at CPU bounds
  two/four, tiny/rollover inputs and all storage modes. Every six-artifact byte and
  manifest field except execution matches the independent reference; full verify
  and both recoveries independently reconstruct explicit expected occurrences.
  Canonical topology, root, run ID, cap, codec build and level are fixed within
  each comparison. This is a bounded representative matrix, not a full Cartesian
  sweep or a statement about DEFLATE equality across dependency builds.
- **P15-T02:** [test_rollover_boundaries.py](../../tests/test_rollover_boundaries.py)
  retains exact/one-over R1/R2/key projections, decimal-offset regeneration and
  index/GZI domination at every supported cap quotient.
  [test_workflow_boundaries.py](../../tests/test_workflow_boundaries.py) reaches
  completed artifact registration before independent physical/metadata marker
  equality and one-over refusal, reserves the completion inventory slot, proves
  whole-group inventory exhaustion and separates encoded/represented limits.
  [test_hostile_pool_matrix.py](../../tests/test_hostile_pool_matrix.py) supplies
  truthful over-cap index/GZI files. Bounded private ceilings isolate these tests;
  no physical near-five-billion-byte host run is claimed. Offsets 9/10 cannot be
  reached by secured records whose minimum length is 83 bytes; neighboring valid
  offsets straddle 100/1000/10000 instead.
- **P15-T03:** [test_hostile_pool_matrix.py](../../tests/test_hostile_pool_matrix.py)
  repairs outer lengths/hashes before testing every index header/entry field,
  reserved/order/tail checks, key fields and relationships, canonical decimals/
  hex, embedded LF, metadata markers, tag joins and complete occurrence domains.
  Physical addresses include record interiors, member headers, prior-member
  decoded endpoints, EOF and a fabricated BGZF start inside skipped payload.
  Byte-identical records and repeated original text do not conceal duplicate/
  missing/substituted occurrences. Valid-length biological body mutations reach
  normalized source-digest checks after their joins succeed.
  [test_secured_field_mutations.py](../../tests/test_secured_field_mutations.py)
  retains repaired role/run-ID/tag/plus mutations. Applicable cases run through
  full verify and both recovery modes with validator witnesses and no fallback.
  A truthful trailing index length fails the earlier exact manifest formula;
  bounded IndexReader tests retain the independent defensive tail check.
  Duplicate tags are a global verification obligation; selective recovery does
  not pretend to perform that global proof. An internally consistent permutation
  of indistinguishable occurrences remains valid: unsigned pools cannot prove
  historical provenance.
- **P15-T04:** [test_indexed_union_io.py](../../tests/test_indexed_union_io.py)
  retains independently derived record/member boundaries and actual read/seek,
  inflation, hash, cache/lease and index-visit instrumentation. The fixture proves
  shared selected members, records spanning more than two members, crossing key
  rows, empty selections and unused bundles. All required indexes/sidecars are
  consumed once for the union; selected payload members are reused. Sequential
  mode never resolves/stats/opens the explicitly bypassed indexes.
  [test_recovery_scope_acceptance.py](../../tests/test_recovery_scope_acceptance.py)
  repairs outer hashes on corrupt unselected payload: indexed recovery succeeds
  with exact selected contents and its narrow scope, while full verify and
  sequential recovery reach the CRC error and clean up. The hostile matrix also
  distinguishes unselected key/domain corruption from selected validation.
- **P15-T05:** [test_originals_acceptance.py](../../tests/test_originals_acceptance.py)
  checks relocated paths, repeated basenames, changed compression/container bytes
  and physical newlines against independently expected normalized originals.
  Sample catalog/identity, role, header, plus, sequence, quality, occurrence order,
  count and missing-file failures are witnessed after complete pool verification.
  Explicit originals requests never downgrade to pool-only success. Sources,
  foreign files, descriptors and metadata leases survive or release as required
  while caught tracebacks remain live.
- **P15-T06:** [test_workflow_acceptance_faults.py](../../tests/test_workflow_acceptance_faults.py)
  exercises mix, verify and both recoveries after authenticated run sealing:
  transient RAM/descriptor refusal at a subsequent real workflow admission,
  pressure reduction, scratch-credit exhaustion, partial ENOSPC and zero writes,
  and sealed children in later merge passes. Positive partial writes finish with
  correct contents. Additional checkpoints cover staged artifacts, an earlier
  drained compression lane, source closure, actual work despite understated
  manifest totals, and a completely staged earlier recovery sample followed by
  a later digest failure. Publication faults occur after real artifact/marker
  linking and at the corresponding directory syncs. Precommit failure rolls back
  the batch; foreign replacements retain their distinct inode and bounded
  uncertain-residue warnings rather than authorizing guessed deletion.
  Postcommit housekeeping warnings preserve the complete set.
  Prior ownership, interrupted handoff, late-gather and production-worker tests
  remain complementary evidence. Failures retain their first typed cause,
  bounded notes, source/foreign preservation and immediate owner cleanup.
- **P15-T07:** [test_workflow_signals.py](../../tests/test_workflow_signals.py) uses
  [workflow_signal_harness.py](../../tests/workflow_signal_harness.py), a guarded
  subprocess helper with explicit readiness acknowledgments. Both real SIGINT
  and SIGTERM interrupt nine stages: parser, retained native result, indexed and
  sequential gather, verify scan, later merge pass, compression, artifact
  publication and postcommit JSON rendering. The worker/native result and codec
  thread checkpoints follow completed native calls before result handoff; they
  do not claim cancellation at a particular C instruction. Broken IPC after
  actual terminal work remains a causal resource failure. Existing production
  worker-exit tests, blocked-oldest completion and terminate/kill/reap tests
  supply the remaining lifecycle lanes, with production 10/5-second constants
  checked independently of accelerated test deadlines.
  SIGKILL uses a supervised single-process group with no descendants at three
  acknowledged boundaries: pre-marker, marker-visible/pre-sync and committed.
  Private residue permissions and source/foreign inode snapshots are checked;
  only durable-commit survivors are independently required to verify. Visibility
  before sync is not proof of durability, and no cleanup/resume is promised after
  an uncatchable stop or a real power loss.
- **P15-T08:** the signal matrix and
  [test_cli_workflow.py](../../tests/test_cli_workflow.py),
  [test_originals_acceptance.py](../../tests/test_originals_acceptance.py) and
  [test_api.py](../../tests/test_api.py) check success/failure/130/143 statuses,
  absence of success reports on failure, redaction, no scope downgrade and
  generated marker context after commit. Installed validation selects the exact
  console executable in a fresh outside-checkout environment. Signal harnesses
  call that installed package's same CLI entrypoint, preserving injection
  witnesses without introducing runtime test options.

### Runtime defect found and repaired

[Pool.close](../../src/readmixer_py/pool.py) previously stopped at a source-close
exception, retaining its manifest/registry leases on failed verification. Cleanup
now attempts the remaining anchored directory and both metadata owners even when
an earlier cleanup step fails. It preserves the first causal error, adds bounded
secondary notes, and retains an unreleased lease for retry. Constructor cleanup
also preserves an admission error if cleanup fails. Before/after source-close,
primary-plus-secondary and constructor-failure tests hold tracebacks live while
checking all owned descriptors and charges. No pool format, CLI, algorithm or
public report ownership contract changes.

### Completion and remaining qualification

All eight local obligations have executable acceptance owners above. Current
candidate evidence distinguishes source tests, installed console/package tests,
archive/source hashes and bounded private-prefix validation from historical
results. Supplied originals remain unchanged. The complete 30,042,389-pair
mix/verify/both-recovery run is still a Plan 16 task.

Linux x86-64 CPython 3.11.7 local acceptance does not qualify macOS arm64,
CPython 3.14, dependency locks, constrained resources, native memory coefficients,
representative speedup or physical near-default caps. Plans 16/17 own those lanes;
Plan 10 remains the sole release-completion gate and package publication remains
separate. No deferred feature scope has been added.
