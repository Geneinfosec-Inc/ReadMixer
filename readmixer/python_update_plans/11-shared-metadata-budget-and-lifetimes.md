# Plan 11 — Shared Metadata Budget and Lifetimes

Status: **Implemented locally in `2.0.0.dev3`**. Shared accounting, retained
object ownership and serial operation integration have regression coverage.
Measured resource/platform qualification remains with Plans 16/17; downstream
paged ownership and scheduler integration remain Plans 12–14. This is not release
qualification. See [IMPLEMENTATION.md](IMPLEMENTATION.md) and
[metadata-evidence.json](metadata-evidence.json) for exact execution scope.
Historical [pipeline-evidence.json](pipeline-evidence.json) remains unchanged.

Prerequisites: the implemented interfaces and ownership contracts from
[03](03-resource-admission-and-storage.md) and
[05](05-encrypted-scratch-and-safe-publication.md). Implement this plan before
[12](12-paged-output-ownership-and-publication.md), then the process execution
work in Plans 13 and 14. Plan 15 owns the remaining workflow acceptance matrix,
Plan 16 owns measured scale/resource qualification, and Plan 17 owns exact
candidate release evidence. Those gates do not become complete with this plan.

## 1. Historical dev2 starting point

[resources.py](../../src/readmixer_py/resources.py), `resolve_resources`, already
resolved `B`, canonical terminal slice `T`, descriptor allowance, and
`ResourcePlan.metadata_bytes = min(64 MiB, B // 16)`. `Budget.acquire`, `Lease`,
and `Budget.checkpoint` enforce aggregate admitted RAM and descriptors, preserve
transferred ownership, and react to pressure. Dev2 did not enforce that metadata
ceiling across owners; dev3 adds that shared dimension.

Independent dev2 reservations were larger than a shared allowance:
`mixing._mix` reserves three metadata allowances for retained state and another
five for manifest serialization; `pool.Pool` reserves five allowances for the
manifest plus a per-artifact identity registry. Catalog `_Admission` has its own
representation checks. These are real aggregate admissions, but several
independent category caps do not satisfy Plan 03's shared ceiling.

Existing authenticated paging in `ledger.PagedLedger`, `sorting.ExternalSorter`,
and the shuffle frontier is implemented. Reuse it. This plan adds the missing
accounting dimension and fixes object lifetimes; it does not replace those codecs,
the terminal envelope, the shuffle algorithm, or the final artifact schema.

## 2. One atomic admission model

Use `M = ResourcePlan.metadata_bytes` for the entire invocation. Extend
`Budget(..., metadata_limit_bytes=None)`; `None` derives
`min(64 MiB, limit_bytes // 16)`, while operation construction passes resolved
`M` explicitly. Explicit values must be nonnegative and no larger than that
derived ceiling. No CLI option can raise `M` independently of `B`.

Extend `acquire(category, bytes, descriptors=0, *, metadata_bytes=0)` and `Lease`
with an integer metadata charge. Require `0 <= metadata_bytes <= bytes`.
Metadata bytes are a subset of the aggregate byte charge, not an additional
aggregate debit. Check all dimensions under the existing budget lock, before
constructing the admitted representation or opening its descriptors. Rejection
raises safe `ResourceError` category/request/available figures without paths,
sample identities, records, or keys.

Expose bounded counters `used_metadata_bytes`, `remaining_metadata_bytes`, and
`peak_metadata_bytes`. Keep `M` and `T` fixed after resolution. Pressure may
restrict aggregate admission and trigger existing spill/cancellation behavior;
it must not enlarge metadata capacity or alter canonical terminal boundaries.

`Lease.transfer` preserves all dimensions without a released interval. Add
atomic `resize(bytes, descriptors=0, *, metadata_bytes=0)` and
`split(owner, bytes, descriptors=0, *, metadata_bytes=0)` for staged construction
and ownership handoff. A split preserves the combined charges and leaves both
resulting metadata amounts valid subsets of their byte amounts. Allocate new
handles before modifying counters. Failed allocation, failed admission, or an
invalid request leaves the original lease unchanged. Release remains idempotent.

Increasing a lease precedes allocation; shrinking follows removal of the
corresponding references/buffers. Split/transfer replaces release-then-acquire
handoffs. The bounded lease registry, trusted-root registry, and their own
control objects require an explicit admitted metadata owner; do not recursively
create one lease for every accounting bookkeeping object. Retain the existing
active-lease count cap as an additional limit.

## 3. Metadata classification and ownership

Charge the following resident state to both aggregate RAM and `M`:

- Catalog samples, paths, source snapshots, discovery frontier, deduplication
  sets, ordering workspace, and retained per-source reports.
- Resident segment entries, reader-pin bookkeeping, trusted ledger roots,
  pending registry records, decoded registry pages and cursor ranges. Strict
  memory also charges its retained registry-page representation; payload pair
  segments remain payload rather than metadata.
- Sort-run/frontier registries, identity-coverage sample offsets/counts, and
  scheduling/result descriptors. A payload arena or coverage observation stream
  does not become metadata merely because an index refers to it.
- Parsed manifest/receipt objects, retained encoded completion bytes, manifest
  admission token/object bookkeeping, pool file snapshots, selector sets,
  output ownership, bundle/part descriptors, and report construction/retention.
- Per-operation disk-device/reservation bookkeeping and the bounded controller
  registries needed by the later scheduler.

Reusable pair, compression, cryptographic, and generic stream I/O workspaces
remain aggregate-RAM charges. Their containing metadata objects are charged
separately. Do not charge the same backing allocation once through a container
and again through an alias. Conservative representation upper bounds are
acceptable; undocumented exclusions and whole-run uncharged dictionaries are not.

Split mixed leases such as `PagedLedger`'s current page/root/cleanup reservation:
root/page/range storage is metadata, while reusable authenticated-reader overlap
is ordinary workspace. Account each actual resident registry representation in
memory mode, including simultaneously retained encoded/decoded copies.

Use existing checked arithmetic and representation preflights before allocation.
Replace blanket `3 * metadata`/`5 * metadata` reservations with conservative
content-dependent construction and retained-state charges. Reserve dictionary,
set, list, tuple, string, and object overhead as well as encoded bytes. Unknown
native workspace remains in its separately versioned aggregate envelope.

Catalogs and completed JSON representations must fit their admitted resident
representation or fail. This plan does not add a streaming manifest schema.
Auto/external owners page eligible registry growth; strict memory admits the
resident pages or rejects growth without disk scratch. A failed page handoff
retains both its source ownership and the overlapping charges until cleanup.

## 4. Integrate construction, close, and return lifetimes

Pass the same budget through `runtime.operation_resources`, catalog construction,
`SegmentStore`, `PagedLedger`, sort/coverage owners, `Pool`, transaction, emitters,
and every public operation. Do not derive a fresh metadata allowance per owner.
Standalone internal tests may construct their own budget explicitly.

In [catalog.py](../../src/readmixer_py/catalog.py), connect `_Admission.reserve`
to an owned lease and transfer construction charges to the returned catalog.
Maintain separate source-descriptor close and metadata disposal lifetimes:
`SampleCatalog.close()` currently closes only `_root_fd` and leaves `samples`
resident. Keep those objects charged until discarded or handed off. Do not
release their charge simply because ingress finished.

In `Pool`, reserve the bounded encoded input and conservative parsed cost before
decoding, then release only representations no longer retained. `close_sources`
closes descriptors without retiring manifest/file-identity ownership. Final
disposal clears owned references before releasing their representation charge.
Local aliases held by verification/recovery must remain under the operation's
retained-state owner until their final use.

In `mixing`, `artifacts.bundles`, and `recovery`, charge appended source, bundle,
part, and output descriptors before retention. Shared dictionaries referenced by
receipt and report are one allocation with explicit ownership, not two free
aliases. Keep completion source objects, serialization buffers, encoded bytes,
transaction metadata, and the preallocated success report admitted together.

An emitter's codec close is not disposal of descriptors still used to encode a
manifest or receipt. A public operation retains report charges through final
construction and postcommit cleanup, then transfers the bounded result to its
caller on return. Do not add a public context-managed report or require callers
to release internal leases. Failed operations drop retained graphs and borrowed
views before releasing charges, including when exception tracebacks survive.

Preserve the independent limits: 10,000 samples, 20,000 input files, 16 MiB
encoded catalog, 100,000 final files including the marker, 1,024 trusted roots,
and bounded metadata documents. A smaller shared or aggregate budget can reject
an object before those maxima. Do not present a maximum as guaranteed capacity.

## 5. Scenarios and traceability

Existing [test_resources.py](../../tests/test_resources.py) transfer, concurrent
admission, pressure, and allocation-failure tests are the regression baseline.
Existing catalog/ledger/storage tests establish isolated bounds and ownership;
they do not yet prove shared admission. Add the following named scenarios and
map actual collected node IDs only after implementation.

- **P11-T01:** two owners each fit individually but exceed `M` together; exact
  equality succeeds and one byte over fails before mutation. Aggregate bytes
  include metadata once. Parent `P03-T03`/`P03-T05`; `AUD-10`, `AUD-22`.
- **P11-T02:** concurrent acquire, resize, split, transfer, allocation failure,
  cancellation, and repeated release preserve all counters and lease ownership.
  Pressure leaves `M`/`T` unchanged. Parent `P03-T03`; `AUD-10`, `AUD-22`.
- **P11-T03:** catalog construction, ledger roots/pages, segment entries, and
  output metadata compete for one ceiling; exact count and byte boundaries
  reject before retention. Parent `P03-T05`/`P03-T06`; `AUD-10`, `AUD-23`.
- **P11-T04:** memory registry pages never spill; auto/external page handoff
  releases resident metadata only after authenticated ownership succeeds.
  Failed handoff retains its source. Parent `P03-T04`/`P03-T05`; `AUD-10`.
- **P11-T05:** source/codec close does not free retained catalog, manifest,
  descriptor, or report charges; final disposal and failure paths do. Include
  retained tracebacks and shared descriptor aliases. Parent `P03-T03`/`P03-T05`;
  `AUD-10`, `AUD-12`, `AUD-23`.
- **P11-T06:** admitted public mix, verify, and both recover modes complete with
  catalog/manifest/receipt serialization overlap charged; hostile representation
  expansion and exhausted metadata fail before publication. Parent `P03-T06`,
  `P07-007`, `P08-008`; `AUD-10`, `AUD-12`.

## 6. Completion and qualification boundary

- [x] Every current resident metadata owner has a bounded representation and one shared
  admission path; category names are not used as implicit policy inference.
- [x] Local P11 scenarios pass with existing resource/storage/serial workflow tests.
- [x] Counters demonstrate no successful shared-ceiling oversubscription and
  no release while operation-owned representations remain live.
- [x] Resource documentation describes both aggregate and metadata limits,
  lower effective capacity, and cleanup/returned-report lifetimes.
- [ ] Plan 12 consumes this interface; later worker admission uses the same
  dimension rather than creating independent child allowances.

Admission accounting is not an operating-system RSS cap. Python/native envelope
calibration and process-tree measurements remain `P03-T07` and Plan 16 work.
Only Plan 17 may close the exact candidate/platform evidence gate.

## 7. Dev3 implementation and local evidence

`resources.Budget` admits one metadata subset, including its own bounded lease
registry, and commits all three resource dimensions through one authoritative
state. Resource admission envelope version 2 remains unqualified until measured.
`catalog` retains its lease while borrowed records remain alive.
`Pool.close_sources()` retains manifest/file identities; final disposal drops
those graphs. Registry-purpose memory segments and decoded ledger pages are
metadata, while generic codec/payload workspaces remain ordinary RAM.

`metadata.serialization_lease` counts canonical JSON bytes with bounded chunks
and admits validation/encoding copy overlap before materialization. Mix and
recovery retain their source/output descriptors through publication. Returned
reports transfer to caller ownership without a public close API. Transaction
ownership is still a capped RAM inventory; Plan 12 will replace its representation.

Current regression anchors are `tests/test_resources.py` (`P11_T01`/`P11_T02`),
`tests/test_catalog_metadata.py`, `tests/test_storage_metadata.py`, `tests/test_ledger.py`,
`tests/test_disk_paths.py`, and `tests/test_metadata_workflows.py`. Exact collected
nodes and actual source/installed results are listed in the dev3 evidence. These
local checks do not qualify 100,000-file capacity, native RSS envelopes or another
platform. Encoded/count maxima remain independent refusal limits, not guaranteed
capacity at every smaller RAM budget.
