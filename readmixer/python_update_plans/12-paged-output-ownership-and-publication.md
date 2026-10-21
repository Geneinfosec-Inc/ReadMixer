# Plan 12 — Paged Output Ownership and Publication

Status: Implemented locally in `2.0.0.dev4`; supported-platform and measured
scale qualification remain with Plans 16/17. See
[IMPLEMENTATION.md](IMPLEMENTATION.md) and
[ownership-evidence.json](ownership-evidence.json) for candidate-specific results.
Historical dev2/dev3 evidence and archives remain unchanged.

Prerequisites: [11](11-shared-metadata-budget-and-lifetimes.md), the implemented
authenticated storage/ledger interfaces, and the existing
[05](05-encrypted-scratch-and-safe-publication.md) publication contract. Follow
this plan with process execution/lifecycle Plans 13 and 14. Plan 15 extends
workflow fault coverage, Plan 16 qualifies scale, and Plan 17 records the exact
release candidate. Plan 05 continues to own the cryptographic transcript.

## 1. Baseline and required change

[transaction.py](../../src/readmixer_py/transaction.py), `OutputTransaction`,
already creates private exclusive partials, validates closed bytes and inodes,
uses no-replace hard links, syncs directories, publishes the completion marker
last, and preserves durable outputs during cleanup failure. Keep those semantics.

The historical dev2/dev3 `_artifacts` list retained every mutable `Artifact`;
`_names` retained every basename. Their admitted RAM cap could reject further
growth. Dev4 removes both through authenticated paging. Cleanup still relies on
verified ownership records rather than paths from an untrusted directory listing.

[ledger.py](../../src/readmixer_py/ledger.py), `PagedLedger`, already seals
immutable chained pages, admits a trusted root, and authenticates reverse reads.
Its ordinary `close()` discards pending RAM records because existing callers do
not transfer external-file ownership through those records. That behavior must
not discard pending output ownership in the new journal.

## 2. Operation lifetime and interfaces

Use one operation-wide `SegmentStore`, `OperationKeys`, identity allocator, and
`DiskBudget` for data and ownership ledgers. `SegmentStore.allocate_id()` is
currently store-local and starts at one: creating a second store with the same
keys would reissue identities. This plan chooses one store, not a new KDF or a
second independently initialized namespace.

Extend `OutputTransaction.begin(..., store=...)` with the operation store. The
transaction is the outer cleanup coordinator after successful begin: it borrows
the store while data stages run and closes ownership ledgers/store during final
cleanup. A begin failure leaves the store with its creator, which must close it.
Transfer this cleanup responsibility explicitly; do not keep two context exits
that can turn an already handled postcommit cleanup warning into a new failure.

Finish all pair/sort readers, their seals, codec workers, and data-stage ledgers
before final publication. Keep ownership bookkeeping and its keys alive through
commit/rollback cleanup. Thus "all scratch closes before publication" becomes
"all data-processing scratch closes before publication; ownership bookkeeping
survives publication cleanup." Precommit inability to clean data scratch still
aborts. Postcommit ownership cleanup failure preserves final outputs and uses
the existing `cleanup_complete=false` report contract.

Keep public CLI/configuration, manifests, receipts, artifact names, and final
wire bytes unchanged. Internal `create_partial`, `register_closed`,
`open_owned_partial`, `publish_all`, and `abort` retain their roles. Tests may
inject stores and faults through internal constructors, not production flags.

## 3. Bounded inventory and pending handoff

Add an internal `OutputOwnershipJournal` backed by two `PagedLedger` instances:
a registered-artifact inventory and a successful-final-link journal. Both use
existing registry-purpose encryption, 64 KiB pages, at most 1,024 records per
page, and records bounded to 2 KiB. Admit two trusted roots, pending page buffers,
current decoded pages/cursors, and cleanup overlap under Plan 11 before accepting
output files. Strict memory retains admitted pages and never writes disk scratch;
auto/external pages use the existing authenticated spill behavior.

The transaction retains at most 64 active/unregistered handles, further limited
by shared metadata and descriptor admission. Count closed but unregistered
handles against this bound. Current emitters need only their active bundle/part.
Each handle owns its new stage file until `register_closed` validates and hands
off one immutable inventory record. Keep only the bounded active-handle map;
completed `Artifact` handles may remain with an emitter as small immutable
read-access tokens, but the transaction must not retain them all.

Freeze the private record codec as the following concatenation, with every
multibyte integer big-endian:

    b"RMPYOWN1" || version:u8 || type:u8 || artifact_ordinal:u64
    || basename_length:u16 || ASCII(basename)
    || device:u64 || inode:u64 || registered_length:u64 || SHA256:32 bytes
    || registered_mtime_ns:i64 || registered_ctime_ns:i64

Version is 1; type is 1 for an inventory record and 2 for a successful-link
record. Artifact ordinals start at zero. Basenames have 1..255 bytes and satisfy
the existing generated-basename validator. Timestamps are signed 64-bit
nanoseconds; other numeric fields are unsigned. Reject overflow before encoding,
unknown versions/types, invalid names, mismatched ordinals, inconsistent length,
and trailing bytes before acting. Retain the 2 KiB outer record admission cap.
Link records carry the same validated facts as their inventory record, so cleanup
needs no whole-run lookup table. This is an internal scratch codec, not a new
public artifact schema.

Keep total created/registered counts and monotonic ordinals in admitted scalar
state. Each handle can register once, in creation order. The private journal rejects
registration ordinal gaps/duplicates and link ordinals outside reverse creation
order before a record enters its canonical ownership boundary. Publication requires no active handles and
equal created/registered data-artifact counts. The completion marker retains one
separate bounded handle and is included in the 100,000-file overall ceiling.

Remove the growing duplicate-name set. During data-artifact acceptance, stage
names are never removed; exclusive stage creation therefore rejects duplicates.
Continue checking generated basenames and reserving the marker name before any
creation. No new data artifact can be accepted once publication starts.

Resolve pending journal ownership explicitly:

1. Pre-encode and admit a bounded record before the external ownership change.
   A pending create or link intent owns the syscall-to-handoff interval.
2. Journal append copies into a pre-admitted pending page, then advances one
   canonical ownership commit marker: its owned pending-record boundary. That
   transition transfers deletion and disk-credit authority, even before sealing.
   Active handles and pending records reference the same bounded ownership state;
   they are not independent cleanup owners. Housekeeping may retire a source
   handle afterward, but an interruption there cannot create two authorities.
   Before the marker advances the source owns cleanup; afterward cleanup observes
   the journal as sole owner, including when append never returned to its caller.
3. A full page seals through the existing trusted-root handoff. Keep the source
   pending bytes and charges until the new root owns them; failed sealing cannot
   erase those records. Flush and authenticate inventory pages before linking.
4. Add an explicit ownership-drain API rather than changing ordinary ledger
   close into implicit external-file deletion. It visits owned pending records
   newest-first, then authenticated sealed pages newest-first. Ordinary close
   must reject an undrained ownership journal, never clear its owned pending bytes.

The pending buffer has fixed page capacity and bounded record-offset storage;
do not maintain a separate growing list. Cancellation cannot bypass ownership
handoff or turn a pending file into an untracked path.

## 4. Publication and cleanup protocol

Preallocate the success report and retain admitted completion bytes before final
links. Freeze the registered inventory and validate its complete counts/chain.
Walk it newest-first; deterministic reverse creation order has no final-format
meaning and avoids building a forward index in RAM.

For each artifact, recheck anchored stage identity/private mode and the registered
snapshot, reserve one pending link intent, and call no-replace `link`. On syscall
success, transfer the intent to the successful-link journal. An `OSError`,
including `EEXIST` for a preexisting hard link to the same staging inode, creates
no final-name ownership. Interruption immediately after success remains covered
by the pending intent and identity-checked rollback.

After all links and complete journal validation, sync the destination; remove
owned staging links through inventory traversal and sync changed directories.
Create/validate/sync the marker partial, link it last, and sync the destination.
Only that final successful sync enters the committed state. Preserve existing
indeterminate-durability reporting if the marker link exists but sync fails.

Precommit cleanup removes the marker link first, pending/successful owned final
links second, then active and inventoried staged files. Postcommit cleanup visits
only staging and algorithmic scratch; it never removes final links. Recheck
device/inode immediately before unlink. Missing names are harmless; substituted
inodes and preexisting names are never deleted. Never use recursive deletion of
caller-owned trees or infer ownership from a matching filename alone.

Each cleanup cursor holds one pending/decoded page and at most its 1,024 bounded
progress flags. Process external-file records before dropping the page. Prepare
the trusted predecessor/root transition before page deletion, retaining the
transition across interruption exactly as the existing ledger cleanup does.
Successful actions and disk-byte releases are recorded in page-local progress
before retry; never release physical ownership twice. Final and staging hard
links share one physical-byte charge. Committed final bytes remain charged for
the remaining operation lifetime.

Retry an interrupted idempotent cleanup action once, preserving the first cause.
Attempt other records in the current page and independent owners. If a record
cannot be retired safely, retain that page and its older chain rather than
deleting the only remaining ownership references. Keep cursors bounded; do not
accumulate failed-record lists. Report bounded cleanup warnings/residue when
retries are exhausted. There is no persisted key or supported resume operation.

Corrupt, substituted, truncated, or unreadable pages stop that ownership traversal.
Never guess their paths or bless cleanup from unauthenticated bytes. Before
commit this invalidates success and preserves the primary failure; after durable
commit it leaves valid final outputs with incomplete-cleanup status. Keys remain
available for all feasible protected cleanup, then references are released.

## 5. Scenarios and traceability

Preserve [test_transaction.py](../../tests/test_transaction.py) and its actual
link/fsync/write faults, same-inode `EEXIST`, replacement protection, pending-link
interrupt, and durable-commit tests. Existing `test_ledger.py` authenticates page
chains; new tests must exercise external-file ownership through those pages.

- **P12-T01:** many artifacts cross many inventory/link pages with bounded
  resident handles, roots, metadata, and descriptors; count equality/one-over
  includes the marker. Parent `P03-T05`; `AUD-10`, `AUD-14`, `AUD-23`.
- **P12-T02:** memory pages remain RAM-only; auto/external ownership pages are
  encrypted and contain no plaintext path journal. One store never reissues IDs.
  Parent `P03-T04`/`P03-T05`, `P05-T02`/`P05-T03`; `AUD-10`, `AUD-13`.
- **P12-T03:** inject failure/interruption around create, register, pending append,
  page seal/root handoff, and link/journal handoff. Every surviving owned file has
  exactly one cleanup owner. Parent `P03-T03`, `P05-T05`; `AUD-12`, `AUD-14`.
- **P12-T04:** mutate page identity/order/length/seal and record names/inodes;
  cleanup preserves foreign files and undecodable residue. Parent `P05-T02`,
  `P05-T04`/`P05-T05`; `AUD-13`, `AUD-14`.
- **P12-T05:** marker-first rollback, post-link sync failure, partial page cleanup,
  repeated cleanup, page-drop interruption, and postcommit store cleanup failure
  preserve durability classification and primary errors. Parent `P05-T05`;
  `AUD-12`, `AUD-14`, `AUD-17`.
- **P12-T06:** public mix and both recovery modes use paged ownership across many
  bundles/parts; ENOSPC, metadata exhaustion, and cancellation never publish an
  invalid marker. Retain cap/receipt limits. Parent `P03-T06`, `P07-007`,
  `P08-006`/`P08-008`; `AUD-10`, `AUD-12`, `AUD-14`, `AUD-17`, `AUD-23`.

## 6. Completion checklist

- [x] No transaction-owned whole-inventory `Artifact` list or basename set remains.
- [x] Pending records have explicit bounded ownership and cannot be discarded.
- [x] One namespace/store survives until ownership cleanup; data scratch closes
  before publication and postcommit errors retain committed outputs.
- [x] P12 scenarios and existing transaction/ledger/workflow regressions pass.
- [x] Documentation retains plaintext final-staging, non-atomic visibility,
  crash-residue, unsupported resume, and metadata-cap limitations.

Paging ownership does not guarantee that every 100,000-file manifest/receipt fits
its admitted resident representation. Process reaping remains Plans 13/14 work;
real signal/SIGKILL workflow scenarios (`P05-T06`) belong to Plan 15. Plan 16 owns
measured scale and Plan 17 owns supported-platform qualification. This plan must
not claim those gates from unit faults.

## 7. Local implementation anchors

- [ownership.py](../../src/readmixer_py/ownership.py): exact codec, bounded tickets,
  inventory/link ordering, authenticated iteration and explicit ownership drain.
- [ledger.py](../../src/readmixer_py/ledger.py): owned pending boundary, current-page
  progress/refund state, interrupted page retirement, terminal abandonment.
- [transaction.py](../../src/readmixer_py/transaction.py): active-handle cap,
  exclusive duplicate rejection, one-store publication and marker-first rollback.
- [disk.py](../../src/readmixer_py/disk.py): bounded idempotent `DiskRefund` handoff;
  [storage.py](../../src/readmixer_py/storage.py): terminal residue-preserving release.
- [test_ownership.py](../../tests/test_ownership.py),
  [test_paged_transaction.py](../../tests/test_paged_transaction.py), and
  [test_transaction_review.py](../../tests/test_transaction_review.py) extend the
  original transaction/ledger faults. The public 80-pair artificial long-record
  case crosses real ownership pages in mix and both recovery modes and compares
  independent normalized contents. It is not a scale or host qualification.

Final-format staging remains private plaintext. The public schema, shuffle and
CLI contracts are unchanged. Ownership paging does not remove resident manifest/
receipt limits, promise cleanup after SIGKILL, or enable automatic resume.
