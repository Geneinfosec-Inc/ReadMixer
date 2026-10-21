# Plan 05 — Encrypted Scratch and Safe Publication

Status: Planned. Protocols and tests below are implementation requirements,
not statements that Python encryption, durability, or cleanup has been verified.

Prerequisites: [Plan 01](01-packaging-interfaces-and-compatibility.md) for errors,
[Plan 03](03-resource-admission-and-storage.md) for leases/storage modes, and
[Plan 04](04-global-pair-randomization-and-sharding.md) for canonical identities.
This plan owns the sole cryptographic transcript; other plans must reference it.
The Plan 04 prerequisite is its agreed codec/identity contract, not a completed
shuffle: implement Plans 02/03/05 foundations before the Plan 04 runtime.

## 1. Evidence, threat boundary, and scope

Python creates plaintext chunk/key files
([scramble.py:257](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:257)),
writes final names directly
([scramble.py:459](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:459)),
and swallows compression failures
([scramble.py:603](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:603)).
The existing mkdtemp at
[scramble.py:79](/home/user/Desktop/GeneInfoSec/Open_Source/ReadMixer/readmixer/process/scramble.py:79)
is already private; do not misdescribe it as a public directory.

Transfer purpose-separated keys and authenticated lifecycles from Rust
[kdf.rs](../../crates/readmixer-core/src/kdf.rs),
[tempstore.rs](../../crates/readmixer-core/src/tempstore.rs),
[output.rs](../../crates/readmixer-core/src/output.rs), and cancellation ordering
in [pipeline.rs](../../crates/readmixer-core/src/pipeline.rs).
The Python protocol is new and is not Rust v2/v3 compatible.

Protect algorithmic disk scratch against disclosure and undetected corruption,
substitution, reordering, and truncation. Do not claim protection against a
compromised current-user process, hostile kernel, process dumps, or memory swap.
CPython cannot promise complete key zeroization. Scratch encryption is mandatory;
there is no plaintext scratch option, persisted key, crash resume, or seed flag.
Encryption does not hide scratch sizes or access patterns.
Final FASTQ/key/index/manifest formats remain plaintext or ordinary BGZF:
private final-format partials are deliberate publication staging, not a scratch
encryption bypass for shuffling, verification, or sorting.

## 2. Exact key hierarchy

Implement crypto.py with PyCryptodome HKDF/SHA256, ChaCha20,
ChaCha20_Poly1305, and AES. Obtain one fresh 32-byte root from OS entropy.
Entropy failure is fatal before data admission. Inject deterministic entropy
only through test constructors, never production CLI flags or environment.

All integer encodings below are unsigned; reject values outside their width.
ASCII domain strings have no trailing terminator unless explicitly shown.

    Context(domain, fields) =
        b"RMPY1\x00" || u16BE(len(domain)) || ASCII(domain)
        || u16BE(number_of_fields)
        || concatenate(u32BE(len(field)) || field for each field)
    Derive(key32, domain, fields) =
        HKDF-SHA256(IKM=key32, length=32,
          salt=b"readmixer-python/schema1/hkdf-sha256",
          info=Context(domain, fields))

The four roots are Derive(root, domain, []) for domains root/shuffle,
root/rank, root/tag, and root/scratch. Do not reuse a root across primitives.
The public run identifier is a separate fresh 16-byte OS draw; it is not a key.
Neither the root nor derived keys enter manifests, receipts, logs, filenames,
exceptions, environment, or persisted worker descriptors.

A parent/terminal path P is u8(depth) followed by depth u8 child indices.
The virtual root is b"\x00"; top-level bucket b is b"\x01" || u8(b).
There are at most 32 path components, each in 0..63.

    bucket_key = Derive(shuffle_root, "shuffle/bucket", [P])
    rank_key = Derive(rank_root, "shuffle/rank",
                      [P, u64BE(terminal_pair_count), u8(attempt)])

Plan 04 owns the BLAKE2b bucket transcript. For ranks use ChaCha20 with this
rank_key, a 12-byte all-zero nonce, and stream offset zero. Attempts are 0..7;
each uses its newly derived key. Consume exactly 16*n stream bytes: the first
8*n are the contiguous high u64LE row in canonical pair order; the next 8*n
are the low u64LE row. Sort high then low and reject the entire terminal's
rank draw on any duplicate 128-bit rank. Exhaustion fails; no pair is dropped.

The tag primitive is AES-256 using tag_root to encrypt exactly one block:

    b"RMP1" || u32BE(sample_index) || u64BE(original_pair_index)

The 16-byte result is rendered as the lowercase 32-hex tag in Plan 06.
Use the vetted AES implementation directly; no custom Feistel construction.
All pair identities are distinct and capacity-checked before encryption.
Recovery uses the persisted mapping; it does not require this ephemeral key.

## 3. Segment identity, keys, and block wire format

Implement protected segments in storage.py. Each new segment receives a unique
16-byte u128BE segment_id and fresh 32-byte generation salt from OS entropy.
The coordinator allocates increasing IDs or disjoint worker ID ranges; never
reissue an ID, including after abandonment. Exhaustion fails. Every retry takes
a new ID and salt, so repeated salt draws do not repeat its key context.
Derive their key without a resident secret entry for every historical segment:

    segment_key = Derive(scratch_root, "scratch/segment",
        [run_id, segment_id, generation_salt, u8(purpose), logical_identity])

Purpose values: 1 pair frames, 2 locators, 3 sort runs, 4 registry pages,
5 verifier observations. logical_identity is at most 4,096 bytes and is a
Context encoding owned by the scheduler: segment/pairs uses [P,u32BE(producer),
u64BE(segment_ordinal)]; segment/sort uses [ASCII(sort_purpose),u32BE(level),
u64BE(run_ordinal)]; segment/registry uses [ledger_id[16],u64BE(page),
u64BE(generation)]; segment/observations uses [ASCII(stage),u64BE(ordinal)].
Locators use segment/observations. Reject unknown purposes and malformed fields.

The public fixed 76-byte header H is:

    b"RMPYTMP1" || run_id[16] || segment_id[16] || generation_salt[32]
    || purpose:u8 || reserved_zero[3]

It contains no sample names or original identifiers. Bind H and expected logical
identity through the derived key and every block's authenticated associated data.
Check run/segment identity against the caller's SegmentRef, not just disk bytes.
Store private logical identities and expected seals in admitted registry pages.

Keep one bounded trusted LedgerRoot per logical ledger in RAM: ledger_id,
last SegmentRef, page count, record count, decoded-byte total, and aggregate
SHA-256. Allocate ledger_id from the never-reissued 128-bit identity namespace.
Each immutable page is bound to ledger_id, ordinal, and generation; its
authenticated payload contains the preceding expected SegmentRef or null and
the preceding aggregate digest. Define D0=SHA256(b"RMPY1-ledger-empty") and
Dnext=SHA256(b"RMPY1-ledger-page" || Dprev || u64BE(page_ordinal) ||
page_seal_digest). This permits O(1)-cursor backward digest verification.
Update the root only after a new page seals successfully. The first release
appends pages without in-place rewrite; unused pending pages remain owned by
their writer until root handoff. Page counts, backward ordinals, totals, and
aggregate digest are validated during traversal. A ledger never stores its
own root inside itself or requires a recursively growing trusted-seal registry.

Each block has a 20-byte header B:

    kind:u8 || codec:u8 || reserved_zero[2] || sequence:u64BE
    || decoded_length:u32BE || ciphertext_length:u32BE

Follow B with ciphertext and a 16-byte Poly1305 tag. Use ChaCha20-Poly1305,
nonce b"\x00"*4 || u64BE(sequence), and AAD H || B. Sequence starts at zero
and increases once per data or final block. Reject wrap, gaps, duplicates,
unknown flags, unexpected identifiers, oversized lengths, and trailing bytes.

Data kind is 0; decoded and stored payloads are at most 1 MiB. Codec 0 is raw.
Codec 1 is a complete zlib stream compressed at level 1; writers select it
only when strictly smaller than the raw block. Authenticate before bounded
decompression; require exact decoded length, eof, and no unused/trailing stream.
Charge compression candidates, native state, ciphertext, and plaintext overlap.

Logical PYPAIR1 frames may span these authenticated blocks, never segments.
append_pair is logical atomic admission; readers yield a pair only after all
its bytes and lengths validate. Charge its bounded carry buffer separately.
There is no oversized-block exception for a large frame.

The final kind-1 block uses codec 0 and exactly 56 decoded bytes:

    u64BE(data_block_count) || u64BE(logical_item_count)
    || u64BE(total_decoded_data_bytes) || SHA256(concatenated_data_plaintext)[32]

Its sequence equals data_block_count. Even an empty segment requires a seal.
Compare the seal with the expected SegmentRef and observed data. A consumer
cannot report success from an incompletely consumed stream; context exit must
finish validation or fail/cancel. No final publication occurs before all source
seals validate. On corruption, cancel any descendants derived from that stream.

## 4. Storage interface and private paths

SegmentStore.new_segment(purpose, logical_identity, lease) returns a writer;
append/append_pair consume its lease, seal returns SegmentRef, open_verified
provides a context-managed authenticated stream, and drop releases ownership
only after successful deletion. The logical contract is identical in RAM.
Auto/external disk blocks always follow the format above, including sorter,
verification, and registry data. Bounded registries replace unbounded key maps.

Create tool-owned directories with mode 0700 and files with 0600 before writing
sensitive bytes. Use exclusive creation, no-follow opens, descriptor-relative
operations, and identity checks; reject existing paths instead of truncating.
Shared system temporary parents are allowed only through exclusive private
children. Never pass a user-derived tree to a recursive shell deletion command.

Before creating mix outputs or temporary children, apply the input-root
exclusions in [Plan 02](02-paired-input-and-lossless-parsing.md). Neither the
output nor configured temporary directory may equal or descend from that root.
An allowed ancestor temporary parent still requires checking that its actual
private child lies outside the input tree before creation.

The final destination must be current-user-owned and not group/other writable;
0755 is acceptable. Otherwise fail with a private-directory remediation hint.
Anchor it with a checked directory descriptor. Do not chmod caller-owned
directories. Create a private staging child on that same filesystem.

## 5. Publication transaction

Implement transaction.py: OutputTransaction.begin(destination, completion_kind),
create_partial(relative_basename), register_closed(artifact, length, digest),
open_owned_partial(artifact), publish_all(completion_bytes), and abort(primary).
open_owned_partial returns a read descriptor only after checking expected inode,
regular-file type, ownership, and private mode; it supports index construction.
completion_kind is manifest
for mix or all-selected recovery receipt for recover; verify never publishes.

Track relative paths and expected device/inode identities in an admitted
ownership ledger. Auto/external modes use authenticated encrypted paging as
specified by [Plan 12](12-paged-output-ownership-and-publication.md); strict
memory retains admitted pages or fails without algorithmic spill. Validate
generated basenames and reject separators, traversal, duplicates, or count/size
limit violations. Dev4 implements these ownership pages locally; the preserved
dev2/dev3 evidence describes the earlier capped RAM inventory. Platform/scale
qualification remains open.
create_partial uses exclusive mode-0600 creation; verify permissions before
writing. Final-format partials exist only for intended final artifacts.
The ownership ledger records paths, expected inodes, and link/removal intents
inside encrypted pages. A bounded RAM cursor walks its authenticated chain
backward for cleanup, processing page records before dropping that page.
Keep a bounded pending-file/intent record across each create or link until its
ledger handoff completes. Missing owned names are harmless; mismatched inodes
are never removed. The ledger pages themselves are owned by their trusted root,
not recursively entered into the same ledger. No plaintext path ledger is used.
Committed cleanup visits scratch/staging ownership only; it never removes the
final-artifact links represented in that same ledger.

Close data-processing readers, writers, sort ledgers and worker activity before
publication. Keep ownership journals, their trusted roots, cleanup reservations,
and derived keys alive through publication and rollback/committed cleanup. Use
one operation-wide segment store and never-reissued identity namespace; a second
store with the same operation keys must not restart ID allocation. Pending RAM
ownership records are cleanup obligations even before their page is sealed.
Plan 12 defines their explicit handoff and traversal rather than discarding them
with generic ledger teardown. This lifetime distinction does not permit a data
source to remain unvalidated when the completion marker is committed.

1. Finish every stream, enforce physical caps, validate counts/digests, flush,
   fsync, and close writers. Register the verified inode and size.
2. Hard-link each private partial to its intended final basename using anchored
   directory descriptors. Link creation must fail if the destination exists.
   Never fall back to overwriting rename, copy, or check-then-replace.
3. fsync the destination directory after artifact links; remove owned staging
   links only after their final links are established. Sync changed directories.
4. Create/validate/fsync the bounded completion document as a private partial,
   then publish its hard link last and fsync the destination directory.
5. Mark committed only after that final sync. Remove remaining owned staging
   state. A cleanup-only error after durable commit retains valid outputs and
   returns a committed report with cleanup_complete=false and a bounded warning;
   CLI exit remains 0. Never roll back a durable commit for cleanup-only failure.

A failure before durable completion removes the owned completion link first,
then owned artifact links/partials. Before every unlink, verify expected inode
and device under the anchored parent; never remove a replacement or unrelated
file. Unsupported hard links or directory fsync fail explicitly without an
unsafe fallback. Preserve the primary error and attach cleanup failures.
If the completion link exists but its parent sync fails, durability is
indeterminate: report failure and attempt ownership-aware rollback, but do not
guarantee marker absence if rollback also fails or the process crashes.

This is a physical lifecycle with a last completion marker, not globally atomic
visibility of many files. An interrupted run can leave an incomplete prefix;
without a valid manifest/receipt it is not a completed dataset. A SIGKILL or
power loss may leave ciphertext scratch and private final-format partials.
No automatic resume is supported, and deletion is not secure media erasure.

## 6. Cancellation, tests, and completion

On first failure: stop admission, signal cancellation, stop task submission,
drain/close bounded IPC safely, join workers, terminate unresponsive workers
after the documented grace period, then clean owned scratch and publication
paths. Do not unlink while a worker can still write or publish. Keys remain
available until protected cleanup bookkeeping finishes, then release references.
Best-effort buffer clearing cannot be described as complete Python zeroization.
An interrupt observed after durable commit retains the valid outputs and may
report the already committed marker with the normal cancellation exit status.

- P05-T01: independent HKDF/HMAC transcript vectors, domain/path/length
  separation, rank nonce/layout, AES identity uniqueness, and entropy failure.
- P05-T02: every header/ciphertext/tag/footer corruption, deletion, reordering,
  substitution, trailing bytes, malformed zlib, and oversized length fails.
- P05-T03: empty and cross-block-frame segments, counter/attempt exhaustion,
  retry generations, and no key/nonce reuse under fault injection.
- P05-T04: mode 0700/0600 under permissive umask; hostile symlinks, replaced
  inodes, writable destinations, preexisting names, and cross-filesystem errors.
- P05-T05: boundary failures preserve foreign files and primary errors; before
  marker publication no completion exists, post-link sync failure reports
  indeterminate durability, and durable-commit cleanup failure retains valid
  output with cleanup_complete=false. Failed rollback cannot guarantee absence.
- P05-T06: worker failure, SIGINT/SIGTERM, forced termination, and SIGKILL
  residue follow the documented lifecycle; no secret enters persisted diagnostics.

- [ ] The exact crypto transcript and independent vectors are frozen.
- [ ] All algorithmic disk scratch uses authenticated encrypted segments.
- [ ] Transaction and cancellation fault matrices pass on Linux and macOS.
- [ ] Private final staging, crash residue, and non-atomic visibility are documented.
