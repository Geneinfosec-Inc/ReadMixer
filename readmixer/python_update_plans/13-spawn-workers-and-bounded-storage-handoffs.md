# Plan 13 — Spawn workers and bounded storage handoffs

Status: **Worker infrastructure implemented locally in `2.0.0.dev6`, integrated
into production under Plan 14 in `2.0.0.dev7`; full plan
closure remains open.** The scheduler now supports admitted memory references,
encrypted segment capabilities, coordinator disk credits, authenticated task
ownership and lower-concurrency startup retry. Production stage integration is
owned by Plan 14; resource/platform qualification is owned by Plans 16/17.
Those dependencies and unexecuted acceptance lanes must not be marked complete.
See §10 for current behavior and
[worker-handoff-evidence.json](worker-handoff-evidence.json) for this candidate.
Historical [spawn-evidence.json](spawn-evidence.json) describes dev5 only.

Prerequisites: [11 — Shared metadata budget](11-shared-metadata-budget-and-lifetimes.md)
and [12 — Paged output ownership](12-paged-output-ownership-and-publication.md).
This plan implements the process foundations of [09](09-parallel-execution-and-performance.md),
subject to the admission rules in [03](03-resource-admission-and-storage.md)
and the security/lifecycle contracts in [05](05-encrypted-scratch-and-safe-publication.md).
[14](14-deterministic-parallel-pipeline.md) integrates production pipeline jobs;
[15](15-workflow-boundaries-and-failure-acceptance.md) completes failure acceptance.

## 1. Starting point and compatibility

[execution.py](../../src/readmixer_py/execution.py) already resolves logical CPU,
affinity and cgroup quota limits and provides one bounded compression service.
Its cancellation state preserves a safe first error. Production parsing,
shuffle/terminal sorting, verification, recovery, and rendering still run on the
coordinator. The private [stage_scheduler.py](../../src/readmixer_py/stage_scheduler.py)
now provides `StageScheduler` for one closed `SORT_IDENTITY` stage, acknowledged
bounded byte transfer, and worker shutdown/reaping. It does not receive production
pipeline jobs or final-publication authority. Section 10 records this increment's
interfaces, test anchors, and explicit limits.

[storage.py](../../src/readmixer_py/storage.py) owns a process-local segment
registry and allocation counter. Its `detach` hands ownership to an authenticated
ledger in the same operation; it does not authorize another process to create
or adopt references. [resources.py](../../src/readmixer_py/resources.py) retains
the public workflows' coordinator/headroom checks. The private
[stage_admission.py](../../src/readmixer_py/stage_admission.py), `StageAdmission`,
adds shared-budget reservations and exact-PID aggregate observations for spawned
workers. Neither path bypasses admission or changes the resolved topology.

Keep pool schema 1, shuffle algorithm 1, execution profile 1, public configuration,
CLI spelling, exit statuses, and the no-resume policy unchanged. All new message,
grant, and scheduler types are private; they have no persistence or public seed
format. Library calls do not install process-global signal handlers.

## 2. Coordinator authority and fixed execution partition

Private `StageScheduler`, `StageAdmission`, `WorkerJob`, `WorkerResult`, and
`WorkerFailure` now exist for the identity-sort subset. The scheduler uses the
existing `CancellationState` to retain the first cause and bounded secondary
notes. Extend these foundations to the remaining process-aware pipeline work.
The coordinator alone owns the transaction, final paths, completion decision,
global RAM/descriptors, disk reservations, keys, and source/segment registries.
Workers cannot publish a manifest/receipt or report whole-operation success.

Resolve the existing effective CPU limit C once. Reserve one coordinator slot.
Inspection remains serial. For C = 1, every operation executes synchronously
with zero process and compression slots. For mix/recover with C > 1, the initial
process-slot target is `max(1, C - 2)`; lower it to P for complete stage working
sets, sample/task availability and descriptors. Allocate S compression slots
from `C - 1 - P`, subject to the existing service's RAM/thread limits. Verification
uses S = 0 and an initial process-slot target of C - 1, lowered by the same
admission checks. Once chosen, keep partitions fixed for the invocation; do not
lend unused compression slots to processes or vice versa in this profile.

Within a partition, a stage may use fewer workers. Reserve child baselines while
children remain alive, including idle children. Report `workers_effective` as
one plus the maximum admitted live data-worker count. Where existing mix/recover
reports or manifest execution metadata expose the fields, report
`compression_slots` as S and `cpu_budget` as `workers_effective + S`, preserving
the profile-1 sum invariant. Verification keeps its existing `workers_effective`
and `resources` fields; do not add unversioned execution fields to its report.
Record bounded reduction reasons through safe `resources.detection_sources`
codes or explicitly internal admission telemetry. Actual admission is not a
promise of stage saturation. Keep B, T, bucket topology and terminal limits fixed.

One coordinator-owned `CompressionService` serves all final streams. Children
perform no background compression and create no thread pools of their own.
Worker-side scratch codecs are synchronous work inside their process slot.

## 3. Spawn bootstrap and aggregate admission

Use `multiprocessing.get_context("spawn")` explicitly on Linux and macOS.
Create a lightweight bootstrap module that imports only stdlib and bounded
configuration/errors before installing native limits. Do not inherit open input
or output streams, ciphers, NumPy arrays, or operation keys through fork.

Spawn re-imports the caller's `__main__` before invoking the target, so a target
initializer alone cannot establish pre-import native limits. Serialize this
package's launches under a process-wide launch lock; temporarily set inherited
`OMP_NUM_THREADS`, `OPENBLAS_NUM_THREADS`, `MKL_NUM_THREADS`,
`VECLIB_MAXIMUM_THREADS`, and `NUMEXPR_NUM_THREADS` to `1` around `Process.start`.
Restore every prior parent environment value in `finally`, including failed
starts. Never place keys or serialized jobs in environment variables or argv.
Document the normal guarded `if __name__ == "__main__"` library-launch pattern.

This launch lock coordinates ReadMixer launches, not unrelated application
environment mutation. Test concurrent ReadMixer calls and parent restoration;
do not promise to isolate arbitrary concurrent caller changes to `os.environ`.
Observe native threads on each supported dependency build under Plan 16;
environment settings alone are not evidence of compliance.

Use an initial versioned 64 MiB startup/baseline reservation per child, explicitly
unqualified until measured. Start children one at a time. A READY result reports
safe numeric RSS/descriptor/thread observations after required imports. Increase
the baseline reservation before admitting jobs if observed startup exceeds it;
if this cannot be admitted, stop/reap that child and lower concurrency. Do not
charge the observed baseline twice inside a job's workspace reservation.

Dev6 retains the version-1 baseline and sequential READY checks. A valid READY
RAM/descriptor shortage reaps the whole attempt before rebuilding at lower P,
retaining observed high-water coefficients and allowing serial fallback. Protocol,
thread-count, cancellation, failed-start and cleanup errors are fatal, not retry
triggers. The final partition is fixed before jobs or compression are admitted.
Baseline, workspace, metadata and channel coefficients remain unqualified.

On POSIX, spawn may start an interpreter-owned resource-tracker helper. Dev5
reserves an additional 64 MiB plus 4 KiB metadata and descriptor allowance before
choosing P, then registers that helper's exact PID/birth identity for observation.
It is separate from the at-most-64 data workers and their CPU slots. The helper
can persist after this invocation: ReadMixer releases its per-invocation charge
and registration after reaping data workers, but does not terminate or claim to
reap the interpreter-owned helper.

Job admission includes child baseline, parser/carry or terminal workspace,
storage buffers, result retention, metadata, channels, and all copy overlap.
The child receives a bounded grant with local subleases; it cannot increase its
grant autonomously. Additional retained segment bytes require a coordinator
grant before allocation. Grants are debited globally before being announced.

Add aggregate resource observations for the coordinator and its registered
children. Recheck at stage boundaries, admission/transfer boundaries, and bounded
work checkpoints. Treat measured RSS above accounted allocation as additional
pressure; do not add the entire RSS value to already charged buffers. Preserve
Plan 03's conservative shared-page double counting and fixed resolved topology.
Unexpected child disappearance is a worker failure, not a zero-RSS observation.

## 4. Bounded protocol and protected capabilities

Dev6 extends the closed codec to encrypted segment capabilities, bounded
disk-credit requests/commit acknowledgments and external results. Task/grant
freshness, offsets, observed growth, seals and ownership are checked before ACK.

Use a closed, versioned binary control codec with an explicit message kind and
length; cap each encoded control envelope at 64 KiB before allocation/decoding.
Reject unknown kinds/versions, duplicate identities, invalid widths, excess
lengths, trailing bytes, and messages invalid for the current task state.
No pickle-based application messages, arbitrary callables, exception objects,
full catalogs, arenas, or lists of records cross the channel.

`WorkerJob` binds the operation ID, stage enum, canonical numeric task ID,
grant/generation ID, bounded scalar policy, input references, and allowed key
capabilities. Stream variable reference/assignment collections in bounded
messages or authenticated ledger pages. `WorkerResult` binds the same IDs,
status, checked counts/byte totals/digests, and a bounded result reference.
Do not include original paths, headers, sequences, qualities, or keys in a
failure record or printable representation of a job/result.

Add `SegmentReadGrant`, `SegmentWriteGrant`, `MemoryRef`, and `TransferReceipt`.
A segment grant binds purpose, logical identity, run/segment ID, generation salt,
expected directory identity, file identity when present, permitted access,
and admitted size/count bounds. It carries only the exact derived segment key.
Readers validate the existing authenticated envelope and full seal. Writers
receive fresh coordinator-allocated IDs/salts; no worker allocates IDs from an
independent copy of `_next_id` or resets an existing nonce sequence.

All operation roots and the tag key remain with the coordinator. Scatter jobs
receive only the applicable derived bucket key; refinement jobs receive their
parent-path bucket key; terminal jobs receive the eight derived attempt keys
bound to their exact path and pair count. Ledger-page readers obtain only keys
for authorized pages. Key capabilities have redacted repr and no serialization
outside the bounded in-memory channel. Python zeroization is not promised.

Register pending write ownership before dispatch. The coordinator creates the
private scratch location and anchors its directory identity. For external output
it creates the exclusive mode-0600 file, records the inode, closes its creation
descriptor, and grants the worker a no-follow reopen of that exact owned file.
The worker checks regular-file type, owner/mode, inode and directory before use.
No raw inherited descriptor is an authority. Replaced or foreign files are
preserved and fail closed. Track pending worker scratch in a separate bounded
`SegmentStore` task-ownership ledger using Plan 12's authenticated pending/ACK
handoff pattern. This ledger shares the operation's unique ID allocator and
`DiskBudget`, but not the transaction's final-artifact inventory, root, or count
limit. Scratch files never consume the 100,000-final-artifact allowance; the
`OutputOwnershipJournal` remains exclusive to transaction-owned final files.
Charge task-ledger roots/pages to Plan 11 and apply existing segment/root bounds.

The coordinator grants disk-growth credits against its one filesystem-aware
`DiskBudget` before each bounded write increment. The worker reports actual or
partial committed growth; unused credits are released only after reconciliation
or worker shutdown. Independent child disk budgets must not reserve the same
free space. ENOSPC and broken result channels retain the pending ownership entry.

Add explicit store export/import acknowledgment; do not loosen `_entry` checks.
Authenticate each returned external segment to EOF through an admitted bounded
reader and reconcile its expected seal/counts before accepting the reference.
Persist its ownership in the protected ledger before ACK. Source ownership stays
pinned until ACK; after ACK exactly one owner may retire it. Later consumption
still validates the required frame/ordering semantics; a claimed seal is never
a substitute for authenticated handoff.

## 5. Strict-memory transfer and ordered completion

The current private `submit(payload, lease)` borrows one caller-owned source
lease exclusively for immutable bytes. A second live job cannot borrow that
lease, and the caller must retain it until the receiver validates the input and
its RESULT acknowledges acceptance, or coordinated shutdown reaps the receiver.
The scheduler then releases the borrowed source lease. This legacy private helper remains supported. New `submit_memory(MemoryRef)`
uses an exclusive transferred lease and an exact `TransferReceipt`; rejected
admission before binding leaves the reference caller-owned.

`take_next()` returns results in task order and splits an independent
`StageResult` lease for the caller. Returned bytes and their charge survive
scheduler close; the caller must close the result. Unreturned results remain in
the admitted window, and closing with undrained jobs is a failure followed by
coordinated cleanup. Retaining caller results can prevent later job admission.

`MemoryRef` identifies owner, lease, generation, and exact byte length; it is not
a pathname. Transfer with raw byte messages of at most 1 MiB, validated against
the admitted remaining length. Reserve sender retention, receiver destination,
sender/receiver chunk copies, framing and observed OS transport buffers before
starting. Use bounded raw-byte channels, never an ordinary multiprocessing queue
that can invisibly buffer payloads. Permit one unacknowledged data chunk per
direction; verify final length/digest before ownership acknowledgment.

The sender keeps its lease until the final ACK or coordinated cancellation.
Strict memory lowers concurrency or fails if the complete overlap cannot fit;
it never spills or creates plaintext/shared-memory staging. Auto may seal and
encrypt an eligible result before choosing an external-reference transfer.
No shared-memory adapter is introduced by this plan.

Keep at most two submitted jobs per admitted worker, additionally bounded by
reserved bytes. Every running or completed job owns its grant. Store completed
out-of-order references in a charged bounded window, never a whole-run dictionary.
When the earliest missing task blocks drain, stop further submission at that
window's limits. Continue servicing control, grant, failure and cancellation
messages while a data producer is backpressured; blocking channel sends must not
prevent the coordinator from reaching its shutdown deadlines.

## 6. First failure, shutdown, and commit ordering

The first coordinator-observed causal failure wins, with stage/task ID and safe
bounded numeric context. Later failures and cleanup errors become bounded notes.
Unexpected exit, broken pipe, malformed result, grant violation, or failed seal
cancels the operation. Preserve explicit coordinator cancellation/interrupt type
and signal status. Never print a child traceback containing serialized inputs.

Children check cancellation between bounded input blocks, frame batches, native
sort stages, transfers, and scratch writes. Native work on one admitted block
may finish before checking. Do not impose automatic per-task execution timeouts
or retry failed tasks; slow valid work is not itself failure.

On error or cancellation: stop admissions/submissions; signal cooperative stop;
service/close channels without waiting indefinitely for a producer; join for a
global 10-second grace period; terminate survivors; wait at most another five
seconds; kill remaining children and reap them. Children ignore incidental SIGINT
and observe coordinator cancellation; SIGTERM remains available for termination.
Use monotonic deadlines shared across workers, not 15 seconds per child.

The private scheduler implements this cooperative/terminate/kill sequence and
bounds final kill/reap observation with another shared five-second deadline.
Unknown liveness, failed reaping, or incomplete descriptor cleanup retains its
worker handles, sender ownership and reservations for cleanup retry. It never
releases live-worker ownership to report a successful shutdown. This is not yet
an end-to-end publication or CLI signal acceptance result.

After workers and all writer callbacks have stopped, clean owned scratch and
partials. On success, drain and validate all results, join/reap children, close
source handles and native services, then publish the completion marker last.
An inability to stop/reap a child is failure, never permission to delete its live
files or commit. Preserve Plan 05's SIGKILL residue and post-commit error rules.

## 7. Acceptance and completion

Retain [test_execution.py](../../tests/test_execution.py)'s compression and
first-error tests; they are regression baselines, not process-lifecycle evidence.
Add real subprocess tests with bounded watchdogs and independent child/writer
observations. Register exact collected nodes only after implementing them.
The matrix below remains the full plan's acceptance target; source-focused Linux
coverage of a private subset does not close a whole scenario or the platform gate.

- P13-T01: spawn CLI and guarded library on Linux/macOS; pre-import native limits,
  parent environment restoration, failed start and C=1 without background work.
  Maps to P09-T01, P03-T03 and AUD-22.
- P13-T02: fixed CPU partition, child baseline growth, descriptor reductions,
  aggregate pressure and idle-child charges preserve B/T and profile-1 sums.
  Maps to P03-T02/T03/T07, P09-T01 and AUD-10/AUD-22.
- P13-T03: control lengths/versions, malformed results, incorrect grant IDs,
  forbidden key scope, replaced scratch paths, fresh generations and seal
  substitution fail safely. Maps to P05-T01/T02/T03/T04 and P09-T04/T05.
- P13-T04: disk-credit races, partial writes, ACK loss/repetition, crashes at each
  ownership handoff, and cleanup preserve foreign files and the first cause.
  Maps to P03-T03/T06, P05-T03/T05/T06 and AUD-12/AUD-14/AUD-17.
- P13-T05: strict-memory transfers at exact/overlap limits, blocked receivers,
  failed ACKs, and forced auto spill retain correct leases with no plaintext disk
  escape. Maps to P03-T03/T04, P09-T04 and AUD-10/AUD-23.
- P13-T06: delayed earliest result bounds all submitted/completed bytes while
  control and cancellation remain responsive. Maps to P09-T04 and AUD-10/AUD-23.
- P13-T07: child exceptions/exits, broken channels, real SIGINT/SIGTERM, a child
  ignoring cooperative stop, terminate/kill escalation, and causal cleanup
  failures leave no live writer or false marker. Maps to P05-T06, P09-T05 and
  AUD-12/AUD-14/AUD-22; Plan 15 owns the end-to-end publication matrix.

- [x] The local identity-sort runtime gives each job, transfer, disk increment
  and result an admitted owner. Production stage expansion belongs to Plan 14.
- [x] Local real-process tests cover bounded shutdown and acknowledgment states;
  supported-platform and production workflow acceptance remain open.
- [x] Public format/CLI contracts remain unchanged in local regression tests.
- [ ] Plan 14 uses this runtime; Plan 16 measures its versioned envelopes.


## 8. Historical dev5 implementation scope

The implemented policy accepts only bounded normalized pair-frame batches and
sorts by `(sample_index, original_pair_index)`, rejecting duplicate identities.
It is not the production shuffle algorithm, external recovery sort, parser job,
or complete verification worker. Batches default to 512 KiB, with a private maximum
of 8 MiB. The initial job envelope is `20 * max_job_bytes + 2 MiB + 16 KiB`, whose
metadata subset is `8 * max_job_bytes + 16 KiB`; the subset covers Python row/key
objects as well as bounded task bookkeeping. Those coefficients are conservative
admission policy, not measured memory qualification.

Implementation anchors:

- [stage_admission.py](../../src/readmixer_py/stage_admission.py), `StageAdmission`:
  fixed CPU/task/RAM/metadata/descriptor partition; idle baseline retention; two
  complete job grants per worker; READY growth; exact PID/birth observations;
  replacement pressure charge for RSS/FD excess over existing reservations.
  Missing/replaced workers or the registered helper fail instead of counting zero.
- [worker_protocol.py](../../src/readmixer_py/worker_protocol.py), `inspect_header`,
  `encode_message`, `decode_message`: versioned closed binary messages, 64 KiB
  control-envelope limit, separate 1 MiB raw-chunk limit, strict numeric widths,
  and payload/key-free printable diagnostics. Application messages use no pickle.
- [worker_transport.py](../../src/readmixer_py/worker_transport.py), `FramedSocket`:
  nonblocking bounded duplex frames; header validation before body allocation;
  one outstanding frame per direction; observed kernel socket-buffer bounds.
- [worker_bootstrap.py](../../src/readmixer_py/worker_bootstrap.py), `worker_main`:
  READY after required imports, checked task/grant/input/output transitions,
  complete digests, chunk/result acknowledgments, fixed safe failure codes,
  and cooperative cancellation during bounded framing/sorting checkpoints.
- [stage_scheduler.py](../../src/readmixer_py/stage_scheduler.py), `_start`,
  `StageScheduler`, `StageResult`: spawn launch lock and inherited native limits;
  restored parent environment; bounded submission/completion ownership; ordered
  drain; first-cause cleanup; shared shutdown deadlines and reaping; independent
  returned-result lifetimes. `CompressionService` respects an existing private
  scheduler's remaining CPU partition, without connecting public workflows.

Existing collected test anchors establish the scope to validate for this candidate;
no execution count or release qualification is asserted here:

- [test_stage_admission.py](../../tests/test_stage_admission.py),
  `test_P13_T02_complete_ram_threshold_exact_and_one_below`, metadata/descriptor
  boundaries, READY growth, pressure replacement, exact worker/helper observations.
- [test_worker_protocol.py](../../tests/test_worker_protocol.py),
  `test_P13_T03_exact_independent_wire_vectors`, malformed headers/widths/enums,
  raw-chunk boundaries, and private printable messages.
- [test_stage_scheduler.py](../../tests/test_stage_scheduler.py),
  `test_P13_T01_guarded_library_launch_in_fresh_interpreter`, real private sort
  jobs and caller result lifetimes, multichunk/empty transfers, C=1, failed-start
  environment restoration, fixed compression share, and unexpected child exit.
- [test_worker_state.py](../../tests/test_worker_state.py),
  `test_P13_T03_T05_forged_worker_state_rejects_privately_and_reaps`: real forged
  peers with unknown task/grant, premature result, replayed/wrong ACK, excess
  result/count/chunk, wrong offset and wrong digest, under bounded watchdogs.
- [test_stage_review.py](../../tests/test_stage_review.py),
  `test_P13_T05_one_sender_lease_cannot_back_two_live_jobs`, interrupted result
  ownership, socket-construction cleanup, secondary shutdown interruption,
  `test_P13_T07_real_uncooperative_child_is_escalated_and_reaped`, and
  `test_P13_T07_unreaped_child_retains_all_admission_until_successful_retry`.
  Escalation tests shorten waits while a separate check preserves production
  grace constants; they do not claim real CLI SIGINT/SIGTERM coverage.

The dev5 missing storage/memory/startup mechanisms listed in earlier revisions
are implemented by dev6 (§10). Plan 14 production job/key-policy integration,
workflow publication acceptance and Plans 16/17 measured platform qualification
remain open. In particular, scatter/refinement/terminal key capabilities must be
issued for those exact policies when their production stages are introduced;
the current closed stage is identity sorting and receives only exact segment keys.


## 9. Post-dev5 bounded regression follow-up

Four additional cases cover launch-lock contention/restoration with controlled
fake starts and two real workers with deterministically delayed earliest output.
The latter verifies retained results, full-window refusal, canonical draining
and cooperative STOP while blocked. See
[test_stage_launch_concurrency.py](../../tests/test_stage_launch_concurrency.py),
[test_stage_backpressure.py](../../tests/test_stage_backpressure.py) and the
[validation and exact handoff](POST_DEV5_WORKER_ACCEPTANCE.md). Runtime code is
unchanged. This is additional P13-T01/T06 coverage, not completion of this plan
or new installed-candidate evidence.


## 10. Dev6 protected worker infrastructure

[worker_storage.py](../../src/readmixer_py/worker_storage.py) implements bounded,
redacted `SegmentReadGrant`/`SegmentWriteGrant` capabilities. Each binds the run,
segment ID, salt, purpose, logical context, exact derived segment key, directory
and inode identity, mode, byte/count bounds and write task/grant identity.
Workers reopen only the coordinator's exclusive mode-0600 file in its private
anchored directory. They cannot allocate IDs, derive other keys, delete files,
construct an independent disk budget or publish completion markers.

`WorkerStorage` uses the operation store's allocator and shared disk budget, with
a distinct authenticated owned `PagedLedger`. Registered writes and transfer
records page independently of the final-artifact journal/count cap. The active
read/write window is bounded by scheduler capacity. The coordinator reserves each
bounded physical increment before the worker writes it, observes actual growth,
then acknowledges reconciliation. Interrupted partial writes/commits remain owned
until workers are reaped. Returned segments are authenticated to EOF and imported
before result ACK; undelivered outputs remain task-owned. `StageSegmentResult`
transfers a verified reference and caller lease, survives scheduler shutdown and
must close before its store. Retryable cleanup preserves foreign/uncertain residue
and never uses corrupt journal records to guess deletions.

[worker_memory.py](../../src/readmixer_py/worker_memory.py) adds `MemoryRef`,
`TransferReceipt` and `LocalWorkspace`. Receipts bind run, owner, generation,
task, grant, length and digest. References transfer exclusive lease authority,
retain it through ACK/cancellation and reject mismatched receipts. Child-local
subleases include registry/metadata/descriptors under the already-issued global
job charge; they cannot enlarge that grant. The dev6 job envelope is
`20 * max_job_bytes + 16 MiB + 16 KiB`, including protected codec/copy overlap;
metadata remains `8 * max_job_bytes + 16 KiB`. These are unqualified versioned
admission charges, not measured hard RSS guarantees.

`submit_segment` uses admitted raw transfers in memory mode. Auto seals/spills
an eligible input before capability dispatch; external/auto output stays encrypted.
CPU one uses the same fixed identity-sort policy synchronously. Running/completed
jobs remain limited to two per worker plus a byte ceiling; canonical delivery and
caller-result retention remain charged. Aggregate observations also occur at
bounded 100 ms coordinator service checkpoints while waiting for results. An
interrupted result return retains its cleanup owner on the scheduler until a
successful retry; secondary cleanup errors do not replace the causal failure.

Current acceptance anchors:

- [test_stage_startup.py](../../tests/test_stage_startup.py): exact READY growth,
  RAM/descriptor reductions, retained coefficients, serial fallback, cancellation
  and reaping-before-retry (`P13-T01/T02/T07`).
- [test_worker_memory.py](../../tests/test_worker_memory.py) and
  [test_stage_memory_refs.py](../../tests/test_stage_memory_refs.py): exclusive
  receipts, interrupted constructor handoffs, local sublease limits, real spawn
  and strict-memory no-spill ownership (`P13-T04/T05`).
- [test_worker_storage.py](../../tests/test_worker_storage.py): capability bounds,
  key scope, inode/mode/seal changes, paging, interrupted registration/credit/
  transfer, partial writes and conservative cleanup (`P13-T03/T04/T05`).
- [test_stage_external.py](../../tests/test_stage_external.py) and
  [test_stage_external_faults.py](../../tests/test_stage_external_faults.py): real
  encrypted spawn/serial round trips, input preservation, lost ownership returns,
  credit/commit checkpoints, unexpected exit and corrupt/foreign output.
- [test_worker_external_protocol.py](../../tests/test_worker_external_protocol.py):
  independent extended wire vectors and exact/one-over envelopes (`P13-T03`).
- [test_stage_signals.py](../../tests/test_stage_signals.py): guarded fresh-module
  harness, real SIGINT/SIGTERM, concurrent real launches and pressure checkpoints
  (`P13-T01/T02/T07`). This harness uses the private scheduler and existing signal
  handling; it does not claim production parallel CLI execution.

Full plan closure still requires Plan 14 to use this runtime and prove production
byte identity/publication order, Plan 15's end-to-end fault checkpoints, Plan 16
resource/native-build calibration, and Plan 17's installed Linux/macOS matrix.
Local tests and configured CI cannot supply unexecuted platform evidence.

## 11. Dev7 production integration

[Plan 14](14-deterministic-parallel-pipeline.md) now uses this runtime for source
parsing, complete writer generations, terminal ranks and closed production sort
policies. Its exact source/installed results belong to
[parallel-pipeline-evidence.json](parallel-pipeline-evidence.json). Sections above
that describe dev5/dev6 or future production jobs are historical implementation
boundaries. Plan 15 owns remaining workflow fault breadth; Plans 16/17 own native
charge calibration, scale, supported-host and immutable release-candidate gates.
