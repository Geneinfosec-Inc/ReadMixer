# Post-dev5 worker acceptance handoff

Historical stopping point, superseded by the dev6 section in
[IMPLEMENTATION.md](IMPLEMENTATION.md). Startup fallback and encrypted handoff
mechanisms below are now locally implemented; the recorded focused results here
remain attached to this earlier test-only increment.

This is a small regression-only increment after `2.0.0.dev5`, deliberately bounded
to preserve the user's remaining usage. Runtime/package version and production
behavior are unchanged. No additional complete plan is claimed.

## Work completed

Plan 13 now has four additional local cases:

- `tests/test_stage_launch_concurrency.py` (`P13-T01`, two cases): controlled
  concurrent calls to `_start` contend on the real launch lock. The first or
  second fake process start fails. Temporary native limits remain active inside
  each start; present, empty and absent parent values restore before lock
  handoff and after failure. Events and bounded joins detect leaked test threads.
  This is launch-boundary coverage, not two simultaneous real interpreter starts.
- `tests/test_stage_backpressure.py` (`P13-T06`, two cases): two real spawned
  workers run production memory transfer/sort/ACK code, with a test-only gate
  delaying worker zero. Tasks 2 and 3 complete before task 0. Their results keep
  admitted ownership, the four-job window rejects another submission without
  consuming its sender lease, and normal drain returns tasks 0–3 in order.
  The cancellation case keeps the gate closed while cooperative STOP reaps both
  workers. All input/result leases, RAM, metadata and descriptors are released.

These tests use synthetic bytes. No supplied input files were accessed. Production
mix/verify/recover still use coordinator data processing and compression threads.

## Validation and evidence boundary

Executed locally:

```bash
python -m pytest -q tests/test_stage_launch_concurrency.py
# 2 passed
python -m pytest -q tests/test_stage_backpressure.py tests/test_stage_scheduler.py tests/test_stage_review.py tests/test_worker_state.py tests/test_stage_admission.py tests/test_worker_protocol.py tests/test_execution.py --junitxml=/tmp/readmixer-post-dev5-worker.xml
# 259 passed
python -m pytest --collect-only -q
# 1,586 collected; collection is not execution
```

The four new cases passed within those **261 focused passes**. Audit mappings now
use their actual collected nodes. The full suite, installed-wheel suite, package
builds, supplied-data prefix and benchmarks were **not rerun** for this increment.

[spawn-evidence.json](spawn-evidence.json) remains unchanged and records the exact
historical dev5 candidate: 1,582 source and 1,582 installed-wheel passes. Its source
manifest predates these added tests/documentation and must not be presented as a
fingerprint or complete-suite result for the updated checkout. Earlier evidence
and archived candidates are also preserved. No new candidate was built.

## Exact stopping point and next work

The private worker runtime remains in `stage_scheduler.py`, `stage_admission.py`,
`worker_bootstrap.py`, `worker_protocol.py` and `worker_transport.py`. This turn
changed tests, audit mappings and documentation only. No partially written
runtime feature or migration needs completing before resuming.

A bounded next implementation task is Plan 13's startup admission fallback:
`StageScheduler.__init__` currently aborts the whole scheduler when READY baseline
growth cannot fit. Add reaping and reconstruction at lower concurrency before
accepting jobs, preserving global ownership, native environment restoration and
the fixed compression partition. Add exact admission, cancellation and cleanup
retry tests before connecting production stages.

Remaining plan status:

- **11/12:** locally implemented; downstream use and platform/scale qualification
  remain obligations of later plans.
- **13 — partial:** startup fallback above; derived-key segment capabilities,
  protected task-ownership pages, disk credits and reconciliation, authenticated
  external handoffs, `MemoryRef`/`TransferReceipt`, worker subleases and encrypted
  auto spill remain missing. Concurrent real-spawn acceptance, external faults
  and production signal/publication cases remain open. The new private memory
  backpressure cases do not qualify production workload behavior.
- **14 — unimplemented:** production LPT parser scheduling, direct scatter,
  canonical parallel refinement/terminal delivery, bounded tag/render batching,
  and verification/recovery sort integration.
- **15 — partial:** the serial six-artifact oracle and earlier rollover/mutation/
  union-I/O cases exist; broader metadata/physical boundaries, corruption and
  checkpoint-proven workflow/process/signal failure scenarios remain.
- **16 — partial tooling only:** measured resource envelopes, greater-than-four-
  times-budget workflows, representative repeated benchmarks, and the complete
  30,042,389-pair supplied-data mix/verify/both-recovery run remain outstanding.
- **17 — open:** immutable candidate preparation, isolated dependency/platform
  lanes and evidence reconciliation. **Plan 10 remains the sole release gate.**

See [Plan 13](13-spawn-workers-and-bounded-storage-handoffs.md) for the full
contract and [implementation inventory](IMPLEMENTATION.md) for historical scope.
