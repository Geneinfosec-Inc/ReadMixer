# Corrections prepared after dev10 platform execution

These are **preparation patches**, not edits to the immutable dev10 candidate.
The failed dev10 reports remain failed. Original runtime code, archived tests,
artifacts and Plan 16 measurements retain their original identities.

`recovery-lifetime-portability.patch` replaces a hard-coded CPython reference count
with the same counting expression applied to independently owned control objects.
A new negative control proves an added owner is detected and release restores the
baseline. The entire prepared lifetime test file passed 10 tests on CPython 3.11.7
and 10 on CPython 3.14.7, against the separately installed unchanged dev10 runtime.
The evidence is `dist/plan17-dev10/preparation/refcount-fix-evidence.json`.

The second observed failure is
`test_pipeline.py::test_P06_004_oversized_singleton_rolls_back`: its automatic-worker
configuration encountered a 512-MiB admission refusal before the intended
oversized-pair boundary. Preserve the failed report and require a deterministic
fixture to reach that boundary; weakening the expected exception is insufficient.
`singleton-boundary-admission.patch` explicitly selects one worker for that
serial boundary assertion. The original and prepared case each passed eight
isolated attempts on both Python versions (32 controls total). The full-suite
early refusal was **not reproduced** by isolated controls, so the patch does not
exclude another suite interaction. Full new-candidate execution remains required.
Evidence is `dist/plan17-dev10/preparation/singleton-boundary-admission/index.json`.
See the current [execution index](../../../readmixer/python_update_plans/release-evidence.json)
for the corresponding preparation correction and evidence.

For the next candidate:

1. Select a **new preparation tree**, preserving the original candidate bundle and
   the current user's unrelated work. Apply the reviewed patches there. For example,
   `git apply --check tools/release/preparation/recovery-lifetime-portability.patch`
   followed by `git apply tools/release/preparation/recovery-lifetime-portability.patch`.
   Check/apply `tools/release/preparation/singleton-boundary-admission.patch` the
   same way; it changes one configuration field and adds no test nodes.
   Do not patch an archived qualification workspace and relabel its failed run.
2. Give the new candidate its own version and complete selected source manifest,
   including the revised tests and shipped documentation. The lifetime negative
   control adds one test; expected collection becomes 2,704 if these are the only
   test additions. Confirm actual collection, rather than only changing a count.
3. Build/hash its wheel and sdist once in a recorded isolated build environment.
   Record original provenance and adopt those exact archives with `candidate.py`.
   `prepare_candidate.py` now provides a narrowly scoped selected-tree frontend:
   run it with the already locked build venv's `python -I`, specifying `--source`,
   `--environment`, a new `--output`, the verified `--base-candidate`, and the
   actually collected `--expected-tests`. It prepares metadata, freezes every
   selected path/length/hash before the single archive build, records private
   commands/logs and builder fingerprints, verifies original source is unchanged,
   and adopts the resulting exact archives. It rejects caches or excluded files
   instead of silently omitting them. Review Plan 17 §4 explicitly.
4. Prepare a new applicability policy bound to the new candidate ID and its eight
   unchanged Linux-mechanism test hashes. Retain the old policy and update the
   checker's recognized policy binding deliberately; do not merely weaken checks.
5. Repeat affected ordinary lanes and native dependency/resource/calibration checks.
   Review old measurements for explicitly scoped supporting reuse where runtime,
   fixtures, native builds and controls are unchanged. Old reports must never be
   rewritten to claim execution against the replacement archives.
6. Follow [the operator guide](../README.md) to complete macOS host evidence,
   remaining synthetic measurements, documentation/audit reviews and the final
   Plan 10 decision. Publication is a separate action.

The independent preparation passes support the proposed corrections. They are not
a complete source suite, installed suite, platform lane or release qualification.

## Native Mac successor preparation

On 2026-10-19 a new isolated preparation tree derived from the verified dev10
selected source produced `2.0.0.dev11`, candidate
`sha256:197110fc1431984e92565c7b44632565be089f248595b158a8c6327a15d9b19c`.
Both patches apply once, retain the extra-owner negative control and the intended
complete-pair boundary, and collect 2,704 tests on native arm64 CPython 3.11.9 and
3.14.8. Eleven focused corrected tests pass on each. The first frontend attempt
rejected pytest-generated caches before any archive was built; that diagnostic is
retained. The second frontend attempt built each candidate archive once using the
locked Python 3.11 environment and documented builder versions. The complete
selected source, aggregate replay-verified patch, old/new manifests, build inputs,
exact original archives, packet and failed-attempt logs belong to the private Mac
evidence export. Full platform/resource acceptance and Linux execution against
these exact replacement archives remain separate obligations.
