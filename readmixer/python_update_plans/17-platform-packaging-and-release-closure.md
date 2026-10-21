# Plan 17 — Platform packaging and release closure

Status: **17A frozen; core external qualification tooling implemented; 17B remains open**.
The immutable candidate remains `2.0.0.dev10`. The new
[release execution index](release-evidence.json) records actual host attempts and
separate external-tool identities. The [operator guide](../../tools/release/README.md)
contains exact commands, host prerequisites, evidence-return instructions and
final-review requirements. Current documentation/tools are a later snapshot;
they do not change or inherit the frozen archive fingerprints.

The [Plan 16 execution record](resource-qualification-evidence.json) remains
unchanged: 2,703 source plus 2,703 installed tests, all 25 local synthetic cells,
270 supplement observations, six physical-cap workflows and the complete
30,042,389-pair supplied-data task passed on the recorded Linux CPython 3.11.7
build. Plan 17 has additionally executed a fresh isolated Linux 3.11 ordinary
lane. Remaining host attempts and failures are enumerated in the release index.
This plan organizes candidate freezing, platform evidence, and reconciliation. [Plan 10](10-acceptance-documentation-and-release.md) remains
the sole portable release-completion gate. Package publication is a separate
release action; writing or completing these documents does not publish anything.

Parents: [01](01-packaging-interfaces-and-compatibility.md),
[06](06-bundles-keys-indexes-and-manifests.md),
[09](09-parallel-execution-and-performance.md), and Plan 10 §§6–8.
Dependencies are the mechanisms in Plans
[11](11-shared-metadata-budget-and-lifetimes.md),
[12](12-paged-output-ownership-and-publication.md),
[13](13-spawn-workers-and-bounded-storage-handoffs.md),
[14](14-deterministic-parallel-pipeline.md), the acceptance cases in
[15](15-workflow-boundaries-and-failure-acceptance.md), and prepared tooling from
[16A](16-resource-scale-and-performance-qualification.md#1-baseline-and-phase-ordering).

## 1. Historical evidence and missing qualification

[pipeline-evidence.json](pipeline-evidence.json) is the immutable historical dev2
local record: 1,122 source tests and 1,122 installed-wheel tests passed on Linux
x86-64 / CPython 3.11.7. It records wheel/sdist fingerprints, source equivalence,
outside-checkout import checks, dependency-free help/version, installed private
prefix validation, and an installed-package tiny warm-up plus five-run benchmark.
Do not describe those package checks or that benchmark as unperformed.

The local installed environment used `system-site-packages` for its recorded
dependencies. This demonstrates package-location isolation, not a fully isolated,
locked multi-platform dependency environment. The pinned
[foundation lock](../../requirements-foundations.lock) explicitly describes its
historical local environment; it is not a Python 3.14 or macOS release lock.
[Foundation](foundation-evidence.json), [storage](storage-evidence.json), and
[source-snapshot](source-snapshot.json) records retain their own original scopes.

The existing [development CI workflow](../../.github/workflows/python.yml)
continues to rebuild and resolve dependencies for development feedback. It is
explicitly labeled as non-release evidence. The new opt-in
[release workflow](../../.github/workflows/release-qualification.yml) transfers
one hash-verified candidate packet to all four native cells, records each selected
wheel closure before isolated hash-locked installation, and retains bounded
public reports plus optional authenticated encrypted private diagnostics.
Configuration alone is not an executed support matrix. Resource-host work remains
separate; absent private raw evidence blocks final integrity verification.

## 2. Acyclic preparation, freezing and closure

**17A** starts after Plans 11–15 and Plan 16A's candidate-affecting tooling and
profile changes are ready for qualification. It resolves dependencies, freezes
source, builds the candidate, and establishes isolated installs. No measured
Plan 16B result is a prerequisite for this initial freeze.

Plan 16B and platform/acceptance execution then run against those exact archives
and recorded dependency builds. **17B** reconciles their outcomes afterward.
A failed measurement can reveal a code, profile, fixture or lock change. Make
that correction in preparation, issue a new candidate identity, rebuild, and
rerun affected lanes. Do not mutate the artifact associated with an existing
candidate identity or pretend an earlier pass tested the replacement.

This order also applies to documentation distributed in the source archive.
The historical dev2 manifest includes planning/status files; changing those
files means the current documentation tree is a later snapshot. Preserve its
original fingerprints rather than recomputing them under old pass/fail claims.
An unchanged historical archive remains usable historical evidence.

## 3. Resolve the explicit support and dependency matrix

The required matrix is **Linux x86-64 and macOS arm64**, each with **64-bit
CPython 3.11 and 3.14**. Record exact patch releases and actual machine architecture.
Do not infer architecture from `*-latest` runner labels or claim an untested
architecture because a pure-Python wheel can be installed there.

Resolve compatible exact runtime, test, build, and interoperability dependencies
for each cell. Create hash-locked inputs covering transitive packages and the
actual wheels/sdists selected; record platform markers and Python ABI. Include
NumPy, PyCryptodome, psutil, build tooling, pytest and independent HTSlib/pysam
oracles as appropriate. Pinning only top-level lower bounds or saving a freeze
after an uncontrolled install is insufficient to replay an environment.

Use fully isolated virtual environments without `system-site-packages`, cleared
source-tree import overrides, and controlled dependency installation. Record
Python executable/build, zlib build/runtime, NumPy native configuration and
thread controls, cryptographic library version, and oracle executable/library
versions and fingerprints. Resolve ordinary native build prerequisites on each
host explicitly. A failed required cell remains failed/unqualified; do not
silently drop Python 3.14 or substitute an architecture to make the matrix pass.

The foundation lock can remain as historical documentation. New release locks
must be labeled for their tested environments and must not relabel its older
packages as a tested modern matrix. Dependency changes can affect codec output,
workspace charges and admission; rerun the corresponding tests and calibration.

## 4. 17A — Freeze one identifiable candidate

Capture a source manifest of the complete selected candidate, including new
runtime code, tests, scripts, schemas, locks and shipped documentation. Include
tracked and intended untracked files, paths relative to the repository, lengths,
and exact-byte SHA-256 values in a defined canonical encoding. Record base commit
and dirty-tree scope without implying that a commit hash covers untracked work.
Exclude private datasets, outputs, caches, secrets and build products explicitly.

Freeze the source before building the wheel and source distribution once in a
controlled build environment. Record package version, candidate ID, source
manifest hash, build configuration and dependency-lock hashes, archive lengths,
and archive hashes. Verify archive contents contain no supplied input data or
private generated outputs. Evidence that hashes its own archive should remain
outside that archive to avoid recursive fingerprint requirements.

Distribute those exact candidate archives to matrix jobs; verify hashes before
installation and after transfer. The current `py3-none-any` package allows a
single wheel to be tested with platform-specific dependency environments. Native
dependency differences remain separate recorded qualification dimensions.
Do not let each matrix job silently rebuild a different wheel under the same ID.

Build a wheel from the frozen sdist in a separate isolated check outside the
checkout and compare installed package source/content against the candidate.
Record source-content equivalence separately from bit-identical wheel archive
reproducibility; ZIP timestamps/build metadata can affect archive bytes. If
reproducible archives are claimed, enforce and test that stronger build contract.
Retain both outputs and explain any intentional build-only metadata differences.

## 5. Installed-package and ordinary CI lanes

For every matrix cell, install the verified wheel and locked dependencies into
fresh isolated environments. Copy tests/helpers/forwarder to a directory outside
the checkout without copying `src/`; assert imports resolve to the installation.
Test the console script, module invocation and old-path forwarder. Add a separate
dependency-free help/version check proving lazy native imports and no workers.

Exercise typed library errors without `sys.exit`, global logger/start-method
mutation, and CLI configuration/operational/cancellation statuses. Run complete
mix, full verification, originals verification and both recovery modes with
independent normalization/identity expectations. Check report scopes, redaction,
private permissions, raised record bounds, unsupported versions and absence of
public entropy overrides. Relevant current files are
[test_api.py](../../tests/test_api.py),
[test_cli_workflow.py](../../tests/test_cli_workflow.py),
[test_ingress_workflow.py](../../tests/test_ingress_workflow.py), and
[test_large_records.py](../../tests/test_large_records.py).

Run the complete ordinary correctness suite against both source and installed
candidate where specified. Record collected, executed, passed, failed and skipped
counts with individual node outcomes; collection alone does not establish a pass.
Run documentation/link, shell and archive-content checks without importing or
running the predecessor engine. Distinguish historical source links from current
implementation links and proposed tests from existing executed nodes.

Require independent BGZF/.gzi/virtual-offset interoperability in each supported
cell, building on [test_bgzf.py](../../tests/test_bgzf.py). Optional local skips
are useful development behavior but cannot satisfy a required release cell.
Compare exact bundle bytes across workers/storage only within the fixed codec
build/topology context. Across differing native builds require each build's
independent goldens, interoperability and preservation claims, not unsupported
cross-zlib byte equality.

## 6. Resource-host and filesystem lanes

Ordinary CI runs modest fixtures and bounded fault cases. Separate controlled
resource hosts execute Plan 15's real signals/crashes and Plan 16B's memory,
CPU, descriptor, scratch, device and performance matrix on the frozen candidate.
Record where CI lacks privileges or devices; a configured job is not a host run.

On Linux qualify cgroup memory/CPU limits and affinity; on macOS qualify the
portable contract without importing Linux enforcement claims. Exercise spawn,
native-thread configuration, low descriptors, shared/separate output and scratch
filesystems, and slow-storage/backpressure. Record filesystem/mount properties
relevant to no-follow opens, private permissions, hard links and directory sync.
Do not extrapolate one filesystem result to unsupported network filesystems.

Recheck ownership-aware rollback and no-replace publication at real boundaries,
including directory-sync indeterminacy and durable-commit retention. A cleanup
warning after commit must preserve valid data, while an explicit interruption
keeps its cancellation outcome. Record actual process termination/reaping and
residue checks rather than substituting compression-thread tests for workers.
The greater-than-four-times-budget lane must cite actual admitted B and measured
resource observations. An OS kill is a failed run, not successful admission.

## 7. Candidate-bound evidence and privacy

Create a bounded per-candidate evidence record with a machine-readable scenario
matrix. Each row identifies planned scenario, parent plan/AUD IDs, actual test
nodes or host command, candidate/archive/lock identities, platform, result,
reached fault stage, and the scope of assertions. Separate missing code, missing
regressions, unexecuted hosts, skipped dependencies and failed measurements.

Retain raw machine-readable logs, exit statuses, counts and hashes needed to
reproduce summaries. For benchmarks preserve warm-up, individual measurements,
medians/ranges, validation and sampling limitations. Record executable import
location as a checked fact without publishing private absolute paths. Source,
archive, lock and evidence hashes are different identifiers, not interchangeable.

Store immutable candidate records separately and link them from a current status
index. Never replace historical dev2 counts, files or hashes with new results.
If a source or dependency changes, identify a new candidate and state which checks
must be rerun; do not carry forward affected passes solely because names match.
The audit coverage manifest provides traceability, while execution evidence
provides actual outcomes. A collected reference is not release qualification.

Publish artificial fixture definitions, aggregate real-input counts, host facts
and appropriately scoped timings only. Private paths, input fingerprints,
records, restoration mappings and sample identifiers remain in access-controlled
local records. CI and public evidence must not redistribute supplied datasets.
Include evidence integrity checks and explicit omissions rather than attempting
an unbounded dump of per-read results or sensitive process diagnostics.

## 8. 17B — Reconcile release and local-task outcomes

Reconcile every required parent-plan scenario with Plans 11–16 and the executed
matrix. Update public help, README, migration/security/API documentation, format
versions, supported builds and performance statements around the measured final
behavior. Correct stale claims that local dev2 wheel fingerprints/install tests
or its tiny benchmark are absent. Preserve unresolved platform/resource limits.

Report two distinct outcomes: **portable release qualification** under Plan 10,
and **full supplied-data local validation** under Plan 16-T06. The latter is a
required elected local follow-on task, not a public CI prerequisite. Its absence
or incomplete execution must remain visible and cannot be labeled completed by
synthetic tests, ingress-only scans or prefix recovery. Conversely, its success
does not replace the reproducible synthetic/platform release matrix.

Plan 10 may be marked complete only when all of its required implementation,
correctness, interoperability, resource, package and benchmark gates have
executed successfully for the frozen candidate. Missing required cells block
that claim; do not redefine a skip as a pass. Keep the local-task status separate
even when portable release qualification has an independent result. Publishing
a package remains a subsequent action, not an automatic effect of this record.

## 9. Planned scenarios and completion

The following IDs identify obligations. Actual execution status and tool/test anchors
are in [release-evidence.json](release-evidence.json); these IDs are not pytest node names.

- **P17-T01:** immutable source/archive identities and sdist content-equivalence
  checks. Parents `API-01`, Plan 10 §6; `AUD-19/20`.
- **P17-T02:** isolated hash-locked Linux x86-64/macOS arm64 Python 3.11/3.14
  installs and actual architecture/import checks. Parents `API-01/02/04`,
  `P09-T01`, Plan 10 §6; `AUD-12/19/20/22`.
- **P17-T03:** installed complete workflows, wire/interoperability, redaction and
  permissions. Parents `API-03..07`, `P06-003/007/009`, `P08-010`;
  `AUD-03/06/12/13/16/18/19/20/24`.
- **P17-T04:** required resource/signal/filesystem hosts and candidate-bound
  Plan 16B measurements. Parents `P03-T06/T07`, `P05-T04/T06`, `P09-T01/T05/T07`,
  Plan 10 §§4–6; `AUD-10/12/13/14/17/19/22/23`.
- **P17-T05:** immutable, privacy-preserving evidence with honest failed/skipped/
  unexecuted status and exact scenario/AUD traceability. Parent Plan 10 §§6/7;
  `AUD-19/20`, with references to all affected `AUD-01..AUD-24` obligations.
- **P17-T06:** final documentation and separate portable-release/local-task
  reconciliation. Parents `API-01/07`, Plan 10 §8, `P16-T06`;
  `AUD-12/18/19/20/24`.

Complete this plan only when 17A and 17B have executed evidence, every required
portable qualification cell passes, and Plan 10's decision follows those results.
Report the separate full local task accurately; an incomplete local task stays
open even if the portable gate passes. No unrun required lane, private-input
substitution or rebuilt artifact can inherit a pass.

## 10. Executable handoff and remaining closure

External tools now provide safe candidate adoption/transport, per-native-host
closed dependency locks, source/installed ordinary lanes, resource-host launches,
private diagnostic encryption and fail-closed evidence assessment. Their regression
suite is separate from the candidate's frozen 2,703 tests. The external tools passed 274 focused regressions; 150 bounded Linux 3.14
supplement observations passed. The full 3.14 source/installed suites failed
(2 and 3 tests respectively), so prepared corrections/new-candidate work and
remaining supplemental orchestration precede release closure. No runtime feature
or pool schema changed in this work.

[platform-applicability.json](../../tools/release/platform-applicability.json)
was declared before any macOS attempt. It identifies eight exact, source-hashed
Linux-only qualification-launcher nodes. A macOS lane must collect all 2,703 nodes,
report eight `not_applicable` outcomes without pass credit, and pass every remaining
2,695 node in both source and installed suites. Ordinary JUnit retains eight skips;
no other skips/xfails are accepted. The portable nofile/reaping external regressions
and actual macOS low-descriptor/resource profiles are required replacements.
This amendment concerns Linux infrastructure mechanisms, not production behavior,
independent interoperability or a weaker portable release contract.

The [operator guide](../../tools/release/README.md) is the runnable handoff. Return
complete report directories, exact native wheelhouses/locks, harness snapshots,
commands, observations, failures and encrypted/private diagnostics to the shared
workspace. A CI badge or summary screenshot cannot close a gate. The checker
independently verifies ordinary evidence and explicitly labels attributed resource,
calibration and documentation reviews. `ready_for_plan10_review` is not an automatic
release: Plan 10 still requires the substantive final decision, and publication
remains separate.
