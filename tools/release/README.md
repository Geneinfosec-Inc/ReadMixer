# Frozen-candidate release qualification

These external tools implement the remaining orchestration for [Plan 17](../../readmixer/python_update_plans/17-platform-packaging-and-release-closure.md).
They do not change the frozen package or publish it. The historical candidate is
`2.0.0.dev10`, ID
`sha256:426e563211c32b5bea806862d65bd0754419e397fb53ecaaa7239e11a00810ff`.

The exact wheel SHA-256 is
`4dbdab8d01c91488894e30ce0b842d426ee7a5f58a401c4786b22969d94c0691`;
the exact sdist SHA-256 is
`6ff7f6f635b200c12b8a5a271b0cae39b650baad375e38b89050d2a9e54b1043`.
The current documentation/harness checkout is a later snapshot, with separate
hashes. **Do not rebuild these archives from the current dirty checkout and call
the result dev10 qualification.**

Native Mac preparation on 2026-10-19 produced successor `2.0.0.dev11`, ID
`sha256:197110fc1431984e92565c7b44632565be089f248595b158a8c6327a15d9b19c`.
Its wheel hash is `219f4fbacdaf4afc416792c2a349203b1e02ee99087eb73766f47255ae1eb396`
and sdist hash is `7e4f94656772ee2fc0e94c023ea5cbf25650665f8918c34a3a8090af8f432a5b`.
The separately selected source applies both authorized preparation patches, changes
the package version and updates shipped status documentation. Both native Python
versions collect 2,704 tests; 11 focused correction tests passed on each. Those
preparation checks do not qualify a platform lane. Old Linux evidence remains
bound to dev10; replacement Linux cells and all final reviews remain required.
Dev11's native ordinary attempts failed or were interrupted and remain retained.
Its preparation checks did not qualify either complete ordinary Mac cell.

Dev12 remains preserved as sha256:0a88bf5087dc358c9df6a3a7af413a05a45a53437c79df355044f76008b2e0d7. Its complete native
preparation suites passed, but its formal Python 3.11 source suite failed
test_P14_T07_spawned_complete_verification_and_both_union_recoveries during late
storage-manager ledger admission. Dev12 is not qualified. Original archives,
preparation attempts and formal reports retain their own identities.

Candidate 2.0.0.dev13 remains preserved as sha256:8d97dc9b97978eab2effc146cffab1d07d1ea568194d2dd2f9f5bab664850ad3.
Its Python 3.11 formal source suite failed a strict native SIGINT descriptor
census. The originating allocation was not recorded; a separately controlled
directory-descriptor handoff diagnosis does not prove that historical leak site.
All original formal results, preparation failures, exact archives and policies
retain their own candidate identities and remain in the private return evidence.

Candidate 2.0.0.dev14 remains preserved as sha256:3645a6e4b41138f8f0dd3cc4fd9a31974df2dd975c2f458c6de7a725c9d14776.
Its Python 3.11 formal source and installed suites each failed the same two
directory-cancellation test fixtures: ordinary open returned a lower free
descriptor before the intended exception injection. Python 3.14 attempt002 was
explicitly NOT_RUN. The original failures remain rejected and the unexecuted
cell remains unexecuted; later fixture checks do not rewrite those outcomes.
All original formal results, preparation failures, exact archives and policies
retain their own candidate identities and remain in the private return evidence.

The replacement frozen candidate is 2.0.0.dev15, ID
sha256:bbc294507fb64e0bd82e5a235f4245fff14e4229609983929705107f73cf51e8. Its exact wheel SHA-256 is
19b84ddf71835f9f840adbc0d2c79d9a58aada9e9f3bbcb226e616777358d3a8; its sdist SHA-256 is
63068c4218c89fa23f18ee2a7ed4e0831abf26a7894a633f6fa6c3a58992b4a4.
The exact preparation changes and their evidence are recorded separately.
Preparation checks do not grant ordinary or resource qualification.

The current candidate-specific policy requires 2782 collected tests,
the same eight exact source-hashed Linux-only N/A nodes on Mac, and
2774 applicable passes per complete Mac suite.
Historical policies remain in platform-applicability-dev10.json,
platform-applicability-dev11.json, platform-applicability-dev12.json,
platform-applicability-dev13.json and platform-applicability-dev14.json; all six exact candidate/hash bindings are
recognized. Every other skip, xfail, failure or missing node blocks acceptance.
All four release cells and substantive human reviews remain required. Historical
commands and results retain their original identities; use the verified successor
for new execution. No push, publication or global release gate closure.

## What is already established

The original Plan 16 record remains unchanged: 2,703 source and 2,703 installed
tests, all 25 synthetic matrix cells/150 observations, 270 supplement observations,
six physical-cap workflows, and all 30,042,389 supplied pairs fully mixed, verified,
and recovered through both modes. That evidence applies to its recorded Linux
x86-64 CPython 3.11.7 native build. There is no reason to repeat the private full
supplied-data task merely to transfer these exact archives to another host.

Plan 17 additionally has a fresh, accepted Linux x86-64 CPython 3.11.7 lane at
`dist/plan17-dev10/platform/linux-x86_64-py311-attempt-001/`: 2,703 source and
2,703 installed tests, zero failures/skips, independent bgzip and pysam checks,
six dependency-free entry-point probes, private filesystem checks and matching
contents of a separately rebuilt sdist wheel. ZIP archive timestamps differ;
all 51 member contents match. [Current status](../../readmixer/python_update_plans/release-evidence.json)
records subsequent attempts and the exact remaining work.

## Current release blockers: CPython 3.14 regression fixtures

The repaired native CPython 3.14.7 lane finished with **2,701 passed / 2 failed**
for source and **2,700 passed / 3 failed** for installed tests. Both suites fail
`test_gathered_frames_have_no_production_owner_when_output_starts[indexed]` and
`[sequential]`. The frozen test assumes `sys.getrefcount` returns 3 for a solely
registry-owned bytes object; the same expression returns 2 on this interpreter.
An independent added-owner control increases each baseline by one. This supports
a test portability diagnosis; it does **not** make the archived failures passes.
The installed suite also fails `test_P06_004_oversized_singleton_rolls_back`:
its automatic-worker/512-MiB configuration refuses a segment reader before reaching
the intended oversized-pair boundary. This requires a deterministically admitted
fixture and proof of the intended checkpoint, not a relaxed exception assertion.
The preparation patches and their native focused-run evidence are recorded in the
[current status](../../readmixer/python_update_plans/release-evidence.json).

Do not spend release-host time expecting this dev10 packet to close all gates.
It remains useful for reproducing/diagnosing the current candidate. The [separately staged regression corrections](preparation/README.md) must be reviewed and included in a **new
frozen candidate** before final qualification. Preserve these dev10 archives and
all original Plan 16 results. Review which unchanged-runtime measurements can be
cited as prior supporting evidence, and rerun every affected lane under the new
identity; never relabel old raw reports as executions of the replacement.

## Candidate transport

Run commands from the repository root. All destinations described as new must
not exist; preserve failed attempts and use a fresh suffix for a retry. Keep the
whole release directory backed up: `dist/` is intentionally ignored by Git.
Only the public candidate packet is suitable for general distribution.

```bash
python -I tools/release/candidate.py verify --candidate dist/plan17-dev10/candidate
python -I tools/release/packet.py pack \
  --candidate dist/plan17-dev10/candidate --output /tmp/readmixer-dev10-candidate.zip
```

`pack` prints the packet SHA-256. Copy that packet and the **external**
`tools/release/` directory and `.github/workflows/release-qualification.yml`
to the other host (the external regression suite checks that workflow). Supply the printed packet hash and
candidate ID independently of the downloaded file:

```bash
python3 -I tools/release/packet.py unpack \
  --archive /tmp/readmixer-dev10-candidate.zip \
  --sha256 PACKET_SHA256_PRINTED_BY_PACK \
  --candidate-id sha256:426e563211c32b5bea806862d65bd0754419e397fb53ecaaa7239e11a00810ff \
  --output /tmp/readmixer-dev10-candidate
```

The packet contains exactly the candidate manifest and original wheel/sdist.
It contains no supplied data, output pools, environment credentials or diagnostics.
`candidate.py adopt` exists for adopting the already frozen provenance. The
separate [`prepare_candidate.py`](prepare_candidate.py) helper freezes an explicitly
selected preparation tree, records the complete source and locked builder, builds
one wheel and sdist, then adopts those exact bytes. It rejects excluded content,
requires the exact selected dependencies in an isolated environment and preserves
failed command logs. It does not select source or resolve dependencies. If runtime/source
changes become necessary, prepare a new version, complete source manifest and
controlled single build; then adopt those new artifacts and rerun affected lanes.

## Native platform preparation and ordinary lane

The required cells are Linux x86-64 and macOS arm64, each with CPython 3.11 and
3.14. Use an actual native interpreter; an x86 Mac/Rosetta run cannot qualify arm64.
Install `bgzip` from HTSlib on the host (`brew install htslib` on an Apple Silicon
Mac, or the distribution's `tabix`/HTSlib package on Linux). The report records its
version/hash. Ensure the selected Python provides `ssl`, `ctypes`, `zlib`, `bz2`,
`lzma`, `sqlite3`, `multiprocessing`, and `venv`. A locally compiled interpreter
with missing optional libraries is incomplete host preparation.

Choose distinct task paths and the intended native interpreter. These examples
prepare macOS arm64/Python 3.14; repeat with Python 3.11 and `py311` paths.
Use ordinary user-owned directories with sufficient free disk; do not place the
workspace inside the checkout or candidate directory.

On macOS, give each invocation a new private temporary directory reached through
its canonical path (for example, a task-owned directory under `/private/tmp`).
Set `TMPDIR`, `TMP`, and `TEMP` to that path before starting Python. The default
`/var/folders` spelling crosses a symlink and is deliberately rejected by the
runtime's no-follow directory checks. Record this host launch setting; preserve
any earlier failed attempt instead of changing its results.

```bash
release_python=python3.14
release_cell=macos-arm64-py314
release_base="$HOME/readmixer-qualification/$release_cell-attempt-001"
test ! -e "$release_base" || exit 1
mkdir -p "$release_base/environment/wheels"
"$release_python" -I -m venv "$release_base/bootstrap"
"$release_base/bootstrap/bin/python" -I -m pip install 'packaging==25.0'
"$release_base/bootstrap/bin/python" -I -m pip download --only-binary=:all: \
  --dest "$release_base/environment/wheels" \
  'numpy>=1.26.4,<3' 'pycryptodome>=3.23,<4' 'psutil>=5.9.3,<8' \
  'pytest>=7.4,<9' 'build==1.2.2.post1' 'setuptools==68.2.2' \
  'wheel==0.41.2' pip 'packaging==25.0' pysam
"$release_base/bootstrap/bin/python" -I tools/release/lock_environment.py capture \
  --wheelhouse "$release_base/environment/wheels" \
  --lock "$release_base/environment/requirements.lock" \
  --manifest "$release_base/environment/environment.json" \
  --require 'numpy>=1.26.4,<3' --require 'pycryptodome>=3.23,<4' \
  --require 'psutil>=5.9.3,<8' --require 'pytest>=7.4,<9' \
  --require 'build==1.2.2.post1' --require 'setuptools==68.2.2' \
  --require 'wheel==0.41.2' --require pip --require 'packaging==25.0' --require pysam
"$release_base/bootstrap/bin/python" -I tools/release/platform_lane.py \
  --candidate /tmp/readmixer-dev10-candidate \
  --environment "$release_base/environment" \
  --workspace "$release_base/work-001" \
  --report "$release_base/report-001" --expected-cell "$release_cell"
```

The network resolution above is **preparation**, performed once per new environment.
Qualification installs use only selected local wheels and exact hashes in a fresh
isolated environment. Retain the entire wheelhouse, lock and manifest; a package
version list alone is insufficient. Replay an existing lock without resolving
again. Changed selected wheels/native build invalidate affected qualification.

The runner executes archived tests, scripts and source, checks actual installed
imports and dependency versions, retains commands/exit codes/JSON/JUnit and its
own exact helper files, and compares a separate sdist rebuild. A nonzero exit is
unqualified, even if most tests passed. Do not edit archived tests to make a host
pass. Investigate the recorded failure, keep it, and decide whether host repair,
external harness correction, or a new package candidate is needed.

Eight **specific** frozen tests exercise the explicitly Linux-only qualification
launcher. [The predeclared applicability policy](platform-applicability.json)
identifies their exact nodes and source hashes. On macOS they remain collected
and appear as eight raw JUnit skips / eight `not_applicable` nodes, with **no pass
credit**; the candidate manifest test count minus these eight nodes must pass
in each suite, including both HTSlib oracles (dev10 required 2,695). Every other skip/xfail fails the lane. Portable descriptor/reaping
regressions and real macOS low-descriptor/resource workflows remain mandatory.
This policy does not waive production behavior or make ordinary CI a resource run.

## Manual GitHub Actions alternative

[release-qualification.yml](../../.github/workflows/release-qualification.yml)
is an opt-in four-cell workflow. It has not been dispatched by this work. It
accepts `candidate_url` (HTTPS), `candidate_sha256` (packet hash), and `candidate_id`.
Place **only the candidate packet** at a suitable accessible URL, make the external
harness/workflow available in your repository, then invoke workflow dispatch.
Each job checks the actual architecture and uses the same input archive bytes.
Actions are pinned to commit hashes. Per-job dependency selection is preparation;
subsequent installs are offline/hash-locked. Retain every matrix artifact.

Ordinary hosted runners do not provide the complete resource-host evidence.
A runner with fewer than four admitted CPUs cannot satisfy a four-CPU resource
profile. Use a suitable separate release host; do not lower the requested CPU
configuration just to obtain a pass.

Raw diagnostics are private and omitted from ordinary public artifact upload.
Use the optional encrypted-diagnostics transport described by
`private_evidence.py --help` and the workflow's `READMIXER_EVIDENCE_KEY` secret, or
run locally and retain the full report directory. A public-only artifact can be
inspected but cannot satisfy final raw-evidence integrity. Do not send the key in
chat, command-line arguments, the candidate packet or a public artifact.

To retain CI diagnostics securely, create a random key **once**, outside the
checkout/evidence tree, and store it in your password manager. This command writes
an exclusive private file and does not print the key:

```bash
python3 -I - <<'PY'
import base64, os, secrets
from pathlib import Path
p = Path.home() / 'readmixer-private-evidence-key.txt'
with os.fdopen(os.open(p, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), 'wb') as f:
    f.write(base64.b64encode(secrets.token_bytes(32)) + b'\n')
PY
```

Set the repository Actions secret `READMIXER_EVIDENCE_KEY` to that file's contents
through GitHub's secret interface. Do not commit the file. After downloading the
artifacts, restore raw diagnostics with a Python environment that has the locked
PyCryptodome installed:

```bash
IFS= read -r READMIXER_EVIDENCE_KEY < "$HOME/readmixer-private-evidence-key.txt"
export READMIXER_EVIDENCE_KEY
"$release_base/work-001/venv/bin/python" -I tools/release/private_evidence.py decrypt \
  --archive /path/to/downloaded/public/private-evidence.rmp17 \
  --output /path/to/returned/report/private
unset READMIXER_EVIDENCE_KEY
```

The output directory must be new. Authentication failure leaves no extracted
plaintext; a missing key cannot be recovered from the encrypted artifact. The
key itself is not needed in chat: keep it available locally for reconciliation.

The older [python.yml](../../.github/workflows/python.yml) remains a development
feedback workflow. Its per-job rebuilds/floating dependencies are not release
qualification and cannot replace this lane.

## Resource and native-build qualification

The platform workspace retains `venv/bin/python` and `installed-work/scripts/`.
Use those exact installed resources and the same environment on each actual
release host. [resource_lane.py](resource_lane.py) verifies every archived helper,
installed module and selected dependency payload, including native library bytes.
It runs frozen matrix definitions with one warm-up and five measured observations.

```bash
"$release_base/bootstrap/bin/python" -I tools/release/resource_lane.py \
  --candidate /tmp/readmixer-dev10-candidate \
  --environment "$release_base/environment" \
  --installed-python "$release_base/work-001/venv/bin/python" \
  --archived-scripts "$release_base/work-001/installed-work/scripts" \
  --workspace "$release_base/resource-work-001" \
  --report "$release_base/resource-report-001" \
  --scratch-parent /Volumes/ActualDistinctScratchFilesystem \
  --profile bounded
```

`--scratch-parent` must identify a real distinct device for the separate-device
cell; a second directory on the same device is insufficient. On Linux choose an
actual separately mounted scratch filesystem and a host permitting the frozen
cgroup/affinity launcher. Read the emitted preflight/launch failure rather than
running arbitrary system-wide cleanup or reusing unrelated mounts.

Run again with **new** workspace/report names and `--profile scale`; this checks
normalized input strictly greater than four times actual admitted RAM for external,
auto and strict-memory outcomes. The large `--profile physical` measures the
physical common-cap boundary; the existing six Linux 3.11 dev10 observations are
already preserved. Determine any necessary repetition for a different codec/build
in the attributed final review. Do not automatically repeat the private supplied
data on public release hosts.

`resource-lane.json` says which selected profile completed. It explicitly leaves
component/startup measurements, native charge calibration, ordinary real-signal
outcomes, and filesystem provenance separate. A successful profile alone does not
qualify Plan 17. On macOS the four Linux enforcement cells are explicitly outside
the portable contract; portable nofile/process-group controls are measured there.

The additional bounded component/startup/charge probes have their own runner:

```bash
"$release_base/bootstrap/bin/python" -I tools/release/supplement_lane.py \
  --candidate /tmp/readmixer-dev10-candidate \
  --environment "$release_base/environment" \
  --installed-python "$release_base/work-001/venv/bin/python" \
  --archived-scripts "$release_base/work-001/installed-work/scripts" \
  --workspace "$release_base/supplement-work-001" \
  --report "$release_base/supplement-report-001"
```

This preserves archived validators and adds the new native environment binding.
Its five jobs cover 150 observations: component/startup probes, default and
64-MiB calibration probes, and tiny three-pair fresh-process workflows at CPU
budgets one/four. The tiny workflow is startup/correctness evidence, not
representative throughput. It explicitly leaves the original 108 repeated
six-artifact-oracle observations and 12 CPU2/CPU4 compression-charge observations
outside its scope; these remain necessary new-build evidence. Exact node IDs, warm-up/repetition
counts and compression-probe requirements are retained at
`dist/plan17-dev10/supplements/linux-x86_64-py314-attempt-001/scope-and-remaining.json`. The full larger
resource matrix remains separate. A sampled charge excess is a failure to
investigate, not permission to raise the asserted limit without requalification.

The external `native_supplement_lane.py` runner implements that remaining
native-build orchestration without modifying archived tests or scripts:

```bash
"$release_base/bootstrap/bin/python" -I tools/release/native_supplement_lane.py \
  --candidate /tmp/readmixer-dev11-candidate \
  --environment "$release_base/environment" \
  --installed-python "$release_base/work-001/venv/bin/python" \
  --archived-scripts "$release_base/work-001/installed-work/scripts" \
  --workspace "$release_base/native-supplement-work-001" \
  --report "$release_base/native-supplement-report-001" \
  --profile all
```

Use the actual verified candidate path and new destinations for every attempt.
Profiles `oracle`, `compression` and `fresh` can run independently. `all` retains
108 individual oracle observations with phase JSON/JUnit, 12 full retained
compression-window observations, and 12 larger fresh-workflow observations at
counts `(256,128,0)` and CPU budgets one/four. Each configuration includes one
warm-up and five measurements. Completed or failed larger fresh observations
are saved individually before the next child, preserving earlier results across
interruptions. Every job binds the exact candidate, dependency wheelhouse/lock,
installed package/dependency payloads and executed harness. Actual host CPU
admission is checked before the archived oracles; their existing explicit
topology observations remain scoped as test controls. Compression records actual
live threads, native workspaces, the full two/six-result window, independent BGZF
decoding, joined threads and zero remaining Budget charges. Charge excess or
missing measurements fail acceptance. No kernel CPU/RAM enforcement, cold cache,
universal memory bound or cross-platform speedup is asserted.

Run the external harness tests on each new native host as well:

```bash
"$release_base/work-001/venv/bin/python" -m pytest -q tools/release/tests \
  -o addopts= --junitxml="$release_base/release-tool-tests.xml"
```

Retain the output and actual helper hashes. In particular the portable nofile
kernel-readback/unchanged-parent/private-output test and timeout/reaping test must
execute on macOS, not merely have a Linux pass. Charge calibration must inspect
actual measured builds; if a charge is deficient, fix preparation and issue a new
candidate, then rerun affected evidence. A sampled peak is not a universal RSS
bound. Keep all observations, warm-ups, failed attempts and cache treatment.

## Assemble evidence and request the final review

Bring back each **complete report directory**, its exact environment/wheelhouse,
and the external harness snapshot. Copy the selected artifacts into a private
local tree, for example `dist/plan17-dev10/imported/`. Preserve failed attempts.
For CI, download all matrix artifacts and decrypt the private diagnostic file to
the corresponding report's `private/` directory; retain the key securely elsewhere.
Do not edit reports or screenshots to remove a failed outcome.

Create a new index rather than overwriting an earlier assessment:

```bash
python -I tools/release/gates.py init \
  --candidate dist/plan17-dev10/candidate \
  --output dist/plan17-dev10/closure-index-001.json
```

Each non-null reference in the index has `path` (relative to the chosen evidence
root), `bytes`, and `sha256`. A platform slot points to its `report.json`. A Linux
report cannot fill a macOS slot. Leave unexecuted entries null. Ask Codex to import
and fingerprint returned files; setting a boolean to true is not execution evidence.
The current [release status](../../readmixer/python_update_plans/release-evidence.json)
provides an initial index/assessment location with available evidence.

Resource and calibration conclusions need an **attributed review** of the actual
observations for each selected native environment. The tool deliberately separates
this review from mechanically checked ordinary tests. Generate an unaccepted
review skeleton, for example:

```bash
python -I tools/release/gates.py review-template \
  --candidate dist/plan17-dev10/candidate --kind resources \
  --cell macos-arm64-py314 --environment-sha256 EXACT_ENVIRONMENT_MANIFEST_SHA256 \
  --output dist/plan17-dev10/macos314-resource-review-001.json
```

The reviewer supplies identity/date, a specific finding for **every** criterion,
and hashes of supporting execution records. Do not populate acceptance merely
because a command started or a summary exists. `--kind calibration` covers
native/workspace/startup/performance observations. A global `--kind documents-audit`
review (without cell/environment flags) covers all parent scenarios, all 24 audit
mappings, documentation, privacy, historical fingerprints and harness tests.
Existing Linux 3.11 Plan 16 evidence should be reviewed for scoped reuse with the
retained `historical-lock-equivalence.json`; it is not transferable to a different
native dependency build.

```bash
python -I tools/release/gates.py assess \
  --candidate dist/plan17-dev10/candidate \
  --index dist/plan17-dev10/closure-index-001.json \
  --evidence-root . --output dist/plan17-dev10/assessment-001.json
```

Exit 1 lists missing/invalid obligations; exit 0 means **ready for the Plan 10
review**, not published or automatically released. The checker rehashes records,
reconciles raw collection/execution/JUnit, verifies native versions, fixed policy,
entry points, stages, rebuilt contents and retained harness. Attributed reviews
are labeled `review_attested` with `automated_execution_validation: false`.
`--public-only` intentionally leaves final raw integrity blocked. Both
`plan10_complete` and `plan17_complete` stay false until the final substantive
release decision is documented. Publishing a package requires a separate action.

To hand the work back, put the returned evidence under the shared checkout's
`dist/plan17-dev10/imported/` and say:

> Continue Plan 17 reconciliation using the reports and wheelhouses at [local
> directory], including failed attempts. Rehash the files, check the actual hosts
> and dependency builds, review every remaining criterion, update the status and
> tell me whether Plan 10 can be closed. Do not publish the package.

A full report directory is useful proof. A screenshot, CI green badge, package
version list, “tests passed” message, or edited completion flag alone is not.
