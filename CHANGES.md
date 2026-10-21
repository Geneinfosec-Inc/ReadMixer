# Unreleased GitHub preparation — 2026-10-21

The README now documents clean-clone installation, editable development, wheel
and source builds, the current four-command CLI, single-sample recovery and
Docker usage. Build contexts exclude local datasets, recovery material and
environments. Installation helpers and development CI follow the package layout.

A complete macOS arm64 / CPython 3.11.9 run mixed 48 samples (30,042,389 pairs),
fully verified the pool and independently matched one recovered sample against
its originals. A public aggregate record omits private paths and identifiers.
Timings describe this machine and workload, not general performance guarantees.

Development-test portability corrections retain independent reference ownership
checks and deterministic boundary setup. Exact Linux kernel fixtures remain
active on Linux; native portable resource checks run on both platforms. Test
modules run in separate interpreters so resource checks use fresh coordinator
baselines, preserving all budgets and within-case lifetime assertions.

Mac installed-wheel signal checks exposed an asynchronous CLI cancellation gap:
a signal after exclusive worker-scratch creation could leave its descriptor and
empty file without a cleanup owner. Directory revalidation also had an open-to-
owner handoff that could leak during cancellation. Cancellation now waits until
these short transitions have recorded ownership and closed their descriptors.
Ten real-signal handoff regressions reject the original code; nested-signal and
error-reset controls cover the guard. Successful artifact formats are unchanged.
Historical frozen archives and evidence are unchanged; Python 3.14 execution and
supported-platform qualification remain separate validation work.

---

# Unreleased external release qualification tooling — 2026-09-30

Plan 17 now has safe immutable-candidate transport, native dependency wheel locks,
isolated source/installed execution, explicit Linux-only test applicability,
resource and bounded supplement launchers, authenticated private-diagnostic
transport, and a fail-closed evidence checker. A separate manual CI workflow uses
one candidate across native platform jobs. Development CI is labeled separately.

The frozen dev10 runtime and completed Plan 16 archives/evidence are unchanged.
A fresh Linux CPython 3.11 lane passed all 2,703 source and 2,703 installed tests.
CPython 3.14 qualification exposed two exact-reference-count regression failures
and an installed boundary fixture that reached an earlier admission refusal.
They remain recorded failures, with separate regression corrections prepared for
review/new-candidate work. Package publication and Plan 10/17 closure remain open.
See the [operator handoff](tools/release/README.md) and
[execution index](readmixer/python_update_plans/release-evidence.json).

---

# Unreleased resource qualification corrections — 2026-09-29

Version `2.0.0.dev10` corrects a failure exposed by Plan 16's larger-than-RAM
auto-storage workload. Sorter outputs that already require encrypted disk storage
now write there from creation, so an unsealed merge run cannot exhaust resident
RAM before its planned spill. Strict memory mode retains its no-spill contract.

Frame LF checks and header token scanning now search borrowed contiguous buffers
in native code, preserving exact byte/error contracts without payload copies.
Independent grammar, buffer-lifetime and artifact-oracle tests cover the change.
The serial verification fixture explicitly selects one worker, avoiding unrelated
host CPU counts inflating its setup admission.

Qualification now journals completed observations privately after owned cleanup;
checkpoint failures prevent a successful final report. The physical-cap workload
has a 64 GiB host envelope for its input/output page cache, independent of the
unchanged 2 GiB application budget. Explicit constrained-host limits still apply.
Dev9 measurements, failed auto-scale observations and administratively interrupted
long runs remain historical evidence. The frozen dev10 candidate completed the
scoped local Plan 16 qualification: 2,703 source and 2,703 installed tests,
25 matrix cells with 150 observations (126 complete workflows and 24 expected
resource refusals), and 270 supplemental observations. All 30,042,389 supplied
pairs were mixed, fully verified, and independently compared after recovery in
both modes. Greater-than-four-budget external/auto workflows, clean strict-memory
refusal, and the physical default-cap lane passed under their recorded controls.

[Candidate-bound evidence](readmixer/python_update_plans/resource-qualification-evidence.json)
retains individual measurements, failed earlier attempts and their limitations.
These are local Linux build/workload results, not universal RSS or speedup claims.
Plan 17 supported-platform qualification and the Plan 10 release-completion gate
remain open; package publication is separate. Pool schema 1, shuffle algorithm 1
and public commands remain unchanged.

---

# Unreleased workflow acceptance — 2026-09-28

Version `2.0.0.dev9` prepares Plan 16 qualification: bounded coordinator/worker
stage and I/O telemetry, independent complete supplied-data validation,
component and fresh-process benchmarks, a named workload matrix, isolated Linux
host controls, and a fully hash-locked local dependency environment. Failed
observations remain in reports and suppress performance summaries. Runtime
schema 1, shuffle algorithm 1 and the public CLI remain unchanged. Qualification
results are recorded separately from the frozen source archive; historical
candidate results are not relabeled as dev9 evidence. A new low-compressibility
v3 fixture recipe prevents accidental conflicting mate markers in opaque headers;
existing v1/v2 fixture bytes remain unchanged.


Version `2.0.0.dev8` completes Plan 15's bounded local acceptance with 337 new
cases: independent worker artifact oracles, exact marker/inventory boundaries,
self-consistent hostile pools, explicit recovery/original-input scopes, reached
resource and publication faults, real signals, broken IPC and crash residue.

A source-close failure in full verification could leave pool metadata/registry
leases held. Cleanup now attempts all remaining owners and preserves the first
causal error, including constructor cleanup failures. Tests retain the failing
tracebacks while checking descriptor and accounting release. Guarded acceptance
helpers ship in the source archive; installed validation exercises the exact
console entrypoint outside the checkout.

Evidence is recorded separately in `workflow-acceptance-evidence.json`; all older
candidate records and archive hashes remain unchanged. Plan 16 resource/scale/
performance/full supplied-data work, Plan 17 locked platform qualification and
Plan 10 release closure remain open. Formats, CLI commands and deferred feature
scope are unchanged.

---

# Unreleased deterministic parallel pipeline — 2026-09-28

Version `2.0.0.dev7` implements Plan 14 production integration: deterministic LPT
source jobs, exact normalized admission, complete 64-writer scatter generations,
coordinator-issued encrypted capabilities/raw handoffs, canonical terminal queues,
bulk AES tags, and stable closed-policy sort jobs shared by verification/recovery.
Resource admission includes ownership registries and transport overlap; serial
fallbacks preserve fixed topology and exact artifact bytes. Producer scratch and
workers close before publication. Reports describe actual admitted concurrency.

Local tests exercise real spawned workflows, independent artifact equality,
one-pass source reading, one union gather, hostile handoffs and worker cleanup.
Historical candidate evidence stays unchanged; dev7 evidence is recorded separately
in `parallel-pipeline-evidence.json`. Plans 15–17 and Plan 10 release closure remain
open. No representative performance or supported-platform qualification is claimed.

---

# Unreleased protected worker handoffs — 2026-09-28

Version `2.0.0.dev6` implements Plan 13's remaining private worker mechanisms:
READY admission fallback, exclusive memory receipts and child subleases, exact
segment-key capabilities, coordinator disk credits, authenticated paged task
ownership and ordered encrypted results. Cleanup retains uncertain ownership,
reaps writers before deletion, and preserves causal errors across interrupted
handoffs. Real-process fault, signal and concurrent launch tests extend local
acceptance. Disk reservations can retain their cleanup owner before committing
accounting, and interrupted credit reconciliation remains retryable.

Public data stages remain serial with compression threads. Plan 14 production
integration and Plans 15–17 workflow/resource/platform acceptance remain open;
this is a development candidate, not full cross-plan release closure. Historical
evidence/archives are preserved; dev6 uses worker-handoff-evidence.json.

---

# Unreleased private worker foundations — 2026-09-25

Version `2.0.0.dev5` partially implements Plan 13: explicit spawn workers with
pre-import native-thread limits, aggregate child/helper admission, fixed CPU
partitions, bounded binary control/chunk channels, exclusive sender-lease
retention, ordered charged results, and cooperative/terminate/kill/reap cleanup.
The private runtime supports one fixed in-memory identity-sort policy. Real
subprocess tests cover malformed peer states, failed starts, interruptions,
shutdown escalation and reservations retained until successful reaping.

Production mix/verify/recover still execute their data stages on the coordinator.
Encrypted segment capabilities, derived keys, task ownership pages, disk credits,
auto-spill handoffs, startup concurrency retry and Plan 14 integration remain open.
Versioned worker charges have local checks, not platform/scale calibration.

Plan 15 adds a bounded independent serial six-artifact oracle, including explicit
pair identities, exact wire bytes, rollover and both recovery modes. Plans 15/16
remain partial; Plan 17 and the Plan 10 release gate remain open. Separate dev5
spawn evidence preserves all historical results and archive hashes unchanged.

---

# Unreleased paged output ownership — 2026-09-24

Version `2.0.0.dev4` implements Plan 12 locally: one operation store holds bounded
active output handles, authenticated inventory/link pages, and interrupted
handoffs. Publication traverses pages with the completion marker last. Cleanup
retains per-page progress, prevents repeated disk refunds, preserves uncertain
or foreign residue, and reports ordinary postcommit failures as bounded warnings.

Plan 15 adds independently instrumented indexed-union I/O/cache tests and
self-consistent secured-record mutations. Plan 16A adds versioned high/low-
compressibility and varied-length fixtures, fresh process/import/CLI startup
lanes, and scoped child CPU/native-thread observations. These are partial plan
increments, not measured release qualification. Plans 13/14, the remaining
Plan 15 acceptance matrix, Plan 16 scale/full-data qualification and Plan 17
remain open. Historical dev2/dev3 evidence and archives are preserved.

---

# Unreleased shared metadata admission — 2026-09-24

Version `2.0.0.dev3` implements Plan 11's shared resident-metadata ceiling as a
subset of total RAM admission. Lease acquisition, resize, split, transfer and
release account for metadata, RAM and descriptors together. Catalogs, protected
registries, parsed pools, output descriptors, serialization and reports share
that ceiling. Closing source/codec descriptors no longer releases retained
metadata prematurely. Failed operation tracebacks release owned object graphs.

Added independent Plan 15 rollover coverage for reachable R1/R2/key boundaries,
decimal-offset regeneration, and mathematically dominated index/sidecar growth.
This is partial Plan 15 acceptance. Paged publication ownership, spawn workers,
parallel production stages, representative scale/performance measurements and
platform/release closure remain open. Historical dev2 evidence is unchanged.

---

# Unreleased complete serial Python workflow — 2026-09-23

Version `2.0.0.dev2` enables paired mixing, complete independent verification,
and union sample recovery in indexed or explicit sequential mode. Added canonical
bucket/refinement and native cryptographic rank sorting; exact BGZF, CSV key,
physical-order index and manifest formats; synchronized capped bundles/recovery
parts; private receipt-last publication; and one CPU/memory-admitted compression
service shared by all lanes. No production deterministic seed is exposed.

Independent format, corruption, bijection, recovery, privacy, distribution and
concurrency tests now exercise the public workflow. HTSlib interoperability and
bounded-prefix roundtrips of supplied input data passed locally. Added synthetic
fixture/benchmark tooling and CI configuration. Spawn parser/terminal workers,
paged output ownership, measured resource envelopes and the complete platform/
release matrix remain pending; this is not a qualified release.

---

# Unreleased Python storage foundations — 2026-09-23

Version `2.0.0.dev1` adds lossless PYPAIR1 frames, domain-separated operation
keys, authenticated RMPYTMP1 scratch, memory/external/auto storage, disk capacity
reservations, safe directory anchors, paged ownership ledgers, bounded external
sorting, exact identity coverage, and last-marker transactional publication.
Corruption, allocation, I/O, capacity, cancellation, and ownership-transition
faults are covered by regression tests. Supplied paired gzip inputs were fully
validated, and a bounded prefix passed encrypted frame roundtrip checks.

These internal primitives prepare the serial shuffle and output pipeline.
Public mix/verify/recover remain unavailable. See the current
[implementation record](readmixer/python_update_plans/IMPLEMENTATION.md) for
precise coverage, test counts, and remaining limits.

---

# Unreleased Python modernization foundations — 2026-09-23

Added the `readmixer-py` development package, strict read-only paired inspection,
byte-preserving bounded FASTQ/gzip decoding, canonical safe catalogs, typed errors,
explicit byte units, and initial resource admission. Retired the independent-record
chunk engine and its old flags; its source remains in Git history. The old script
now forwards to the installed package. Core and plotting dependencies are separate.

Mix/verify/recover deliberately fail as unsupported until their complete paired
implementations exist. Full release qualification remains pending. See
[implementation progress](readmixer/python_update_plans/IMPLEMENTATION.md).

---

# Installation Updates - Changes Summary

## Overview

This document summarizes the changes made to address installation issues on systems with older GCC versions (< 9.3), particularly on cluster environments with GCC 4.8.5.

## Problem Statement

Users reported installation failures when running `pip install -r requirements.txt` on systems with GCC < 9.3:
- NumPy 2.x requires GCC >= 9.3 to compile from source
- contourpy package has similar GCC requirements
- Error: "NumPy requires GCC >= 9.3"

## Solution Implemented

The solution uses conda to install pre-compiled binaries of numpy and contourpy, bypassing the GCC version requirement.

## Files Added

### 1. **environment.yml**
Location: `readmixer/environment.yml`

Conda environment specification file that includes:
- Python 3.11
- numpy 2.3 (pre-compiled via conda)
- contourpy 1.3 (pre-compiled via conda)
- All other dependencies via pip

**Usage:**
```bash
conda env create -f environment.yml
conda activate readmixer-env
```

### 2. **requirements-pip.txt**
Location: `readmixer/requirements-pip.txt`

Pip requirements file with numpy removed (for use with Method 2).

**Contents:** All original requirements except numpy (which is installed via conda).

### 3. **Dockerfile.conda**
Location: `readmixer/Dockerfile.conda`

Alternative Dockerfile using conda base image for maximum compatibility.

**Usage:**
```bash
docker build -f Dockerfile.conda -t readmixer-conda:latest .
docker run -v /path/to/data:/data readmixer-conda:latest -s -p /data
```

### 4. **INSTALLATION.md**
Location: `readmixer/INSTALLATION.md`

Comprehensive installation guide covering:
- 5 different installation methods
- Detailed troubleshooting section
- Environment-specific recommendations
- GCC version checking instructions
- Conda installation instructions
- Verification steps

### 5. **setup.sh**
Location: `readmixer/setup.sh`

Interactive installation helper script that:
- Detects available tools (conda, gcc)
- Checks GCC version
- Recommends appropriate installation method
- Can perform automatic installation
- Provides guidance for missing dependencies

**Usage:**
```bash
chmod +x setup.sh
./setup.sh
```

## Files Modified

### 1. **README.md**
Location: `README.md` (root)

**Changes:**
- Updated System Requirements section to mention GCC >= 9.3 requirement
- Reorganized Installation section with 4 methods:
  1. Conda with environment.yml (recommended)
  2. Conda with manual package installation
  3. pip (requires GCC >= 9.3)
  4. Docker (both standard and conda versions)
- Added explanation of why conda method is needed
- Added new "Installation Issues" section under Troubleshooting
- Added instructions for checking GCC version
- Added link to INSTALLATION.md

### 2. **readmixer/readme.md**
Location: `readmixer/readme.md`

**Changes:**
- Same updates as main README.md
- Updated to match the structure and content of root README.md

### 3. **Dockerfile**
Location: `readmixer/Dockerfile`

**Changes:**
- Added comments explaining GCC requirement
- Added reference to Dockerfile.conda alternative
- No functional changes to the build process

## Installation Methods

Users now have 5 installation methods to choose from:

### Method 1: Conda with environment.yml (Recommended)
```bash
conda env create -f environment.yml
conda activate readmixer-env
```
**Best for:** All users, especially cluster systems

### Method 2: Conda with Manual Installation
```bash
conda create -y -n readmixer-env python=3.11 numpy=2.3 contourpy=1.3
conda activate readmixer-env
pip install -r requirements-pip.txt
```
**Best for:** Users needing control over package versions

### Method 3: pip (Requires GCC >= 9.3)
```bash
python -m venv readmixer-env
source readmixer-env/bin/activate
pip install -r requirements.txt
```
**Best for:** Modern systems with GCC >= 9.3

### Method 4: Docker Standard
```bash
docker build -t readmixer:latest .
docker run -v /path/to/data:/data readmixer:latest -s -p /data
```
**Best for:** Docker users with compatible base images

### Method 5: Docker with Conda
```bash
docker build -f Dockerfile.conda -t readmixer-conda:latest .
docker run -v /path/to/data:/data readmixer-conda:latest -s -p /data
```
**Best for:** Docker users needing maximum compatibility

## Key Improvements

1. **Compatibility:** Works on systems with any GCC version
2. **User-Friendly:** Interactive setup script and comprehensive documentation
3. **Flexibility:** Multiple installation methods for different environments
4. **Troubleshooting:** Detailed error explanations and solutions
5. **Docker Support:** Both standard and conda-based Docker options
6. **Documentation:** Comprehensive INSTALLATION.md guide

## Testing Recommendations

To verify the changes work correctly:

1. **Test on system with GCC 4.8.5:**
   ```bash
   ./setup.sh
   # Should recommend conda method
   ```

2. **Test conda installation:**
   ```bash
   conda env create -f environment.yml
   conda activate readmixer-env
   python readmixer.py --help
   ```

3. **Test manual conda installation:**
   ```bash
   conda create -y -n readmixer-test python=3.11 numpy=2.3 contourpy=1.3
   conda activate readmixer-test
   pip install -r requirements-pip.txt
   python readmixer.py --help
   ```

4. **Test Docker conda build:**
   ```bash
   docker build -f Dockerfile.conda -t readmixer-conda:test .
   docker run readmixer-conda:test --help
   ```

## Migration Guide for Existing Users

Existing users who had working installations are not affected. However, if they want to adopt the new recommended method:

1. **Remove old environment:**
   ```bash
   conda deactivate
   conda env remove -n readmixer-env
   ```

2. **Create new environment:**
   ```bash
   conda env create -f environment.yml
   conda activate readmixer-env
   ```

## Backward Compatibility

All changes are backward compatible:
- Original `requirements.txt` still works on systems with GCC >= 9.3
- Original `Dockerfile` still works
- No changes to the main `readmixer.py` or processing code
- Existing installations continue to work

## References

- NumPy GCC requirements: https://numpy.org/devdocs/user/building.html
- Issue reported by user on cluster system with GCC 4.8.5
- Workaround provided: Pre-install numpy and contourpy via conda

---

**Date:** October 29, 2025
**Affects:** Installation process only, no functional changes to ReadMixer

