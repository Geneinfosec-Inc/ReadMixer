#!/usr/bin/env python3
"""Freeze an explicitly selected tree and build one new candidate in a locked venv.

The caller selects/corrects source in a new private preparation tree first. This
helper never selects the checkout, resolves dependencies, or edits an old bundle.
Every attempt, including an unsuccessful build, remains in its new destination.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import importlib.metadata
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))
from candidate import (_source_allowed, adopt_candidate, canonical, load_candidate,
                       read_json, sha256_file)
from lock_environment import verify_environment


def write_new(path, value):
    with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'wb') as stream:
        stream.write(canonical(value) + b'\n')
        stream.flush()
        os.fsync(stream.fileno())


def selected_manifest(root):
    """Reject rather than silently omit any unselected or linked source content."""
    root = Path(root)
    if root.is_symlink() or not root.is_dir():
        raise ValueError('selected source must be an ordinary directory')
    result = {}
    for path in sorted(root.rglob('*')):
        mode = path.lstat().st_mode
        if stat.S_ISLNK(mode):
            raise ValueError('linked selected source is forbidden')
        if stat.S_ISDIR(mode):
            continue
        name = path.relative_to(root).as_posix()
        if not stat.S_ISREG(mode) or not _source_allowed(name):
            raise ValueError('selected source contains excluded content')
        result[name] = {'bytes': path.stat().st_size, 'sha256': sha256_file(path)}
    if not result:
        raise ValueError('empty selected source')
    return result


def require_unchanged(root, manifest):
    if selected_manifest(root) != manifest:
        raise ValueError('frozen selected source changed')


def _run(command, cwd, report, ordinal):
    start = datetime.now(timezone.utc).isoformat()
    env = {k: v for k, v in os.environ.items() if k not in
           ('PYTHONPATH', 'PYTHONHOME', 'VIRTUAL_ENV', '__PYVENV_LAUNCHER__')}
    env.update({'PIP_NO_INDEX': '1', 'PIP_DISABLE_PIP_VERSION_CHECK': '1',
                'OPENBLAS_NUM_THREADS': '1', 'OMP_NUM_THREADS': '1',
                'MKL_NUM_THREADS': '1', 'VECLIB_MAXIMUM_THREADS': '1',
                'NUMEXPR_NUM_THREADS': '1'})
    log = report / ('command-%03d.log' % ordinal)
    with os.fdopen(os.open(log, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'wb') as output:
        process = subprocess.run(command, cwd=cwd, env=env, stdout=output,
                                 stderr=subprocess.STDOUT, check=False)
    record = {'command': [str(x) for x in command], 'cwd': str(cwd),
              'started_utc': start, 'ended_utc': datetime.now(timezone.utc).isoformat(),
              'exit_code': process.returncode, 'log': log.name, 'log_sha256': sha256_file(log)}
    write_new(report / ('command-%03d.json' % ordinal), record)
    if process.returncode:
        raise ValueError('preparation command failed; original command and log retained')
    return record


def build_candidate(source, environment, output, base_bundle, expected_tests):
    """Run with the selected locked build interpreter using ``python -I``."""
    source, environment, output = map(lambda p: Path(p).resolve(), (source, environment, output))
    if output.exists() or output.is_symlink():
        raise ValueError('preparation destination must be new')
    if source == output or source in output.parents or output in source.parents:
        raise ValueError('source and preparation destination must be separate')
    before = selected_manifest(source)
    base = load_candidate(base_bundle)
    manifest = read_json(environment / 'environment.json')
    lock = (environment / 'requirements.lock').read_bytes()
    verify_environment(environment / 'wheels', manifest, lock)
    selected = {w['name'].lower().replace('_', '-'): w['version'] for w in manifest['wheels']}
    actual = {d.metadata['Name'].lower().replace('_', '-'): d.version
              for d in importlib.metadata.distributions()}
    if sys.prefix == sys.base_prefix or actual != selected:
        raise ValueError('build requires the exact selected dependencies in an isolated venv')
    output.mkdir(mode=0o700, parents=True)
    report = output / 'private'; report.mkdir(mode=0o700)
    shutil.copy2(__file__, output / 'executed-prepare_candidate.py')
    shutil.copytree(source, output / 'selected-source', copy_function=shutil.copy2)
    frozen = output / 'selected-source'
    # Generate packaging metadata before the source freeze, without building an
    # archive. Its selected egg-info files are then part of the complete manifest.
    metadata = _run([sys.executable, '-I', '-c',
                     'import setuptools.build_meta as b; b.get_requires_for_build_sdist()'],
                    frozen, report, 1)
    records = selected_manifest(frozen)
    write_new(output / 'source-freeze.json', {
        'schema': 'readmixer.release.source-freeze.v1',
        'source_manifest': records, 'source_manifest_sha256': __import__('hashlib').sha256(canonical(records)).hexdigest(),
        'base_candidate_id': base['candidate_id'], 'base_archives': base['archives'],
        'build_environment_sha256': sha256_file(environment / 'environment.json'),
        'build_lock_sha256': sha256_file(environment / 'requirements.lock'),
        'input_manifest': before, 'metadata_preparation': metadata,
        'source_scope': 'All and only files in the separately selected preparation tree; generated metadata frozen before archive build.'})
    shutil.copytree(frozen, output / 'build-source', copy_function=shutil.copy2)
    archives = output / 'archives'; archives.mkdir(mode=0o700)
    build = _run([sys.executable, '-I', '-m', 'build', '--no-isolation', '--wheel', '--sdist',
                  '--outdir', str(archives)], output / 'build-source', report, 2)
    require_unchanged(source, before)
    require_unchanged(frozen, records)
    wheels = list(archives.glob('*.whl')); sdists = list(archives.glob('*.tar.gz'))
    if len(wheels) != 1 or len(sdists) != 1 or len(list(archives.iterdir())) != 2:
        raise ValueError('build did not produce exactly one wheel and one sdist')
    import tomllib
    version = tomllib.loads((frozen / 'pyproject.toml').read_text())['project']['version']
    provenance = {'schema': 'readmixer.release.build-provenance.v1', 'package_version': version,
        'base_commit': base.get('base_commit'), 'dirty_tree_scope': [
            'New source derived from verified ' + base['candidate_id'] + '; see source-freeze input manifest and recorded preparation patch.'],
        'base_candidate_id': base['candidate_id'], 'source_manifest': records,
        'source_scope': 'Exact separately selected tree frozen before the single archive build; historical evidence remains candidate-bound.',
        'build_environment_sha256': sha256_file(environment / 'environment.json'),
        'build_lock_sha256': sha256_file(environment / 'requirements.lock'),
        'builder': {'python': sys.version, 'executable_sha256': sha256_file(Path(sys.executable).resolve()),
                    'selected_distributions': actual}, 'build_command': build,
        'archives': {p.name: {'bytes': p.stat().st_size, 'sha256': sha256_file(p)} for p in (*wheels, *sdists)}}
    write_new(output / 'candidate-provenance.json', provenance)
    return adopt_candidate(wheels[0], sdists[0], output / 'candidate-provenance.json',
                           output / 'candidate', expected_tests)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('source', 'environment', 'output', 'base-candidate'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--expected-tests', type=int, required=True)
    args = parser.parse_args(argv)
    result = build_candidate(args.source, args.environment, args.output, args.base_candidate, args.expected_tests)
    print(json.dumps({'candidate_id': result['candidate_id'], 'version': result['version'], 'archives': result['archives']}, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
