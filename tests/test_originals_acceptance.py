"""P15-T05/T08: originals are compared by normalized identity, never location."""

import gzip
import hashlib
import json

import pytest

from readmixer_py.config import VerifyConfig
from readmixer_py.errors import InputError, IntegrityError
import readmixer_py.verification as verification
from test_api import cli
from test_pipeline import mix_fixture


def snapshot(root):
    return {str(p.relative_to(root)): (p.stat().st_mode, hashlib.sha256(p.read_bytes()).digest())
            for p in root.rglob('*') if p.is_file()}


def relocated_sheet(root, expected, mutation=None):
    """Different basenames, compression, gzip members, and physical newlines."""
    root.mkdir()
    rows = []
    for index, sample in enumerate(('alpha', 'beta', 'empty')):
        directory = root / str(index)
        directory.mkdir()
        paths = []
        for role in (1, 2):
            canonical = expected[sample, role]
            if sample == 'alpha' and mutation == 'role_swap':
                canonical = expected[sample, 3 - role]
            if sample == 'alpha' and mutation == 'paired_count':
                canonical = b'\n'.join(canonical.split(b'\n')[4:])
            if sample == 'alpha' and mutation == 'order':
                lines = canonical.split(b'\n')
                canonical = b'\n'.join(lines[4:8] + lines[:4] + lines[8:])
            if sample == 'alpha' and mutation == 'header':
                canonical = canonical.replace(b'@repeat/', b'@change/', 1)
            if sample == 'alpha' and role == 2:
                if mutation == 'metadata':
                    canonical = canonical.replace(b'private', b'changed', 1)
                elif mutation == 'sequence':
                    canonical = canonical.replace(b'AC\x00', b'TC\x00', 1)
                elif mutation == 'quality':
                    canonical = canonical.replace(b'!', b'#', 1)
                elif mutation == 'count':
                    canonical = b'\n'.join(canonical.split(b'\n')[4:])
            # Ingress strips one CR from CRLF; preserve a payload CR by adding
            # another CR. Repeated original identifiers do not collapse ordinals.
            physical = canonical.replace(b'\n', b'\r\n')
            name = f'reads{role}.fq' + ('.gz' if role == 2 else '')
            if role == 2:
                physical = gzip.compress(physical, compresslevel=1, mtime=12345)
            (directory / name).write_bytes(physical)
            paths.append(f'{index}/{name}')
        identity = 'renamed' if mutation == 'identity' and sample == 'beta' else sample
        if not (mutation == 'missing_sample' and sample == 'empty'):
            rows.append(f'{identity}\t{paths[0]}\t{paths[1]}\n')
    sheet = root / 'PRIVATE_sheet.tsv'
    sheet.write_text('sample_id\tr1\tr2\n' + ''.join(reversed(rows)))
    if mutation == 'missing_file':
        (root / '0/reads1.fq').unlink()
    return sheet


@pytest.mark.parametrize('mutation,error,reason', [
    (None, None, None),
    ('identity', IntegrityError, 'identities differ'),
    ('missing_sample', IntegrityError, 'catalog differs'),
    ('role_swap', InputError, 'mate header role'),
    ('paired_count', IntegrityError, 'original pair count differs'),
    ('order', IntegrityError, 'normalized originals differ'),
    ('header', IntegrityError, 'normalized originals differ'),
    ('metadata', IntegrityError, 'normalized originals differ'),
    ('sequence', IntegrityError, 'normalized originals differ'),
    ('quality', IntegrityError, 'normalized originals differ'),
    ('count', InputError, 'different ordinals'),
    ('missing_file', InputError, 'input'),
])
def test_P15_T05_relocated_originals_require_exact_normalized_sample_roles_and_content(
        tmp_path, monkeypatch, mutation, error, reason):
    source, expected, _, manifest = mix_fixture(tmp_path)
    sheet = relocated_sheet(tmp_path / 'PRIVATE_relocated', expected, mutation)
    (source / 'foreign.keep').write_bytes(b'PRIVATE foreign content')
    before = snapshot(tmp_path)
    budgets, witnesses = [], []
    resources = verification.operation_resources
    compare = verification._compare_originals

    def observe_resources(*args, **kwargs):
        plan, budget = resources(*args, **kwargs)
        budgets.append(budget)
        return plan, budget

    def observe_compare(*args, **kwargs):
        # This function is entered only after complete pool verification,
        # original-domain coverage and independent global tag uniqueness.
        witnesses.append('verified-pool-before-originals')
        return compare(*args, **kwargs)

    monkeypatch.setattr(verification, 'operation_resources', observe_resources)
    monkeypatch.setattr(verification, '_compare_originals', observe_compare)
    config = VerifyConfig(manifest=manifest, originals=sheet, workers=1, max_ram=512 << 20)
    if error is None:
        report = verification.verify(config)
        assert report.completed and report.validation_scope == 'pool_and_originals'
        assert report.pairs == 14
        del report
    else:
        with pytest.raises(error, match=reason) as caught:
            verification.verify(config)
        assert caught.value.__traceback__ is not None
        diagnostics = str(caught.value) + str(getattr(caught.value, '__notes__', ()))
        assert 'PRIVATE' not in diagnostics and str(tmp_path) not in diagnostics
    assert witnesses == ['verified-pool-before-originals']
    assert snapshot(tmp_path) == before
    assert len(budgets) == 1
    budget = budgets[0]
    assert budget.active_leases == budget.used_descriptors == budget.used_metadata_bytes == 0
    assert budget.used_bytes == budget.baseline_bytes


@pytest.mark.parametrize('mutation', [None, 'metadata', 'identity', 'role_swap', 'missing_file'])
def test_P15_T08_real_command_never_downgrades_requested_originals_scope(tmp_path, mutation):
    _, expected, _, manifest = mix_fixture(tmp_path)
    sheet = relocated_sheet(tmp_path / 'PRIVATE_relocated', expected, mutation)
    before = snapshot(tmp_path)
    result = cli('verify', '--manifest', manifest, '--originals', sheet,
                 '--workers', '1', '--max-ram', '512MiB', '--json', cwd=tmp_path)
    assert result.returncode == (0 if mutation is None else 1), result.stderr
    if mutation is None:
        assert json.loads(result.stdout)['validation_scope'] == 'pool_and_originals'
        assert not result.stderr
    else:
        assert not result.stdout
        assert 'Error:' in result.stderr
    for private in ('PRIVATE', 'alpha', 'beta', 'opaque', 'private', str(tmp_path)):
        assert private not in result.stdout + result.stderr
    assert snapshot(tmp_path) == before
