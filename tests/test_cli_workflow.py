"""Actual command entrypoints for the complete paired workflow."""

import json

import pytest

from readmixer_py.errors import CancelledError
from test_api import cli
from test_pipeline import fixtures


def test_API_public_mix_verify_both_recovery_modes_and_redacted_json(tmp_path):
    inputs = tmp_path / 'PRIVATE_INPUT'
    fixtures(inputs)
    output = tmp_path / 'pool'
    mixed = cli('mix', '--input', inputs, '--output', output, '--max-ram', '512MiB', '--workers', '1', '--json')
    assert mixed.returncode == 0, mixed.stderr
    mix_report = json.loads(mixed.stdout)
    assert mix_report['pairs'] == 14 and mix_report['compression_slots'] == 0
    manifest = output / mix_report['manifest']
    verified = cli('verify', '--manifest', manifest, '--max-ram', '512MiB', '--json')
    assert verified.returncode == 0, verified.stderr
    assert json.loads(verified.stdout)['validation_scope'] == 'pool_complete'
    results = [mixed, verified]
    for mode, scope in [('indexed', 'selected_records'), ('sequential', 'complete_bundle_streams')]:
        recovered = cli('recover', '--manifest', manifest, '--output', tmp_path / mode,
                        '--sample', 'beta', '--sample', 'alpha', '--io', mode, '--max-ram', '512MiB',
                        '--workers', '1', '--json')
        assert recovered.returncode == 0, recovered.stderr
        report = json.loads(recovered.stdout)
        assert report['pairs'] == 14 and report['validation_scope'] == scope
        assert report['index_validation'] == ('validated' if mode == 'indexed' else 'explicitly_bypassed')
        assert 'outputs' not in report
        assert (tmp_path / mode / report['receipt']).is_file()
        results.append(recovered)
    for result in results:
        assert not result.stderr
        for secret in ('PRIVATE_INPUT', 'alpha', 'beta', 'opaque', '+private', str(tmp_path)):
            assert secret not in result.stdout


@pytest.mark.parametrize('signal,expected', [(2, 130), (15, 143)])
def test_API_postcommit_cancellation_identifies_generated_marker(tmp_path, monkeypatch, capsys, signal, expected):
    import readmixer_py.cli as command
    marker = 'pool.' + 'a' * 32 + '.manifest.json'
    def committed(config):
        error = CancelledError(signal=signal)
        error.add_note('outputs are durably committed; completion marker: ' + marker)
        error.add_note('PRIVATE_DIAGNOSTIC')
        raise error
    monkeypatch.setattr(command, 'mix', committed)
    result = command.main(['mix', '--input', str(tmp_path), '--output', str(tmp_path / 'out'), '--json'])
    assert result == expected
    output = capsys.readouterr()
    assert not output.out and marker in output.err and 'PRIVATE_DIAGNOSTIC' not in output.err


def test_API_repeated_library_imports_retain_callable_public_operations(tmp_path):
    import readmixer_py
    from readmixer_py.mixing import MixReport
    from readmixer_py.verification import VerifyReport
    from readmixer_py.recovery import RecoverReport
    for operation in ('mix', 'verify', 'recover'):
        assert callable(getattr(readmixer_py, operation))
    assert readmixer_py.MixReport is MixReport
    assert readmixer_py.VerifyReport is VerifyReport
    assert readmixer_py.RecoverReport is RecoverReport


@pytest.mark.parametrize('kind,expected', [('keyboard', 130), ('term', 143)])
def test_API_interrupted_rendering_retains_real_committed_pool(tmp_path, monkeypatch, capsys, kind, expected):
    import signal
    import readmixer_py.cli as command
    from readmixer_py import verify, VerifyConfig

    inputs = tmp_path / 'inputs'
    fixtures(inputs)
    output = tmp_path / 'pool'
    original_mix = command.mix
    markers = []

    def interrupted_render(*args, **kwargs):
        if kind == 'term':
            handler = signal.getsignal(signal.SIGTERM)
            assert callable(handler), 'CLI must retain its signal handler while rendering'
            handler(signal.SIGTERM, None)
        raise KeyboardInterrupt

    def commit_then_interrupt(config):
        result = original_mix(config)
        markers.append(result.manifest)
        monkeypatch.setattr(command.json, 'dumps', interrupted_render)
        return result

    with monkeypatch.context() as patch:
        patch.setattr(command, 'mix', commit_then_interrupt)
        result = command.main(['mix', '--input', str(inputs), '--output', str(output),
                               '--max-ram', '512MiB', '--workers', '1', '--json'])
    # Restore the shared json module before invoking the independent verifier.
    monkeypatch.undo()
    captured = capsys.readouterr()
    assert result == expected
    assert not captured.out and markers[0] in captured.err
    assert str(tmp_path) not in captured.err
    manifest = output / markers[0]
    assert manifest.is_file()
    assert verify(VerifyConfig(manifest=manifest, max_ram=512 << 20)).completed
