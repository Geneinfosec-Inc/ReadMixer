"""Pure fake-process controls. No candidate import, child process or signal."""
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

HERE = Path(__file__).parents[1]
spec = importlib.util.spec_from_file_location('release_resource_ownership', HERE / 'resource_lane.py')
lane = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lane)
_REAL_WAIT = lane._portable_wait


class Clock:
    def __init__(self): self.value = 100.0
    def now(self): self.value += 0.1; return self.value
    def sleep(self, seconds): self.value += seconds


class Child:
    pid = 32123
    def __init__(self, waits): self.waits = iter(waits); self.returncode = None
    def wait(self, timeout=None):
        outcome = next(self.waits)
        if isinstance(outcome, BaseException): raise outcome
        self.returncode = outcome
        return outcome


class Controls(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='readmixer-portable-pure-controls-', dir=Path(tempfile.gettempdir()).resolve())
        self.addCleanup(self.temp.cleanup)
        self.prefix = Path(self.temp.name) / 'observed-launch'

    @contextlib.contextmanager
    def fake(self, waits=(0,), signals=None, birth=None):
        child, clock = Child(waits), Clock()
        if signals is None: signals = [ProcessLookupError(3, 'fixture ESRCH')]
        with contextlib.ExitStack() as stack:
            mocks = {}
            for name, value in {
                'subprocess.Popen': dict(return_value=child),
                '_portable_wait': dict(side_effect=lambda process, timeout, journal: process.wait(timeout=timeout)),
                'os.killpg': dict(side_effect=signals),
                'os.getpgid': dict(return_value=child.pid),
                'os.getsid': dict(return_value=child.pid),
                'os.getpgrp': dict(return_value=777),
                'os.getuid': dict(return_value=501),
                'os.waitpid': dict(return_value=(0, 0)),
                'signal.getsignal': dict(return_value=signal.SIG_DFL),
                '_portable_birth': dict(return_value={'source': 'DATA_ONLY', 'birth_text_utc': 'fixture'}),
                '_portable_numeric_group': dict(return_value={'matching_members': [], 'source': 'DATA_ONLY'}),
                'time.monotonic': dict(side_effect=clock.now),
                'time.sleep': dict(side_effect=clock.sleep),
            }.items():
                target = lane
                parts = name.split('.')
                for part in parts[:-1]: target = getattr(target, part)
                mocks[name] = stack.enter_context(patch.object(target, parts[-1], create=True, **value))
            yield child, mocks

    def launch(self):
        return lane.portable_launch(['DATA_ONLY_NOT_EXECUTED'], environment={},
            stdout=io.BytesIO(), stderr=io.BytesIO(), timeout=3, diagnostic_prefix=self.prefix)

    def raw(self): return json.loads(Path(str(self.prefix) + '.result.json').read_text())

    def test_success_literal_zero_absence_and_no_postreap_signal(self):
        with self.fake() as (child, mocks):
            result = self.launch()
        self.assertEqual(result['returncode'], 0)
        self.assertTrue(result['cleanup_verified'])
        self.assertEqual([call.args[1] for call in mocks['os.killpg'].call_args_list], [0])
        self.assertFalse(result['post_reap_signals_sent'])

    def test_nonzero_child_is_not_rewritten(self):
        with self.fake(waits=(7,)):
            self.assertEqual(self.launch()['returncode'], 7)

    def test_transient_eperm_requires_later_actual_esrch(self):
        with self.fake(signals=[PermissionError(1, 'fixture EPERM'), ProcessLookupError(3, 'fixture ESRCH')]) as (_, mocks):
            value = self.launch()
        self.assertTrue(value['cleanup_verified'])
        self.assertEqual([call.args[1] for call in mocks['os.killpg'].call_args_list], [0, 0])
        self.assertEqual(value['events'][-1]['proof'], 'killpg(group,0) raised ESRCH')

    def test_persistent_eperm_never_becomes_absence_or_child_failure(self):
        with self.fake(signals=PermissionError(1, 'fixture EPERM')) as (_, mocks):
            with self.assertRaises(lane.PortableCleanupError): self.launch()
        raw = self.raw()
        self.assertEqual(raw['returncode'], 0)
        self.assertFalse(raw['cleanup_verified'])
        self.assertEqual(raw['primary_exception']['phase'], 'post_wait_absence')
        self.assertTrue(all(call.args[1] == 0 for call in mocks['os.killpg'].call_args_list))

    def test_present_group_never_gets_postreap_signal(self):
        with self.fake(signals=lambda *_: None) as (_, mocks):
            with self.assertRaises(lane.PortableCleanupError): self.launch()
        self.assertTrue(all(call.args[1] == 0 for call in mocks['os.killpg'].call_args_list))
        self.assertEqual(self.raw()['returncode'], 0)

    def test_timeout_identity_preserved_and_signal_only_before_reap(self):
        original = subprocess.TimeoutExpired('DATA_ONLY', 3)
        with self.fake(waits=(original, -2), signals=[None, ProcessLookupError(3, 'fixture ESRCH')]) as (_, mocks):
            with self.assertRaises(subprocess.TimeoutExpired) as raised: self.launch()
        self.assertIs(raised.exception, original)
        self.assertEqual([call.args[1] for call in mocks['os.killpg'].call_args_list], [signal.SIGINT, 0])
        self.assertEqual(self.raw()['returncode'], -2)

    def test_signal_eperm_cannot_mask_original_timeout(self):
        original = subprocess.TimeoutExpired('DATA_ONLY', 3)
        with self.fake(waits=(original, -2), signals=[PermissionError(1, 'fixture'), ProcessLookupError(3, 'fixture')]):
            with self.assertRaises(subprocess.TimeoutExpired) as raised: self.launch()
        self.assertIs(raised.exception, original)
        self.assertTrue(any(e['phase'] == 'signal_error' for e in self.raw()['events']))

    def test_arbitrary_cleanup_oserror_cannot_mask_keyboardinterrupt(self):
        original = KeyboardInterrupt('DATA_ONLY first exception')
        with self.fake(waits=(original, -2), signals=[OSError(5, 'fixture'), ProcessLookupError(3, 'fixture')]):
            with self.assertRaises(KeyboardInterrupt) as raised: self.launch()
        self.assertIs(raised.exception, original)
        self.assertEqual(self.raw()['secondary_errors'][0]['phase'], 'ownership_or_signal_SIGINT')

    def test_cleanup_wait_interrupt_cannot_replace_first_exception(self):
        original = subprocess.TimeoutExpired('DATA_ONLY', 3)
        second = KeyboardInterrupt('DATA_ONLY secondary')
        with self.fake(waits=(original, second, -15), signals=[None, None, ProcessLookupError(3, 'fixture')]):
            with self.assertRaises(subprocess.TimeoutExpired) as raised: self.launch()
        self.assertIs(raised.exception, original)
        self.assertEqual(self.raw()['returncode'], -15)
        self.assertTrue(any(e['phase'] == 'cleanup_wait' for e in self.raw()['secondary_errors']))

    def test_popen_failure_is_literal_and_durable(self):
        original = OSError(12, 'DATA_ONLY Popen failure')
        with self.fake() as (_, mocks):
            mocks['subprocess.Popen'].side_effect = original
            with self.assertRaises(OSError) as raised: self.launch()
        self.assertIs(raised.exception, original)
        self.assertIsNone(self.raw()['returncode'])
        self.assertEqual(self.raw()['primary_exception']['phase'], 'Popen')
        mocks['os.killpg'].assert_not_called()

    def test_final_receipt_failure_keeps_first_timeout_identity(self):
        original = subprocess.TimeoutExpired('DATA_ONLY', 3)
        with self.fake(waits=(original, -2), signals=[None, ProcessLookupError(3, 'fixture')]):
            with patch.object(lane._PortableJournal, 'finish', side_effect=OSError(5, 'DATA_ONLY write')):
                with self.assertRaises(subprocess.TimeoutExpired) as raised: self.launch()
        self.assertIs(raised.exception, original)
        self.assertTrue(Path(str(self.prefix) + '.request.json').exists())
        self.assertTrue(Path(str(self.prefix) + '.events.jsonl').read_text())

    def test_final_receipt_failure_blocks_normal_success(self):
        original = OSError(5, 'DATA_ONLY write')
        with self.fake():
            with patch.object(lane._PortableJournal, 'finish', side_effect=original):
                with self.assertRaises(OSError) as raised: self.launch()
        self.assertIs(raised.exception, original)

    def test_reaped_signal_refusal_precedes_any_identity_or_signal_call(self):
        with self.fake() as (child, mocks):
            child.returncode = 0
            journal = lane._PortableJournal(None, {})
            self.assertFalse(lane._portable_signal_unreaped(child, signal.SIGKILL, journal)['signal_sent'])
        mocks['os.getpgid'].assert_not_called()
        mocks['os.killpg'].assert_not_called()

    def test_unreaped_wrong_session_never_signaled(self):
        original = subprocess.TimeoutExpired('DATA_ONLY', 3)
        with self.fake(waits=(original, -2), signals=[ProcessLookupError(3, 'fixture')]) as (_, mocks):
            mocks['os.getsid'].side_effect = [32123, 123]
            with self.assertRaises(subprocess.TimeoutExpired) as raised: self.launch()
        self.assertIs(raised.exception, original)
        self.assertEqual([call.args[1] for call in mocks['os.killpg'].call_args_list], [])

    def test_denied_numeric_census_does_not_override_persistent_eperm(self):
        with self.fake(signals=PermissionError(1, 'fixture')) as (_, mocks):
            mocks['_portable_numeric_group'].side_effect = PermissionError(1, 'DATA_ONLY census denied')
            with self.assertRaises(lane.PortableCleanupError): self.launch()
        self.assertFalse(self.raw()['cleanup_verified'])
        self.assertTrue(any(e['phase'] == 'numeric_group_readback_failed' for e in self.raw()['events']))

    def test_private_exact_diagnostics_and_exclusive_request(self):
        with self.fake(): self.launch()
        evidence = lane.portable_launch_evidence(self.prefix)
        self.assertEqual(len(evidence['files']), 3)
        self.assertEqual(evidence['observed_returncode'], 0)
        self.assertTrue(evidence['cleanup_verified'])
        for ref in evidence['files'].values():
            path = Path(ref['path'])
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(lane.fingerprint(path), {k: ref[k] for k in ('bytes', 'sha256')})
        old = {p.name: p.read_bytes() for p in self.prefix.parent.iterdir()}
        with self.fake() as (_, mocks):
            with self.assertRaises(FileExistsError): self.launch()
        mocks['subprocess.Popen'].assert_not_called()
        self.assertEqual(old, {p.name: p.read_bytes() for p in self.prefix.parent.iterdir()})

    def test_symlink_ancestor_refused_before_spawn(self):
        real = self.prefix.parent / 'real'
        real.mkdir()
        link = self.prefix.parent / 'alias'
        link.symlink_to(real, target_is_directory=True)
        self.prefix = link / 'observed'
        with self.fake() as (_, mocks):
            with self.assertRaises(ValueError): self.launch()
        mocks['subprocess.Popen'].assert_not_called()
        self.assertEqual(list(real.iterdir()), [])

    def test_unavailable_birth_is_explicit_not_invented_or_signal_authority(self):
        with self.fake() as (_, mocks):
            mocks['_portable_birth'].side_effect = ProcessLookupError(3, 'fixture fast child')
            result = self.launch()
        self.assertEqual(result['native_identity']['pid'], 32123)
        self.assertNotIn('birth', result['native_identity'])
        self.assertEqual(result['native_identity']['birth_error']['error_type'], 'ProcessLookupError')
        self.assertTrue(result['cleanup_verified'])

    def test_positive_wrong_initial_session_blocks_acceptance(self):
        with self.fake(waits=(0,), signals=[ProcessLookupError(3, 'fixture')]) as (_, mocks):
            mocks['os.getsid'].return_value = 123
            with self.assertRaises(lane.PortableCleanupError): self.launch()
        raw = self.raw()
        self.assertEqual(raw['primary_exception']['phase'], 'native_identity')
        self.assertEqual(raw['native_identity']['sid'], 123)
        self.assertTrue(any(e['phase'] == 'native_identity_mismatch' for e in raw['events']))
        self.assertTrue(raw['primary_traceback_private'])
        self.assertTrue(all(call.args[1] == 0 for call in mocks['os.killpg'].call_args_list))

    def test_interrupted_wait_reaped_pid_reuse_never_authorizes_signal(self):
        original = KeyboardInterrupt('DATA_ONLY waitpid/store gap')
        with self.fake(waits=(original, ChildProcessError('DATA_ONLY already reaped'),
                             ChildProcessError('DATA_ONLY already reaped'),
                             ChildProcessError('DATA_ONLY already reaped')),
                       signals=PermissionError(1, 'DATA_ONLY reused group')) as (_, mocks):
            mocks['os.waitpid'].side_effect = ChildProcessError('DATA_ONLY ECHILD; numeric PID reused')
            with self.assertRaises(KeyboardInterrupt) as raised: self.launch()
        self.assertIs(raised.exception, original)
        self.assertTrue(all(call.args[1] == 0 for call in mocks['os.killpg'].call_args_list))
        self.assertIsNone(self.raw()['coordinator_reaped'])
        self.assertFalse(self.raw()['cleanup_verified'])

    def test_custom_sigchld_disposition_refuses_escalation(self):
        original = subprocess.TimeoutExpired('DATA_ONLY', 3)
        with self.fake(waits=(original, -2), signals=[ProcessLookupError(3, 'fixture')]) as (_, mocks):
            mocks['signal.getsignal'].side_effect = [signal.SIG_DFL, signal.SIG_IGN]
            with self.assertRaises(subprocess.TimeoutExpired) as raised: self.launch()
        self.assertIs(raised.exception, original)
        mocks['os.waitpid'].assert_not_called()
        self.assertEqual([call.args[1] for call in mocks['os.killpg'].call_args_list], [])

    def test_partial_final_evidence_keeps_fingerprint_and_stops_next_case(self):
        original = subprocess.TimeoutExpired('DATA_ONLY', 3)
        with self.fake(waits=(original, -2), signals=[None, ProcessLookupError(3, 'fixture')]):
            with self.assertRaises(subprocess.TimeoutExpired) as raised: self.launch()
        self.assertIs(raised.exception, original)
        path = Path(str(self.prefix) + '.result.json')
        path.write_bytes(b'{"DATA_ONLY_PARTIAL":')
        evidence = lane.portable_launch_evidence(self.prefix)
        self.assertEqual(evidence['files']['result.json']['bytes'], path.stat().st_size)
        self.assertEqual(evidence['read_errors'][0]['error_type'], 'JSONDecodeError')
        self.assertTrue(lane.portable_launch_requires_hold(evidence))
        # Caller custody is additive; a prior exception type is never replaced.
        row = dict(error_type=type(original).__name__, status='failed')
        row['portable_launch_evidence'] = evidence
        if lane.portable_launch_requires_hold(evidence):
            row.update(status='failed', halted_for_unresolved_launch_cleanup=True)
        self.assertEqual(row['error_type'], 'TimeoutExpired')
        self.assertTrue(row['halted_for_unresolved_launch_cleanup'])

    def test_raw_waitpid_reap_records_literal_status_without_signal(self):
        original = subprocess.TimeoutExpired('DATA_ONLY', 3)
        with self.fake(waits=(original,), signals=[ProcessLookupError(3, 'fixture')]) as (_, mocks):
            mocks['os.waitpid'].return_value = (32123, 7 << 8)
            with self.assertRaises(subprocess.TimeoutExpired) as raised: self.launch()
        self.assertIs(raised.exception, original)
        self.assertEqual(self.raw()['returncode'], 7)
        self.assertEqual(self.raw()['literal_status_source'], 'waitpid(pid,WNOHANG)')
        self.assertEqual([call.args[1] for call in mocks['os.killpg'].call_args_list], [0])

    def test_echild_cannot_trigger_popen_synthetic_zero_wait(self):
        original = KeyboardInterrupt('DATA_ONLY destructive wait return/store gap')
        with self.fake(waits=(original, 0), signals=PermissionError(1, 'fixture')) as (child, mocks):
            child.wait = Mock(wraps=child.wait)
            mocks['os.waitpid'].side_effect = ChildProcessError(10, 'DATA_ONLY ECHILD')
            with self.assertRaises(KeyboardInterrupt) as raised: self.launch()
        self.assertIs(raised.exception, original)
        self.assertEqual(child.wait.call_count, 1)
        self.assertIsNone(self.raw()['returncode'])
        self.assertIsNone(self.raw()['coordinator_reaped'])
        mocks['os.killpg'].assert_not_called()

    def test_non_esrch_processlookup_cannot_claim_absence(self):
        with self.fake(signals=ProcessLookupError(5, 'DATA_ONLY wrong errno')):
            with self.assertRaises(ProcessLookupError): self.launch()
        self.assertEqual(self.raw()['returncode'], 0)
        self.assertFalse(self.raw()['cleanup_verified'])

    def test_non_eperm_permissionerror_is_not_permission_probe(self):
        with self.fake(signals=PermissionError(13, 'DATA_ONLY EACCES')):
            with self.assertRaises(PermissionError): self.launch()
        self.assertEqual(self.raw()['primary_exception']['errno'], 13)
        self.assertFalse(self.raw()['cleanup_verified'])

    def test_traceback_format_failure_does_not_mask_primary(self):
        original = subprocess.TimeoutExpired('DATA_ONLY', 3)
        import traceback
        with self.fake(waits=(original, -2), signals=[None, ProcessLookupError(3, 'fixture')]):
            with patch.object(traceback, 'format_exception', side_effect=OSError(5, 'DATA_ONLY formatter')):
                with self.assertRaises(subprocess.TimeoutExpired) as raised: self.launch()
        self.assertIs(raised.exception, original)
        self.assertEqual(self.raw()['returncode'], -2)
        self.assertTrue(any(e['phase'] == 'primary_traceback_format' for e in self.raw()['secondary_errors']))

    def test_nonobject_final_evidence_retains_error_and_hold(self):
        with self.fake(): self.launch()
        path = Path(str(self.prefix) + '.result.json')
        for value in ([], None, 7, 'DATA_ONLY'):
            with self.subTest(value=value):
                path.write_text(json.dumps(value))
                evidence = lane.portable_launch_evidence(self.prefix)
                self.assertEqual(evidence['files']['result.json']['bytes'], path.stat().st_size)
                self.assertEqual(evidence['read_errors'][0]['error_type'], 'ValueError')
                self.assertTrue(lane.portable_launch_requires_hold(evidence))

    def test_evidence_formatter_failure_cannot_mask_read_error(self):
        with self.fake(): self.launch()
        Path(str(self.prefix) + '.result.json').write_bytes(b'{"DATA_ONLY_PARTIAL":')
        import traceback
        with patch.object(traceback, 'format_exception', side_effect=OSError(5, 'DATA_ONLY formatter')):
            evidence = lane.portable_launch_evidence(self.prefix)
        self.assertEqual(evidence['read_errors'][0]['error_type'], 'JSONDecodeError')
        self.assertEqual(evidence['read_errors'][0]['traceback_format_error']['error_type'], 'OSError')
        self.assertTrue(lane.portable_launch_requires_hold(evidence))

    def test_normal_sigchld_ignore_refused_before_any_spawn(self):
        with self.fake() as (_, mocks):
            mocks['signal.getsignal'].return_value = signal.SIG_IGN
            with self.assertRaises(lane.PortableCleanupError): self.launch()
        mocks['subprocess.Popen'].assert_not_called()
        mocks['os.waitpid'].assert_not_called()
        mocks['os.killpg'].assert_not_called()
        self.assertEqual(self.raw()['primary_exception']['phase'], 'prelaunch_contract')
        self.assertIsNone(self.raw()['returncode'])

    def test_public_wait_decodes_actual_raw_exit_and_signal_status(self):
        for status, expected in ((0, 0), (7 << 8, 7), (2, -2)):
            with self.subTest(status=status):
                with self.fake() as (child, mocks):
                    mocks['os.waitpid'].return_value = (32123, status)
                    result = _REAL_WAIT(child, 1, lane._PortableJournal(None, {}))
                self.assertEqual(result, expected)
                self.assertEqual(child.returncode, expected)

    def test_public_wait_echild_never_turns_into_zero(self):
        with self.fake() as (child, mocks):
            mocks['os.waitpid'].side_effect = ChildProcessError(10, 'DATA_ONLY ECHILD')
            with self.assertRaises(ChildProcessError): _REAL_WAIT(child, 1, lane._PortableJournal(None, {}))
        self.assertIsNone(child.returncode)

    def test_public_wait_timeout_retains_unreaped_unknown_status(self):
        with self.fake() as (child, mocks):
            mocks['os.waitpid'].return_value = (0, 0)
            with self.assertRaises(subprocess.TimeoutExpired): _REAL_WAIT(child, 0.2, lane._PortableJournal(None, {}))
        self.assertIsNone(child.returncode)

    def test_normal_echild_stops_without_another_wait_or_signal(self):
        original = ChildProcessError(10, 'DATA_ONLY ECHILD')
        with self.fake(waits=(original, 0)) as (child, mocks):
            child.wait = Mock(wraps=child.wait)
            with self.assertRaises(ChildProcessError) as raised: self.launch()
        self.assertIs(raised.exception, original)
        self.assertEqual(child.wait.call_count, 1)
        mocks['os.waitpid'].assert_not_called()
        mocks['os.killpg'].assert_not_called()
        self.assertIsNone(self.raw()['returncode'])

    def test_numeric_parser_only_retains_matching_numeric_group(self):
        completed = subprocess.CompletedProcess([], 0, b' 100 200 501\n 101 32123 501\n', b'')
        with patch.object(lane.subprocess, 'run', return_value=completed) as run:
            result = lane._portable_numeric_group(32123)
        self.assertEqual(result['matching_members'], [dict(pid=101, pgid=32123, uid=501)])
        self.assertEqual(result['complete_numeric_rows'], 2)
        self.assertEqual(run.call_args.kwargs['timeout'], 2)
        self.assertNotIn('args', result)

    def test_invalid_numeric_parser_does_not_infer_empty_group(self):
        for output in (b'101 32123 name\n', b'101 32123 501\n101 32123 501\n'):
            with self.subTest(output=output):
                completed = subprocess.CompletedProcess([], 0, output, b'')
                with patch.object(lane.subprocess, 'run', return_value=completed):
                    with self.assertRaises(ValueError): lane._portable_numeric_group(32123)

    def test_optional_birth_ps_precision_is_declared_and_pid_matched(self):
        completed = subprocess.CompletedProcess([], 0, b'32123 Tue Oct 20 05:30:00 2026\n', b'')
        with patch.object(lane.subprocess, 'run', return_value=completed):
            result = lane._portable_birth(32123)
        self.assertIn('insufficient', result['precision'])
        self.assertEqual(result['source'], 'ps lstart')
        with patch.object(lane.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, b'999 Tue Oct 20 05:30:00 2026\n', b'')):
            with self.assertRaises(ValueError): lane._portable_birth(32123)
