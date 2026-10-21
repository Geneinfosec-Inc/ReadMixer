"""Plan 14 one-pass producer ownership and exact normalized admission."""
from dataclasses import replace
import hashlib
import os
import struct

import psutil
import pytest

from readmixer_py.catalog import build_catalog
from readmixer_py.errors import ConfigurationError, InputError, IntegrityError, ResourceError
from readmixer_py.execution import CpuObservations, resolve_cpu_budget
from readmixer_py.parser_stage import (encode_source, decode_source, encode_report, decode_report,
                                      lpt_assignments, SourceReceiver, IngressCredits)
from readmixer_py.resources import Budget
from readmixer_py.stage_scheduler import StageScheduler
from readmixer_py.worker_protocol import (WorkerJob, ParserJob, ParserBatch, ParserDone,
                                         encode_message, decode_message)

RUN = bytes(range(16))


def inputs(root, counts=(70, 13, 0)):
    root.mkdir()
    for index, count in enumerate(counts):
        for role in (1, 2):
            (root / f's{index}_R{role}.fq').write_bytes(b''.join(
                f'@pair{j}/{role}\r\nACGT\r\n+\r\n!!!!\r\n'.encode() for j in range(count)))


def test_P14_T01_source_capabilities_are_closed_and_private(tmp_path):
    inputs(tmp_path / 'inputs')
    with build_catalog(input_dir=tmp_path / 'inputs') as catalog:
        encoded = encode_source(catalog, catalog.samples[0], 128, 1 << 20)
        cap = decode_source(encoded)
        assert cap.sample == 0 and cap.r1.snapshot == catalog.samples[0].r1.snapshot
        assert str(tmp_path) not in repr(cap)
        job = ParserJob(WorkerJob(RUN, 0, 1, 0, 1 << 20, hashlib.sha256(b'').digest()), encoded)
        assert decode_message(encode_message(job)) == job
        assert str(tmp_path) not in repr(job)
        for bad in (b'', encoded[:-1], encoded + b'\0', b'\xff' + encoded[1:]):
            with pytest.raises(IntegrityError):
                decode_source(bad)


def test_P14_T01_lpt_is_size_descending_with_canonical_ties(tmp_path):
    inputs(tmp_path / 'inputs', (3, 3, 2, 1, 0))
    with build_catalog(input_dir=tmp_path / 'inputs') as catalog:
        assert lpt_assignments(catalog, 2) == ((0, 2), (1, 3, 4))
        assert lpt_assignments(catalog, 1) == ((0, 1, 2, 3, 4),)


@pytest.mark.parametrize('maximum', [None, 2, 1])
def test_P14_T01_normalized_credit_is_exact_without_idle_reservations(maximum):
    credits = IngressCredits(maximum)
    credits.admit(1)
    if maximum == 1:
        with pytest.raises(ResourceError, match='max_total_uncompressed'):
            credits.admit(1)
        assert credits.used == 1
    else:
        credits.admit(1)
        assert credits.used == 2
    credits.used = (1 << 64) - 1
    with pytest.raises(ResourceError):
        credits.admit(1)


@pytest.mark.parametrize('limit', [None, 3060, 3059])
def test_P14_T01_spawn_source_stream_reports_and_quota(tmp_path, limit):
    inputs(tmp_path / 'inputs')
    budget = Budget(768 << 20)
    accepted = []
    with build_catalog(input_dir=tmp_path / 'inputs', budget=budget) as catalog:
        with StageScheduler(budget, resolve_cpu_budget(observations=CpuObservations(2)), RUN,
                operation='mix', task_count=3, parser_record_bytes=128, production=True) as stage:
            assert stage.process_slots == 1
            pids = stage.child_pids
            credits = IngressCredits(limit)
            encoded = encode_source(catalog, catalog.samples[0], 128, stage.parser_batch_bytes)
            receiver = SourceReceiver(decode_source(encoded), lambda frame: accepted.append(bytes(frame)), credits)
            stage.submit_parser(encoded, receiver, producer=0)
            # Compute normalized independent expected lengths, not frame lengths.
            total = sum(2 * len(f'@pair{j}/1\nACGT\n+\n!!!!\n'.encode()) for j in range(70))
            if limit is not None and limit < total:
                with pytest.raises(ResourceError, match='max_total_uncompressed'):
                    stage.take_next()
                assert not accepted
            else:
                with stage.take_next() as result:
                    report = decode_report(result.data)
                    assert report.sample_index == 0 and report.pair_count == 70
                    assert report.r1_records == report.r2_records == 70
                    assert credits.used == total
                    assert len(accepted) == 70
                    assert encode_report(report) == result.data
    assert all(not psutil.pid_exists(pid) for pid in pids)
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0


def test_P14_T08_worker_rejects_changed_source_before_scatter(tmp_path):
    inputs(tmp_path / 'inputs')
    budget = Budget(768 << 20)
    with build_catalog(input_dir=tmp_path / 'inputs', budget=budget) as catalog:
        with StageScheduler(budget, resolve_cpu_budget(observations=CpuObservations(2)), RUN,
                parser_record_bytes=128, production=True) as stage:
            encoded = encode_source(catalog, catalog.samples[0], 128, stage.parser_batch_bytes)
            source = catalog.samples[0].r1.path
            source.write_bytes(source.read_bytes() + b'x')
            receiver = SourceReceiver(decode_source(encoded), lambda frame: pytest.fail('scatter reached'), IngressCredits())
            stage.submit_parser(encoded, receiver, producer=0)
            with pytest.raises(InputError, match='input worker failed'):
                stage.take_next()
    assert budget.used_bytes == 0
