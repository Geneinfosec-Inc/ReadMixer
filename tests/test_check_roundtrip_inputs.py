"""Real public workflows on isolated prefixes, checked by an independent oracle."""
import gzip
import json
from pathlib import Path
import subprocess
import sys

import pytest

_SCRIPT = Path(__file__).resolve().parents[1] / 'scripts' / 'check_roundtrip_inputs.py'


def run_check(directory, *args):
    return subprocess.run([sys.executable,str(_SCRIPT),'--input',str(directory),*args],
                          capture_output=True,text=True,timeout=90,check=False)


def fixtures(directory, *, suffix=False, empty=False, final_cr=False):
    for sample in ('private-alpha','private-beta'):
        for role in (1,2):
            first=f'@private-read/{role} opaque'.encode()+b'\xff\r\nA\r\n+private-plus\xfe\r\n'+(b'\r' if final_cr else b'!\r\n')
            if suffix:first+=b'not a valid next FASTQ record\n'
            (directory/f'{sample}_R{role}.fastq.gz').write_bytes(gzip.compress(first))
    if empty:
        for role in (1,2):(directory/f'empty_R{role}.fastq').write_bytes(b'')


def test_prefix_roundtrip_uses_public_workflows_and_never_claims_full_input_validation(tmp_path):
    fixtures(tmp_path,suffix=True,empty=True)
    before={path.name:path.read_bytes() for path in tmp_path.iterdir()}
    result=run_check(tmp_path,'--pairs-per-sample','1','--workers','2')
    assert result.returncode==0,result.stdout+result.stderr
    report=json.loads(result.stdout)
    assert report['scope']=='bounded_prefix_mix_verify_recovery_check'
    assert report['full_input_validation'] is False
    assert report['samples']==3 and report['nonempty_prefixes']==2 and report['pairs']==2
    for flag in ('completed','prefix_pool_fully_verified','indexed_recovery_equal','sequential_recovery_equal',
                 'independent_gzip_hashes_and_counts_equal','private_temp_cleanup_complete'):
        assert report[flag] is True
    assert report['source_files_modified'] is False
    assert not result.stderr and 'private-' not in result.stdout and str(tmp_path) not in result.stdout
    assert {path.name:path.read_bytes() for path in tmp_path.iterdir()}==before
    invalid=run_check(tmp_path,'--pairs-per-sample','2')
    assert invalid.returncode==1
    assert json.loads(invalid.stdout)['completed'] is False
    assert 'private-' not in invalid.stdout+invalid.stderr


def test_source_final_lone_cr_survives_prefix_fixture_ingress_and_both_recoveries(tmp_path):
    fixtures(tmp_path,final_cr=True)
    result=run_check(tmp_path,'--workers','1')
    assert result.returncode==0,result.stdout+result.stderr
    report=json.loads(result.stdout)
    assert report['pairs']==2
    assert report['independent_gzip_hashes_and_counts_equal'] is True


@pytest.mark.parametrize('args',[('--pairs-per-sample','0'),('--pairs-per-sample','129'),('--workers','0')])
def test_invalid_bounds_are_redacted(tmp_path,args):
    result=run_check(tmp_path/'private-missing',*args)
    assert result.returncode!=0
    assert 'private-missing' not in result.stdout+result.stderr
    assert str(tmp_path) not in result.stdout+result.stderr
