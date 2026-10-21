from pathlib import Path
import hashlib,io,json,sys,tarfile,zipfile
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import candidate as c


def artifacts(tmp_path, extra=None, wheel_extra=None):
    source={'README.md':b'public documentation\n','src/readmixer_py/__init__.py':b'__version__="1.0"\n','pyproject.toml':b'[project]\nname="readmixer-py"\nversion="1.0"\n'}
    generated={'PKG-INFO':b'Metadata-Version: 2.1\nName: readmixer-py\nVersion: 1.0\n','setup.cfg':b'[egg_info]\n'}
    archive=tmp_path/'readmixer-py-1.0.tar.gz'
    with tarfile.open(archive,'w:gz') as t:
        for name,data in {**source,**generated}.items():
            m=tarfile.TarInfo('readmixer-py-1.0/'+name);m.size=len(data);t.addfile(m,io.BytesIO(data))
        if extra:
            name,kind=extra;m=tarfile.TarInfo(name);m.type=kind
            if kind in (tarfile.SYMTYPE,tarfile.LNKTYPE):m.linkname='../../foreign'
            t.addfile(m,io.BytesIO(b''))
    wheel=tmp_path/'readmixer_py-1.0-py3-none-any.whl'
    with zipfile.ZipFile(wheel,'w') as z:
        z.writestr('readmixer_py/__init__.py',source['src/readmixer_py/__init__.py'])
        z.writestr('readmixer_py-1.0.dist-info/METADATA',generated['PKG-INFO'])
        if wheel_extra:z.writestr(wheel_extra,b'excluded')
    records={k:{'bytes':len(v),'sha256':hashlib.sha256(v).hexdigest()} for k,v in source.items()}
    provenance=tmp_path/'provenance.json';provenance.write_text(json.dumps({'package_version':'1.0','source_manifest':records,'archives':{p.name:{'bytes':p.stat().st_size,'sha256':c.sha256_file(p)} for p in (wheel,archive)},'source_scope':'test selected source','base_commit':None,'dirty_tree_scope':[]}))
    return wheel,archive,provenance


def test_adoption_and_extraction_preserve_exact_archives_and_source(tmp_path):
    wheel,sdist,provenance=artifacts(tmp_path)
    record=c.adopt_candidate(wheel,sdist,provenance,tmp_path/'bundle',1)
    assert c.load_candidate(tmp_path/'bundle')==record
    root=c.safe_extract_sdist(tmp_path/'bundle',tmp_path/'stage')
    assert (root/'src/readmixer_py/__init__.py').read_bytes()==b'__version__="1.0"\n'
    assert (tmp_path/'bundle'/wheel.name).read_bytes()==wheel.read_bytes()
    assert len(record['source_manifest'])==3 and len(record['sdist_members'])==5
    assert (tmp_path/'bundle/candidate.json').stat().st_mode & 0o777==0o600
    assert (root/'README.md').stat().st_mode & 0o777==0o600


@pytest.mark.parametrize('name,kind',[
 ('../foreign',tarfile.REGTYPE),('/foreign',tarfile.REGTYPE),('root/a/../../foreign',tarfile.REGTYPE),
 ('root/readmixer/input/x.fastq',tarfile.REGTYPE),('root/link',tarfile.SYMTYPE),('root/hard',tarfile.LNKTYPE),
 ('readmixer-py-1.0/README.md',tarfile.REGTYPE),('second/README.md',tarfile.REGTYPE),('root/scripts/a\\b.py',tarfile.REGTYPE),
])
def test_hostile_sdist_rejected_before_destination_creation(tmp_path,name,kind):
    paths=artifacts(tmp_path,extra=(name,kind))
    with pytest.raises(ValueError):c.adopt_candidate(*paths,tmp_path/'bundle',1)
    assert not (tmp_path/'bundle').exists()


@pytest.mark.parametrize('name',['../escape','/escape','readmixer/input/read.fastq','readmixer_py/../secret','readmixer_py/a\\b.py'])
def test_hostile_wheel_rejected(tmp_path,name):
    paths=artifacts(tmp_path,wheel_extra=name)
    with pytest.raises(ValueError):c.adopt_candidate(*paths,tmp_path/'bundle',1)


def test_manifest_or_archive_tampering_never_inherits_candidate_identity(tmp_path):
    paths=artifacts(tmp_path);c.adopt_candidate(*paths,tmp_path/'bundle',1)
    p=tmp_path/'bundle/candidate.json';value=json.loads(p.read_text());value['expected_tests']=2;p.write_text(json.dumps(value))
    with pytest.raises(ValueError,match='identity'):c.load_candidate(p.parent)
    value['candidate_id']=c._identity(value);p.write_text(json.dumps(value))
    archive=p.parent/value['archives']['wheel']['name'];archive.write_bytes(archive.read_bytes()+b'changed')
    with pytest.raises(ValueError,match='changed'):c.load_candidate(p.parent)


def test_duplicate_json_keys_rejected(tmp_path):
    p=tmp_path/'duplicate.json';p.write_text('{"a":1,"a":2}')
    with pytest.raises(ValueError,match='duplicate'):c.read_json(p)


def test_incomplete_source_provenance_rejected(tmp_path):
    paths=artifacts(tmp_path);p=paths[2];value=json.loads(p.read_text());value['source_manifest'].pop('README.md');p.write_text(json.dumps(value))
    with pytest.raises(ValueError,match='manifest'):c.adopt_candidate(*paths,tmp_path/'bundle',1)


def test_existing_destination_preserves_foreign_files(tmp_path):
    paths=artifacts(tmp_path);dest=tmp_path/'bundle';dest.mkdir();(dest/'foreign').write_bytes(b'preserve')
    with pytest.raises(ValueError,match='new'):c.adopt_candidate(*paths,dest,1)
    assert (dest/'foreign').read_bytes()==b'preserve'
    c.adopt_candidate(*paths,tmp_path/'good',1)
    with pytest.raises(ValueError,match='new'):c.safe_extract_sdist(tmp_path/'good',dest)


def test_expansion_bound_checked_before_reading_member(tmp_path,monkeypatch):
    paths=artifacts(tmp_path);monkeypatch.setattr(c,'MAX_MEMBER',8)
    with pytest.raises(ValueError):c.adopt_candidate(*paths,tmp_path/'bundle',1)
