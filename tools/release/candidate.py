#!/usr/bin/env python3
"""Immutable candidate transport. This tool never rebuilds an adopted archive."""
from __future__ import annotations
import argparse
import email
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import tarfile
import zipfile

SCHEMA = 'readmixer.release.candidate.v1'
CELLS = ('linux-x86_64-py311', 'linux-x86_64-py314', 'macos-arm64-py311', 'macos-arm64-py314')
MAX_MEMBER = 32 << 20
MAX_TOTAL = 128 << 20
MAX_MEMBERS = 4096
MAX_ARCHIVE = 64 << 20
ROOT_FILES = {'LICENSE','MANIFEST.in','README.md','TESTING.md','CHANGES.md','pyproject.toml','PKG-INFO','setup.cfg'}
PREFIXES = ('src/readmixer_py/','src/readmixer_py.egg-info/','tests/','scripts/','readmixer/python_update_plans/','.github/workflows/')
OTHER_FILES = {'readmixer/INSTALLATION.md','readmixer/process/__init__.py','readmixer/readmixer.py'}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode('ascii')


def sha256_file(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for part in iter(lambda: stream.read(1 << 20), b''):
            h.update(part)
    return h.hexdigest()


def _pairs(pairs):
    result = {}
    for key,value in pairs:
        if key in result:
            raise ValueError('duplicate JSON key')
        result[key] = value
    return result


def read_json(path, limit=4 << 20):
    path = Path(path)
    if not path.is_file() or path.is_symlink() or path.stat().st_size > limit:
        raise ValueError('invalid bounded JSON file')
    return json.loads(path.read_bytes(), object_pairs_hook=_pairs)


def _name(name):
    if not isinstance(name,str) or not name or len(name)>512 or '\\' in name or '\x00' in name:
        raise ValueError('unsafe archive path')
    p = PurePosixPath(name)
    if p.is_absolute() or any(x in ('','.','..') for x in name.split('/')) or any(ord(x)<32 for x in name):
        raise ValueError('unsafe archive path')
    return name


def _source_allowed(name):
    _name(name)
    if name in ROOT_FILES or name in OTHER_FILES or re.fullmatch(r'requirements[-a-z0-9]*\.lock',name):
        return True
    if not name.startswith(PREFIXES):
        return False
    # An explicit allowlist excludes supplied data, outputs, caches and credentials.
    if any(x in ('input','output','dist','build','__pycache__','.git','.venv') or x.startswith('.env') for x in name.split('/')):
        return False
    return name.endswith(('.py','.md','.json','.yml','.yaml','.txt','PKG-INFO'))


def archive_manifest(path, kind):
    path = Path(path)
    if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_ARCHIVE:
        raise ValueError('invalid candidate archive')
    result = {}; seen = set(); total = 0; roots = set()
    if kind == 'sdist':
        with tarfile.open(path,'r:gz') as archive:
            for member in archive:
                if len(seen) >= MAX_MEMBERS: raise ValueError('too many archive members')
                name = _name(member.name.rstrip('/') if member.isdir() else member.name)
                if name in seen: raise ValueError('duplicate archive member')
                seen.add(name); parts=name.split('/'); roots.add(parts[0])
                if member.isdir(): continue
                if not member.isfile() or len(parts)<2 or member.size > MAX_MEMBER:
                    raise ValueError('unsupported archive member')
                relative='/'.join(parts[1:])
                if not _source_allowed(relative): raise ValueError('source archive contains excluded content')
                total += member.size
                if total > MAX_TOTAL: raise ValueError('archive expansion exceeds bound')
                with archive.extractfile(member) as source:
                    data=source.read(MAX_MEMBER+1)
                if len(data)!=member.size: raise ValueError('archive size mismatch')
                result[relative]={'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()}
        if len(roots)!=1 or not result: raise ValueError('source archive must have one root')
    elif kind == 'wheel':
        with zipfile.ZipFile(path) as archive:
            for info in archive.infolist():
                if len(seen)>=MAX_MEMBERS: raise ValueError('too many archive members')
                name=_name(info.filename.rstrip('/') if info.is_dir() else info.filename)
                if name in seen: raise ValueError('duplicate archive member')
                seen.add(name)
                mode=info.external_attr>>16
                if stat.S_ISLNK(mode) or info.flag_bits & 1: raise ValueError('unsupported wheel member')
                if info.is_dir(): continue
                if not (name.startswith('readmixer_py/') or re.match(r'readmixer_py-[A-Za-z0-9._]+\.dist-info/',name)):
                    raise ValueError('wheel contains excluded content')
                if info.file_size>MAX_MEMBER: raise ValueError('oversize wheel member')
                total+=info.file_size
                if total>MAX_TOTAL: raise ValueError('archive expansion exceeds bound')
                data=archive.read(info)
                result[name]={'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()}
        if not result: raise ValueError('empty wheel')
    else: raise ValueError('unknown archive kind')
    return result


def _archive_version(path,kind):
    if kind=='wheel':
        with zipfile.ZipFile(path) as z:
            names=[n for n in z.namelist() if re.fullmatch(r'readmixer_py-[A-Za-z0-9._]+\.dist-info/METADATA',n)]
            if len(names)!=1: raise ValueError('wheel metadata missing or ambiguous')
            raw=z.read(names[0])
    else:
        with tarfile.open(path,'r:gz') as t:
            names=[m for m in t if m.isfile() and m.name.count('/')==1 and m.name.endswith('/PKG-INFO')]
            if len(names)!=1: raise ValueError('sdist metadata missing or ambiguous')
            with t.extractfile(names[0]) as f:raw=f.read(MAX_MEMBER+1)
    meta=email.message_from_bytes(raw)
    if meta['Name']!='readmixer-py' or not re.fullmatch(r'[0-9][A-Za-z0-9.!+_-]*',meta.get('Version','')):
        raise ValueError('candidate package identity invalid')
    return meta['Version']


def _identity(record):
    body={k:v for k,v in record.items() if k!='candidate_id'}
    return 'sha256:'+hashlib.sha256(canonical(body)).hexdigest()


def _record(path):
    return {'name':Path(path).name,'bytes':Path(path).stat().st_size,'sha256':sha256_file(path)}


def adopt_candidate(wheel,sdist,provenance,destination,expected_tests):
    wheel,sdist,destination=map(Path,(wheel,sdist,destination))
    if destination.exists() or destination.is_symlink(): raise ValueError('candidate destination must be new')
    if type(expected_tests) is not int or expected_tests<1: raise ValueError('expected test count must be positive')
    inputs={'wheel':wheel,'sdist':sdist}
    members={k:archive_manifest(p,k) for k,p in inputs.items()}
    versions={_archive_version(p,k) for k,p in inputs.items()}
    if len(versions)!=1: raise ValueError('wheel/sdist versions differ')
    for path,rec in members['wheel'].items():
        if path.startswith('readmixer_py/') and members['sdist'].get('src/'+path)!=rec:
            raise ValueError('wheel runtime differs from sdist source')
    source=read_json(provenance)
    original=source.get('source_manifest')
    if not isinstance(original,dict) or not original: raise ValueError('original complete source manifest required')
    if set(members['sdist'])-set(original) != {'PKG-INFO','setup.cfg'} or any(members['sdist'].get(p)!=r for p,r in original.items()):
        raise ValueError('original source manifest differs from frozen sdist')
    archives={k:_record(p) for k,p in inputs.items()}
    if source.get('package_version') != next(iter(versions)) or any(source.get('archives',{}).get(r['name'])!={x:y for x,y in r.items() if x!='name'} for r in archives.values()):
        raise ValueError('original archive provenance differs')
    value={'schema':SCHEMA,'version':next(iter(versions)), 'archives':archives,
        'source_manifest':original,'source_manifest_sha256':hashlib.sha256(canonical(original)).hexdigest(),
        'sdist_members':members['sdist'],'wheel_members':members['wheel'],
        'base_commit':source.get('base_commit'),'dirty_tree_scope':source.get('dirty_tree_scope',[]),
        'source_scope':source.get('source_scope'),'expected_tests':expected_tests,'required_cells':list(CELLS),
        'scope':'Exact prebuilt archives adopted without rebuilding; current checkout and external harness are separate identities.',
        'excluded_content':['private inputs','generated pools/recoveries','secrets','caches','unselected checkout files']}
    value['candidate_id']=_identity(value)
    destination.mkdir(mode=0o700,parents=True)
    for kind,path in inputs.items():
        target=destination/path.name
        with path.open('rb') as f,os.fdopen(os.open(target,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'wb') as out:
            shutil.copyfileobj(f,out);out.flush();os.fsync(out.fileno())
        if _record(target)!=archives[kind]: raise ValueError('archive changed while copying')
    with os.fdopen(os.open(destination/'candidate.json',os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'wb') as out:
        out.write(canonical(value)+b'\n');out.flush();os.fsync(out.fileno())
    load_candidate(destination)
    return value


def load_candidate(bundle):
    bundle=Path(bundle)
    value=read_json(bundle/'candidate.json')
    if value.get('schema')!=SCHEMA or value.get('candidate_id')!=_identity(value):
        raise ValueError('candidate manifest identity mismatch')
    if type(value.get('expected_tests')) is not int or value['expected_tests']<1 or value.get('required_cells')!=list(CELLS):
        raise ValueError('candidate acceptance contract missing')
    if set(value.get('archives',{}))!={'wheel','sdist'}: raise ValueError('candidate archives missing')
    for kind,r in value['archives'].items():
        if not isinstance(r,dict) or set(r)!={'name','bytes','sha256'} or _name(r['name'])!=Path(r['name']).name:
            raise ValueError('unsafe candidate archive name')
        if type(r['bytes']) is not int or r['bytes']<1 or not re.fullmatch('[a-f0-9]{64}',r['sha256']):
            raise ValueError('invalid archive fingerprint')
        path=bundle/r['name']
        if path.is_symlink() or not path.is_file() or _record(path)!=r: raise ValueError('candidate archive changed')
        if archive_manifest(path,kind)!=value[kind+'_members']: raise ValueError('candidate content manifest differs')
        if _archive_version(path,kind)!=value['version']: raise ValueError('candidate version differs')
    if hashlib.sha256(canonical(value['source_manifest'])).hexdigest()!=value['source_manifest_sha256']:
        raise ValueError('candidate source identity differs')
    if set(value['sdist_members'])-set(value['source_manifest'])!={'PKG-INFO','setup.cfg'} or any(value['sdist_members'].get(k)!=v for k,v in value['source_manifest'].items()):
        raise ValueError('candidate source manifest incomplete')
    return value


def safe_extract_sdist(bundle,destination):
    value=load_candidate(bundle);destination=Path(destination)
    if destination.exists() or destination.is_symlink(): raise ValueError('extraction destination must be new')
    destination.mkdir(mode=0o700,parents=True)
    source_root=None
    with tarfile.open(Path(bundle)/value['archives']['sdist']['name'],'r:gz') as archive:
        for member in archive:
            parts=_name(member.name.rstrip('/') if member.isdir() else member.name).split('/')
            path=destination.joinpath(*parts)
            source_root=destination/parts[0]
            if member.isdir():path.mkdir(mode=0o700,parents=True,exist_ok=True);continue
            path.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
            with archive.extractfile(member) as src,os.fdopen(os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600),'wb') as out:
                shutil.copyfileobj(src,out)
    for name,r in value['sdist_members'].items():
        path=source_root/name
        if path.stat().st_size!=r['bytes'] or sha256_file(path)!=r['sha256']: raise ValueError('extracted source differs')
    return source_root


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    sub=parser.add_subparsers(dest='command',required=True)
    p=sub.add_parser('adopt');p.add_argument('--wheel',type=Path,required=True);p.add_argument('--sdist',type=Path,required=True);p.add_argument('--provenance',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--expected-tests',type=int,required=True)
    p=sub.add_parser('verify');p.add_argument('--candidate',type=Path,required=True)
    args=parser.parse_args(argv)
    value=adopt_candidate(args.wheel,args.sdist,args.provenance,args.output,args.expected_tests) if args.command=='adopt' else load_candidate(args.candidate)
    print(json.dumps({'candidate_id':value['candidate_id'],'version':value['version'],'archives':value['archives'],'source_files':len(value['source_manifest'])},sort_keys=True))
    return 0

if __name__=='__main__':raise SystemExit(main())
