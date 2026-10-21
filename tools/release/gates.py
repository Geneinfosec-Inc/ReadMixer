#!/usr/bin/env python3
"""Reconcile candidate-bound evidence; never publish or silently close Plan 10.

Platform results are checked mechanically. Resource/calibration and final document
reviews are explicitly attributed human reviews with fingerprinted supporting
records, not substitutes for execution. Missing evidence always blocks readiness.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import sys
import xml.etree.ElementTree as ET
import zipfile

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))
from candidate import CELLS, archive_manifest, canonical, load_candidate, read_json, sha256_file
from platform_lane import assess_suite, junit_summary

SCHEMA = 'readmixer.release.index.v1'
POLICY_SHA = '4f8f5b17304bf8a8bb62e5ec525283187f6c7a0274074ecaacb1d82b0425d679'
POLICY_BINDINGS = {
    'sha256:bbc294507fb64e0bd82e5a235f4245fff14e4229609983929705107f73cf51e8':
        '99356e097ed78ffd7c462aaceb4b1ebba4339a3b4e07f93218d9e15e66eeb4f1',
    'sha256:3645a6e4b41138f8f0dd3cc4fd9a31974df2dd975c2f458c6de7a725c9d14776':
        'e3dac2eae14fc8e3633b0fa243c4fae9be9bb1b3b4ce8625a711d2872101791b',
    'sha256:8d97dc9b97978eab2effc146cffab1d07d1ea568194d2dd2f9f5bab664850ad3':
        'c4d85ccc468e74c5b65a86d12a9f048bfc226fca816bc43fd8d7aa0990e6ed3a',
    'sha256:0a88bf5087dc358c9df6a3a7af413a05a45a53437c79df355044f76008b2e0d7':
        '83cfad2edc5246c30bef2873ef6f5a9a823190fc9e808133b8da67b0f5523a24',
    'sha256:426e563211c32b5bea806862d65bd0754419e397fb53ecaaa7239e11a00810ff': POLICY_SHA,
    'sha256:197110fc1431984e92565c7b44632565be089f248595b158a8c6327a15d9b19c':
        'd05de85fe05efc701e655c818af1d392f893dd5e6684fcf8095883093dcc2e60',
}
ORACLES = ('test_htslib_bgzip_and_pysam_read_produced_canonical_stream',
           'test_htslib_sidecar_and_virtual_offset_oracles')
CRITERIA = {
    'resources': ['complete_bounded_and_portable_host_profiles', 'greater_than_four_actual_budgets',
        'external_and_auto_and_strict_memory_outcomes', 'one_warmup_five_measured_observations',
        'process_tree_cpu_rss_threads_descriptors_and_io', 'shared_and_distinct_filesystems',
        'slow_storage_backpressure_and_space_query_failure', 'real_signals_crashes_cleanup_and_publication',
        'platform_specific_enforcement_or_explicit_applicability'],
    'calibration': ['versioned_charges_cover_observed_native_build', 'all_required_supplements_and_startup',
        'one_warmup_five_measured_observations', 'all_raw_observations_and_cache_treatment',
        'performance_claims_match_observations'],
    'documents-audit': ['all_AUD_01_through_24_mapped_to_executed_nodes',
        'all_parent_scenarios_reconciled_without_collection_only_credit',
        'current_docs_help_api_migration_security_and_format_versions_checked',
        'links_json_shell_and_archive_content_checks', 'failed_attempts_and_limitations_disclosed',
        'no_supplied_data_or_private_diagnostics_in_public_evidence',
        'full_supplied_data_status_distinct_from_portable_release',
        'historical_archives_and_fingerprints_preserved', 'all_new_harness_tests_passed'],
}
TRACE = {
 'P17-T01': {'parents':['API-01','Plan10-6'], 'audits':['AUD-19','AUD-20']},
 'P17-T02': {'parents':['API-01','API-02','API-04','P09-T01','Plan10-6'], 'audits':['AUD-12','AUD-19','AUD-20','AUD-22']},
 'P17-T03': {'parents':['API-03..07','P06-003','P06-007','P06-009','P08-010'], 'audits':['AUD-03','AUD-06','AUD-12','AUD-13','AUD-16','AUD-18','AUD-19','AUD-20','AUD-24']},
 'P17-T04': {'parents':['P03-T06','P03-T07','P05-T04','P05-T06','P09-T01','P09-T05','P09-T07','Plan10-4..6'], 'audits':['AUD-10','AUD-12','AUD-13','AUD-14','AUD-17','AUD-19','AUD-22','AUD-23']},
 'P17-T05': {'parents':['Plan10-6','Plan10-7'], 'audits':[f'AUD-{i:02}' for i in range(1,25)]},
 'P17-T06': {'parents':['API-01','API-07','Plan10-8','P16-T06'], 'audits':['AUD-12','AUD-18','AUD-19','AUD-20','AUD-24']},
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def inside(root, relative):
    require(isinstance(relative,str) and 0 < len(relative) <= 1024, 'invalid evidence path')
    require('\\' not in relative and not any(ord(c)<32 for c in relative), 'unsafe evidence path')
    parts=PurePosixPath(relative).parts
    require(not relative.startswith('/') and all(p not in ('','.','..') for p in relative.split('/')), 'unsafe evidence path')
    target=Path(root)
    for part in parts:
        target=target/part
        require(not target.is_symlink(), 'symlink evidence refused')
    require(target.is_file(), 'missing evidence file')
    return target


def checked_ref(root, record, max_bytes=256 << 20):
    require(isinstance(record,dict) and {'path','bytes','sha256'} <= set(record), 'invalid evidence reference')
    require(type(record['bytes']) is int and 0 <= record['bytes'] <= max_bytes, 'evidence exceeds bound')
    require(isinstance(record['sha256'],str) and re.fullmatch('[a-f0-9]{64}',record['sha256']), 'invalid evidence hash')
    path=inside(root,record['path'])
    require(path.stat().st_size==record['bytes'] and sha256_file(path)==record['sha256'], 'evidence fingerprint mismatch')
    return path


def record(path, root):
    path=Path(path)
    return {'path':path.relative_to(root).as_posix(),'bytes':path.stat().st_size,'sha256':sha256_file(path)}


def junit_nodes(path):
    result={}
    for case in ET.parse(path).getroot().iter('testcase'):
        key=(case.get('classname'),case.get('name'))
        require(key not in result, 'duplicate raw JUnit testcase')
        result[key]='errors' if case.find('error') is not None else 'failed' if case.find('failure') is not None else 'skipped' if case.find('skipped') is not None else 'passed'
    return result


def xml_node(nodeid):
    head,bracket,parameters=nodeid.partition('[')
    parts=head.split('::')
    require(len(parts)>=2 and parts[0].endswith('.py'), 'unexpected pytest node format')
    return (parts[0][:-3].replace('/','.')+(''.join('.'+p for p in parts[1:-1])),parts[-1]+bracket+parameters)


def validate_platform(path, candidate, *, expected_cell=None, private_required=True):
    report=read_json(path,64 << 20)
    require(report.get('schema')=='readmixer.release.platform-lane.v1', 'unknown platform report schema')
    require(report.get('candidate_id')==candidate['candidate_id'] and report.get('archives')==candidate['archives'], 'platform candidate mismatch')
    cell=report.get('expected_cell')
    require(expected_cell is None or cell==expected_cell, 'report is in the wrong platform slot')
    require(cell in CELLS and report.get('host',{}).get('cell')==cell, 'platform architecture mismatch')
    require(report.get('completed') is True and report.get('accepted') is True, 'platform lane did not pass')
    env_path=inside(path.parent,'environment.json')
    env=read_json(env_path)
    lock=inside(path.parent,'requirements.lock')
    require(not lock.is_symlink() and lock.is_file(), 'missing retained lock')
    require(env.get('schema')=='readmixer.release.environment.v1', 'unknown environment schema')
    require(sha256_file(path.parent/'environment.json')==report['environment']['manifest_sha256'], 'environment manifest changed')
    require(sha256_file(lock)==env['lock_sha256']==report['environment']['lock_sha256'], 'environment lock changed')
    system,machine,py=cell.split('-')
    facts=env['host']
    require(facts['system']=={'linux':'Linux','macos':'Darwin'}[system] and facts['machine']==machine and facts['implementation']=='CPython' and facts['pointer_bits']==64, 'environment host differs')
    require('py'+''.join(facts['python_full_version'].split('.')[:2])==py, 'environment Python differs')
    require(facts['python_full_version']==report['host']['python_version'], 'interpreter build mismatch')
    # The native host already verified wheel tags and dependency closure. Here the
    # foreign-platform record is rehashed, never evaluated using local markers.
    require(report.get('installed_package_matches_wheel',{}).get('matched') is True, 'wheel installation was not verified')
    native=report.get('native',{})
    require(native.get('filesystem',{}).get('passed') is True, 'filesystem probe missing')
    for key in ('implementation','machine','pointer_bits','system','python_build','soabi','sysconfig_platform'):
        require(native['host'][key]==facts[key]==report['host'][key], 'native interpreter facts differ')
    require(native['host']['python_version']==facts['python_full_version'] and native['host']['isolated'] is True and native['host']['base_prefix_distinct'] is True, 'native isolation missing')
    normalize=lambda name: re.sub(r'[-_.]+','-',name).lower()
    expected_deps={normalize(w['name']):w['version'] for w in env['wheels']}
    expected_deps['readmixer-py']=candidate['version']
    actual_deps={normalize(d['name']):d['version'] for d in native['dependencies']}
    require(len(actual_deps)==len(native['dependencies']) and actual_deps==expected_deps, 'installed dependencies differ from locked selection')
    require(report.get('sdist_rebuild',{}).get('all_member_bytes_equal') is True, 'sdist content equivalence missing')
    rebuilt=list((path.parent/'rebuilt').glob('*.whl'))
    require(len(rebuilt)==1 and sha256_file(rebuilt[0])==report['sdist_rebuild']['rebuilt_sha256'] and archive_manifest(rebuilt[0],'wheel')==candidate['wheel_members'], 'retained rebuilt wheel differs')
    lazy=report.get('lazy_checks',[])
    require(len(lazy)==6 and {(x.get('entry'),x.get('flag')) for x in lazy}=={(entry,flag) for entry in ('module','console','forwarder') for flag in ('--help','--version')}, 'dependency-free entry probes missing')
    require(all(x.get('passed') is True and x.get('exit_code')==0 and x.get('native_dependencies_absent') is True and all(x.get(k)==[] for k in ('native_import_attempts','native_modules_loaded','child_creation_attempts')) for x in lazy), 'dependency-free probe did not pass')
    expected=candidate['expected_tests']; allowed=set(); applicable=[]
    if system=='macos':
        policy_path=inside(path.parent,'platform-applicability.json')
        policy_sha = POLICY_BINDINGS.get(candidate['candidate_id'])
        require(policy_sha is not None and sha256_file(policy_path)==policy_sha, 'unrecognized macOS applicability policy')
        policy=read_json(policy_path)
        require(policy['candidate_id']==candidate['candidate_id'], 'applicability candidate mismatch')
        applicable=policy['macos-arm64']
        require(report['platform_applicability']['policy_sha256']==policy_sha and report['platform_applicability']['not_applicable']==applicable, 'reported applicability differs')
        for item in applicable:
            require(candidate['source_manifest'][item['nodeid'].split('::')[0]]['sha256']==item['source_sha256'], 'applicability test source mismatch')
            allowed.add(item['nodeid'])
    expected_package={k.removeprefix('readmixer_py/'):v for k,v in candidate['wheel_members'].items() if k.startswith('readmixer_py/') and k.endswith('.py')}
    require(native['package']['location_verified'] is True and native['package']['system_site_packages'] is False and native['package']['mode']=='installed' and {x['name']:{'bytes':x['bytes'],'sha256':x['sha256']} for x in native['package']['package_files']}==expected_package, 'native package does not match candidate')
    observed=[]
    require(set(report.get('suites',{}))=={'source','installed'}, 'complete source and installed suites required')
    for mode,suite in report['suites'].items():
        require(suite.get('passed') is True and suite.get('collected')==expected, 'incomplete suite')
        nodes=suite.get('nodes',[]); ids=[x['nodeid'] for x in nodes]
        require(len(ids)==expected and len(set(ids))==expected, 'duplicate or missing executed nodes')
        require(allowed <= set(ids), 'declared applicability nodes not collected')
        observed.append(set(ids))
        for item in nodes:
            phases=item['phases']
            if item['nodeid'] in allowed:
                require(any(p['outcome']=='skipped' for p in phases) and all(p['outcome'] in ('passed','skipped') and not p.get('xfail') for p in phases), 'applicability is not an ordinary pass')
            else:
                require(item['outcome']=='passed' and len(phases)==3 and {p['phase'] for p in phases}=={'setup','call','teardown'} and all(p['outcome']=='passed' and p.get('xfail') is False for p in phases), 'failed/skipped/xfail test')
        counts={'executed':expected,'passed':expected-len(allowed),'failed':0,'errors':0,'skipped':len(allowed)}
        require(suite['counts']==counts and suite['junit']['counts']==counts, 'JUnit counts disagree')
        require(all(suite['independent_oracles'].get(n) is True and 'tests/test_bgzf.py::'+n in ids for n in ORACLES), 'required independent oracle missing')
        pkg=suite['package']
        require(pkg['location_verified'] is True and pkg['system_site_packages'] is False and pkg['mode']==mode, 'source/install isolation missing')
        require({x['name']:{'bytes':x['bytes'],'sha256':x['sha256']} for x in pkg['package_files']}==expected_package, 'executed runtime bytes differ')
        if private_required:
            xml=inside(path.parent,'private/'+mode+'-suite.xml')
            require(not xml.is_symlink() and xml.stat().st_size<=64 << 20 and sha256_file(xml)==suite['junit']['sha256'], 'raw JUnit missing or changed')
            require(b'<!DOCTYPE' not in xml.read_bytes(), 'DTD in JUnit refused')
            raw_junit=junit_summary(xml)
            require(raw_junit==suite['junit'], 'raw JUnit outcomes differ from summary')
            require(junit_nodes(xml)=={xml_node(n['nodeid']):'skipped' if n['nodeid'] in allowed else 'passed' for n in nodes}, 'raw JUnit node identities differ')
            collection=read_json(inside(path.parent,'private/'+mode+'-collect.json'),64 << 20)
            execution=read_json(inside(path.parent,'private/'+mode+'-suite.json'),64 << 20)
            assessment=assess_suite(collection,execution,raw_junit,expected,applicable)
            require(assessment['passed'] is True and all(assessment[k]==suite[k] for k in ('nodes','counts','package')), 'raw collection/execution outcomes differ')
    require(observed[0]==observed[1], 'source and installed collections differ')
    harness=report.get('harness',[])
    require(len(harness)>=4 and len({x['name'] for x in harness})==len(harness), 'harness identity missing')
    for item in harness:
        p=inside(path.parent,'harness/'+item['name'])
        require(sha256_file(p)==item['sha256'], 'retained harness differs')
    stages=report.get('stages',[])
    expected_stages={'create-venv','install-locked-dependencies','install-candidate','dependency-check','native-facts','source-collect','source-suite','installed-collect','installed-suite','create-dependency-free','install-dependency-free','sdist-rebuild'} | {'lazy-'+entry+'-'+flag for entry in ('module','console','forwarder') for flag in ('help','version')}
    require(len(stages)==len(expected_stages) and {s['name'] for s in stages}==expected_stages, 'command stages missing')
    for stage in stages:
        require(stage.get('exit_code')==0 and stage.get('timed_out') is False, 'failed command stage')
        if private_required:
            for kind,key in [('command','command_json'),('stdout','stdout'),('stderr','stderr')]:
                p=inside(path.parent,stage[kind+'_ref'])
                require(sha256_file(p)==stage[key+'_sha256'], 'raw command evidence changed')
    if private_required:
        require(read_json(inside(path.parent,'private/native-facts.json'),64 << 20)==native, 'raw native facts differ')
        for item in lazy:
            name='private/lazy-'+item['entry']+'-'+item['flag'].removeprefix('--')+'.json'
            require(read_json(inside(path.parent,name),4 << 20)==item, 'raw lazy probe differs')
    return {'cell':cell,'environment_sha256':report['environment']['manifest_sha256'],
            'passed_per_suite':expected-len(allowed),'not_applicable_per_suite':len(allowed),
            'raw_evidence_verified':private_required,'scope':'ordinary source and installed qualification only'}


def review_criteria(kind,cell):
    extra=['portable_nofile_kernel_readback_parent_unchanged_regression_on_macos',
           'portable_process_group_timeout_reaping_regression_on_macos',
           'actual_macos_low_descriptor_workflows'] if kind=='resources' and cell and cell.startswith('macos-') else []
    return [*CRITERIA[kind],*extra]


def validate_review(path, root, candidate, kind, cell, environment):
    value=read_json(path,4 << 20)
    require(value.get('schema')=='readmixer.release.review.v1', 'unknown review schema')
    require(value.get('candidate_id')==candidate['candidate_id'] and value.get('kind')==kind and value.get('cell')==cell, 'review scope mismatch')
    require(value.get('environment_sha256')==environment, 'review environment mismatch')
    require(value.get('outcome')=='accepted' and isinstance(value.get('reviewer'),str) and bool(value['reviewer'].strip()) and isinstance(value.get('reviewed_at'),str) and re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z',value['reviewed_at']), 'review is incomplete')
    require(isinstance(value.get('limitations'),list), 'review limitations required')
    criteria=value.get('criteria',{})
    require(set(criteria)==set(review_criteria(kind,cell)), 'review omits required criteria')
    refs=value.get('evidence',[])
    require(1<=len(refs)<=4096, 'review needs bounded supporting evidence')
    for ref in refs: checked_ref(root,ref,4 << 30)
    for criterion,item in criteria.items():
        require(isinstance(item,dict) and item.get('accepted') is True and isinstance(item.get('finding'),str) and len(item['finding'].strip())>=20, 'review lacks finding: '+criterion)
        indices=item.get('evidence_indices',[])
        require(isinstance(indices,list) and indices and all(type(i) is int and 0<=i<len(refs) for i in indices), 'review lacks supporting records: '+criterion)
    return {'scope':'attributed review of fingerprinted execution evidence; not an automated re-execution',
            'automated_execution_validation':False,'reviewer':value['reviewer'],'reviewed_at':value['reviewed_at'],'limitations':value['limitations']}


def template(candidate):
    return {'schema':SCHEMA,'candidate_id':candidate['candidate_id'],
        'platforms':{c:None for c in CELLS},
        'reviews':{**{c:{'resources':None,'calibration':None} for c in CELLS},'documents-audit':None},
        'local_supplied_data':None,
        'scope':'References are relative to --evidence-root. Null means unexecuted; never replace a missing run with a pass boolean.'}


def assess(candidate_dir,index_path,root,public_only=False):
    candidate=load_candidate(candidate_dir); index=read_json(index_path)
    require(index.get('schema')==SCHEMA and index.get('candidate_id')==candidate['candidate_id'], 'index candidate mismatch')
    require(set(index.get('platforms',{}))==set(CELLS), 'index must list all four required cells')
    require(set(index.get('reviews',{}))==set(CELLS)|{'documents-audit'}, 'index must list all review scopes')
    rows=[]; platform={}
    def check(label,ref,validator):
        if ref is None:
            rows.append({'obligation':label,'status':'missing'});return None
        try:
            path=checked_ref(root,ref,64 << 20); details=validator(path)
            rows.append({'obligation':label,'status':'review_attested' if details.get('automated_execution_validation') is False else 'verified','evidence':ref,'details':details}); return details
        except (ValueError,KeyError,TypeError,OSError,ET.ParseError,zipfile.BadZipFile) as exc:
            # Evidence errors disclose only bounded categories, never record payloads.
            rows.append({'obligation':label,'status':'failed' if isinstance(exc,ValueError) and str(exc)=='platform lane did not pass' else 'invalid','reason':str(exc)[:240] if isinstance(exc,ValueError) else type(exc).__name__});return None
    for cell in CELLS:
        platform[cell]=check('ordinary:'+cell,index['platforms'][cell],lambda p,c=cell:validate_platform(p,candidate,expected_cell=c,private_required=not public_only))
        require(set(index['reviews'][cell])=={'resources','calibration'}, 'unknown review kind')
        for kind in ('resources','calibration'):
            env=platform[cell]['environment_sha256'] if platform[cell] else 'missing ordinary environment'
            check(kind+':'+cell,index['reviews'][cell][kind],lambda p,k=kind,c=cell,e=env:validate_review(p,root,candidate,k,c,e))
    check('documents-audit',index['reviews']['documents-audit'],lambda p:validate_review(p,root,candidate,'documents-audit',None,None))
    if public_only:
        rows.append({'obligation':'private-raw-evidence-integrity','status':'missing','reason':'Public-only inspection cannot establish complete evidence integrity.'})
    blockers=[r['obligation'] for r in rows if r['status'] not in ('verified','review_attested')]
    local={'status':'unrecorded','scope':'Separate elected local task; no effect on portable readiness.'}
    if index.get('local_supplied_data') is not None:
        try:
            p=checked_ref(root,index['local_supplied_data'],64 << 20); value=read_json(p,64 << 20)
            expected={v['name']:{'bytes':v['bytes'],'sha256':v['sha256']} for v in candidate['archives'].values()}
            require(value.get('archives')==expected, 'local task candidate differs')
            full=value['full_supplied_data']
            require(value.get('full_supplied_data_completed') is True and full['all_comparisons_equal'] is True and full['originals_unchanged'] is True and full['workspace_cleanup_complete'] is True and full['inventory']['pairs']==30042389, 'local task incomplete')
            local={'status':'recorded_complete','pairs':30042389,'evidence':index['local_supplied_data'],
                   'scope':'Preserved aggregate record; private original datasets are not redistributed or rerun.'}
        except (ValueError,KeyError,TypeError,OSError): local={'status':'invalid','scope':'Separate local task record could not be verified.'}
    return {'schema':'readmixer.release.assessment.v1','candidate_id':candidate['candidate_id'],
        'index_sha256':sha256_file(index_path),'checker_sha256':sha256_file(Path(__file__)),
        'validator_hashes':{name:sha256_file(HERE/name) for name in ('candidate.py','platform_lane.py','platform_probe.py')},
        'ready_for_plan10_review':not blockers,'plan10_complete':False,'plan17_complete':False,
        'package_published':False,'blockers':blockers,'obligations':rows,'scenario_traceability':TRACE,
        'full_supplied_data':local,'scope':'Mechanical integrity/readiness assessment plus explicitly attributed reviews. Plan 10 final decision and publication remain separate actions.'}


def write_new(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    with os.fdopen(os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'wb') as stream:
        stream.write(canonical(value)+b'\n');stream.flush();os.fsync(stream.fileno())


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    sub=parser.add_subparsers(dest='command',required=True)
    p=sub.add_parser('init');p.add_argument('--candidate',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    p=sub.add_parser('review-template');p.add_argument('--candidate',type=Path,required=True);p.add_argument('--kind',choices=CRITERIA,required=True);p.add_argument('--cell',choices=CELLS);p.add_argument('--environment-sha256');p.add_argument('--output',type=Path,required=True)
    p=sub.add_parser('assess');p.add_argument('--candidate',type=Path,required=True);p.add_argument('--index',type=Path,required=True);p.add_argument('--evidence-root',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--public-only',action='store_true')
    args=parser.parse_args(argv)
    if args.command=='init': result=template(load_candidate(args.candidate))
    elif args.command=='review-template':
        if (args.kind=='documents-audit') != (args.cell is None):parser.error('per-cell reviews require --cell; documents-audit has none')
        if args.cell and not args.environment_sha256:parser.error('per-cell review requires --environment-sha256')
        result={'schema':'readmixer.release.review.v1','candidate_id':load_candidate(args.candidate)['candidate_id'],
            'kind':args.kind,'cell':args.cell,'environment_sha256':args.environment_sha256,
            'reviewer':None,'reviewed_at':None,'outcome':'pending','limitations':[],
            'criteria':{k:{'accepted':False,'finding':'','evidence_indices':[]} for k in review_criteria(args.kind,args.cell)},'evidence':[]}
    else: result=assess(args.candidate,args.index,args.evidence_root,args.public_only)
    write_new(args.output,result)
    if args.command=='assess':
        print(json.dumps({'ready_for_plan10_review':result['ready_for_plan10_review'],'blockers':result['blockers'],'plan10_complete':False}))
        return 0 if result['ready_for_plan10_review'] else 1
    return 0


if __name__=='__main__':raise SystemExit(main())
