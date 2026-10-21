from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sys
import shutil
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import candidate as c
import gates as g
from test_candidate import artifacts


def write(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(value))


def test_recognized_macos_policies_keep_historical_binding_and_exact_nodes():
    root = Path(__file__).resolve().parents[1]
    old = c.read_json(root / 'platform-applicability-dev10.json')
    new = c.read_json(root / 'platform-applicability.json')
    assert old['candidate_id'] != new['candidate_id']
    assert old['macos-arm64'] == new['macos-arm64']
    assert len(new['macos-arm64']) == 8
    names = ['platform-applicability-dev10.json', 'platform-applicability-dev11.json',
             'platform-applicability-dev12.json', 'platform-applicability-dev13.json',
             'platform-applicability-dev14.json', 'platform-applicability.json']
    policies = [(name, c.read_json(root / name)) for name in names]
    assert len({policy['candidate_id'] for _, policy in policies}) == 6
    for name, policy in policies:
        assert policy['macos-arm64'] == old['macos-arm64']
        assert g.POLICY_BINDINGS[policy['candidate_id']] == c.sha256_file(root / name)
    assert g.POLICY_BINDINGS.get('sha256:' + '0' * 64) is None


@pytest.fixture
def evidence(tmp_path):
    paths=artifacts(tmp_path);candidate=c.adopt_candidate(*paths,tmp_path/'candidate',2)
    reportdir=tmp_path/'report';reportdir.mkdir();lock=reportdir/'requirements.lock';lock.write_text('exact fixture lock\n')
    env={'schema':'readmixer.release.environment.v1','host':{'system':'Linux','machine':'x86_64','implementation':'CPython','pointer_bits':64,'python_full_version':'3.11.7','python_build':['test','date'],'soabi':'test-abi','sysconfig_platform':'linux-x86_64'},'wheels':[],'lock_sha256':c.sha256_file(lock)}
    write(reportdir/'environment.json',env)
    nodes=[{'nodeid':'tests/test_bgzf.py::'+name,'outcome':'passed','phases':[{'phase':phase,'outcome':'passed','xfail':False,'seconds':.01} for phase in ('setup','call','teardown')]} for name in g.ORACLES]
    package_files=[{'name':k.removeprefix('readmixer_py/'),**v} for k,v in candidate['wheel_members'].items() if k.startswith('readmixer_py/')]
    counts={'executed':2,'passed':2,'failed':0,'errors':0,'skipped':0}
    suites={}
    for mode in ('source','installed'):
        xml=reportdir/'private'/(mode+'-suite.xml');xml.parent.mkdir(exist_ok=True);xml.write_text('<testsuites><testsuite>'+''.join('<testcase classname="tests.test_bgzf" name="'+name+'"/>' for name in g.ORACLES)+'</testsuite></testsuites>')
        suites[mode]={'passed':True,'collected':2,'nodes':deepcopy(nodes),'counts':deepcopy(counts),'junit':{'counts':deepcopy(counts),'sha256':c.sha256_file(xml)},'independent_oracles':{x:True for x in g.ORACLES},'package':{'location_verified':True,'system_site_packages':False,'mode':mode,'package_files':package_files}}
    harness=[]
    for i in range(4):
        p=reportdir/'harness'/f'helper{i}.py';p.parent.mkdir(exist_ok=True);p.write_text('# fixture helper\n');harness.append({'name':p.name,'sha256':c.sha256_file(p)})
    stages=[]
    stage_names=['create-venv','install-locked-dependencies','install-candidate','dependency-check','native-facts','source-collect','source-suite','installed-collect','installed-suite','create-dependency-free','install-dependency-free','sdist-rebuild']+['lazy-'+entry+'-'+flag for entry in ('module','console','forwarder') for flag in ('help','version')]
    for i,name in enumerate(stage_names):
        stage={'name':name,'exit_code':0,'timed_out':False}
        for kind,key in [('command','command_json'),('stdout','stdout'),('stderr','stderr')]:
            p=reportdir/'private'/f'stage{i}.{kind}';p.write_text('synthetic fixture\n')
            stage[kind+'_ref']=p.relative_to(reportdir).as_posix();stage[key+'_sha256']=c.sha256_file(p)
        stages.append(stage)
    report={'schema':'readmixer.release.platform-lane.v1','candidate_id':candidate['candidate_id'],'archives':candidate['archives'],'expected_cell':'linux-x86_64-py311','host':{'cell':'linux-x86_64-py311','python_version':'3.11.7'},'completed':True,'accepted':True,'environment':{'manifest_sha256':c.sha256_file(reportdir/'environment.json'),'lock_sha256':c.sha256_file(lock)},'installed_package_matches_wheel':{'matched':True},'native':{'filesystem':{'passed':True}},'sdist_rebuild':{'all_member_bytes_equal':True},'lazy_checks':[{'passed':True}]*6,'suites':suites,'harness':harness,'stages':stages}
    report['host'].update({k:v for k,v in env['host'].items() if k!='python_full_version'})
    native_host={**report['host'],'isolated':True,'base_prefix_distinct':True}
    report['native'].update(host=native_host,dependencies=[{'name':'readmixer-py','version':'1.0'}],package=deepcopy(suites['installed']['package']))
    report['lazy_checks']=[{'passed':True,'entry':entry,'flag':flag,'exit_code':0,'native_dependencies_absent':True,'native_import_attempts':[],'native_modules_loaded':[],'child_creation_attempts':[]} for entry in ('module','console','forwarder') for flag in ('--help','--version')]
    (reportdir/'rebuilt').mkdir();shutil.copyfile(paths[0],reportdir/'rebuilt'/paths[0].name)
    report['sdist_rebuild']['rebuilt_sha256']=c.sha256_file(paths[0])
    for mode,suite in suites.items():
        common={'collected':[n['nodeid'] for n in nodes],'exit_code':0,'package':suite['package']}
        write(reportdir/'private'/(mode+'-collect.json'),{**common,'collect_only':True,'outcomes':[]})
        write(reportdir/'private'/(mode+'-suite.json'),{**common,'collect_only':False,'outcomes':[{'nodeid':n['nodeid'],'phases':n['phases']} for n in nodes]})
    write(reportdir/'private/native-facts.json',report['native'])
    for item in report['lazy_checks']:write(reportdir/'private'/('lazy-'+item['entry']+'-'+item['flag'].removeprefix('--')+'.json'),item)
    p=reportdir/'report.json';write(p,report)
    return tmp_path,candidate,p,report


def test_complete_bound_platform_and_raw_integrity(evidence):
    root,candidate,path,value=evidence
    result=g.validate_platform(path,candidate,expected_cell='linux-x86_64-py311')
    assert result['passed_per_suite']==2 and result['raw_evidence_verified']


@pytest.mark.parametrize('mutation',[
 lambda r:r.update(candidate_id='different'),lambda r:r.update(accepted=False),
 lambda r:r['suites']['installed'].update(collected=1),
 lambda r:r['suites']['source']['nodes'].append(r['suites']['source']['nodes'][0]),
 lambda r:r['suites']['installed']['nodes'][0]['phases'][1].update(outcome='skipped'),
 lambda r:r['suites']['installed']['nodes'][0]['phases'][1].update(xfail=True),
 lambda r:r['suites']['installed']['nodes'][0]['phases'].pop(),
 lambda r:r['suites']['installed']['independent_oracles'].update({g.ORACLES[0]:False}),
 lambda r:r['suites']['installed']['counts'].update(passed=1),
 lambda r:r['suites']['installed']['package'].update(system_site_packages=True),
 lambda r:r['stages'][0].update(exit_code=1),
 lambda r:r['stages'][0].update(timed_out=True),
 lambda r:r['lazy_checks'].pop(),lambda r:r['sdist_rebuild'].update(all_member_bytes_equal=False),
])
def test_summary_pass_cannot_hide_failed_or_missing_requirements(evidence,mutation):
    root,candidate,path,value=evidence;mutation(value);write(path,value)
    with pytest.raises(ValueError):g.validate_platform(path,candidate)


def test_report_cannot_fill_another_required_platform_slot(evidence):
    root,candidate,path,value=evidence
    with pytest.raises(ValueError,match='wrong platform slot'):
        g.validate_platform(path,candidate,expected_cell='macos-arm64-py314')
    index=g.template(candidate)
    for cell in c.CELLS:index['platforms'][cell]=g.record(path,root)
    write(root/'index.json',index)
    result=g.assess(root/'candidate',root/'index.json',root)
    rows={r['obligation']:r for r in result['obligations']}
    assert rows['ordinary:linux-x86_64-py311']['status']=='verified'
    assert all(rows['ordinary:'+cell]['status']=='invalid' for cell in c.CELLS[1:])
    assert not result['ready_for_plan10_review'] and not result['plan10_complete']


@pytest.mark.parametrize('target',['requirements.lock','environment.json','harness/helper0.py','private/source-suite.xml','private/stage0.stdout'])
def test_changed_retained_evidence_blocks_platform(evidence,target):
    root,candidate,path,value=evidence
    p=path.parent/target;p.write_bytes(p.read_bytes()+b' ')
    with pytest.raises(ValueError):g.validate_platform(path,candidate)


@pytest.mark.parametrize('relative',['../escape','/tmp/foreign','private/../file','a\\b','a//b','a/./b','a\x00b'])
def test_unsafe_evidence_paths_rejected(tmp_path,relative):
    with pytest.raises(ValueError):g.inside(tmp_path,relative)


def test_symlink_parent_and_leaf_rejected(tmp_path):
    actual=tmp_path/'actual';actual.mkdir();(actual/'file').write_text('preserve')
    (tmp_path/'alias').symlink_to(actual,target_is_directory=True)
    with pytest.raises(ValueError,match='symlink'):g.inside(tmp_path,'alias/file')
    (actual/'alias').symlink_to(actual/'file')
    with pytest.raises(ValueError,match='symlink'):g.inside(tmp_path,'actual/alias')


def test_public_only_always_requires_final_raw_inspection(evidence):
    root,candidate,path,value=evidence
    (path.parent/'private/source-suite.xml').unlink()
    index=g.template(candidate);index['platforms'][c.CELLS[0]]=g.record(path,root);write(root/'index.json',index)
    result=g.assess(root/'candidate',root/'index.json',root,public_only=True)
    assert 'private-raw-evidence-integrity' in result['blockers']
    assert next(r for r in result['obligations'] if r['obligation']=='ordinary:'+c.CELLS[0])['status']=='verified'
    assert not result['ready_for_plan10_review']


def test_index_requires_every_cell_and_rejects_wrong_candidate(evidence):
    root,candidate,path,value=evidence
    index=g.template(candidate);index['platforms'].pop(c.CELLS[-1]);write(root/'index.json',index)
    with pytest.raises(ValueError,match='all four'):g.assess(root/'candidate',root/'index.json',root)
    index=g.template(candidate);index['candidate_id']='wrong';write(root/'index.json',index)
    with pytest.raises(ValueError,match='candidate'):g.assess(root/'candidate',root/'index.json',root)


def test_human_review_needs_attribution_specific_findings_and_hashed_records(evidence):
    root,candidate,path,value=evidence
    record={'schema':'readmixer.release.review.v1','candidate_id':candidate['candidate_id'],'kind':'calibration','cell':c.CELLS[0],'environment_sha256':'native','outcome':'accepted','reviewer':'Release reviewer','reviewed_at':'2026-09-30T12:00:00Z','limitations':['Fixture review only.'],'evidence':[g.record(path,root)],'criteria':{k:{'accepted':True,'finding':'Inspected the recorded measurements for this exact build.','evidence_indices':[0]} for k in g.CRITERIA['calibration']}}
    p=root/'review.json';write(p,record)
    result=g.validate_review(p,root,candidate,'calibration',c.CELLS[0],'native')
    assert result['reviewer']=='Release reviewer'
    for field,bad in [('reviewer',None),('outcome','pending'),('environment_sha256','other'),('evidence',[])]:
        changed=deepcopy(record);changed[field]=bad;write(p,changed)
        with pytest.raises(ValueError):g.validate_review(p,root,candidate,'calibration',c.CELLS[0],'native')
    changed=deepcopy(record);changed['criteria'][g.CRITERIA['calibration'][0]]['evidence_indices']=[];write(p,changed)
    with pytest.raises(ValueError):g.validate_review(p,root,candidate,'calibration',c.CELLS[0],'native')


def test_output_is_exclusive_and_missing_runs_are_not_passes(evidence):
    root,candidate,path,value=evidence
    index=g.template(candidate);write(root/'index.json',index)
    result=g.assess(root/'candidate',root/'index.json',root)
    assert len(result['blockers'])==13 and all(r['status']=='missing' for r in result['obligations'])
    assert result['full_supplied_data']['status']=='unrecorded'
    g.write_new(root/'assessment.json',result)
    before=(root/'assessment.json').read_bytes()
    with pytest.raises(FileExistsError):g.write_new(root/'assessment.json',{})
    assert (root/'assessment.json').read_bytes()==before


@pytest.mark.parametrize('source',['xml','execution','collection','native','lazy','rebuilt'])
def test_forged_passing_summary_cannot_override_raw_evidence(evidence,source):
    root,candidate,path,value=evidence
    if source=='xml':
        p=path.parent/'private/source-suite.xml';p.write_text('<testsuite><testcase><failure/></testcase><testcase/></testsuite>')
        value['suites']['source']['junit']['sha256']=c.sha256_file(p);write(path,value)
    elif source=='execution':
        p=path.parent/'private/source-suite.json';v=json.loads(p.read_text());v['outcomes'][0]['phases'][1]['outcome']='failed';write(p,v)
    elif source=='collection':
        p=path.parent/'private/source-collect.json';v=json.loads(p.read_text());v['collected'].pop();write(p,v)
    elif source=='native':
        p=path.parent/'private/native-facts.json';v=json.loads(p.read_text());v['host']['isolated']=False;write(p,v)
    elif source=='lazy':
        p=path.parent/'private/lazy-module-help.json';v=json.loads(p.read_text());v['child_creation_attempts']=['spawn'];write(p,v)
    else:
        p=next((path.parent/'rebuilt').glob('*.whl'));p.write_bytes(b'different archive')
    with pytest.raises(ValueError):g.validate_platform(path,candidate)


@pytest.mark.parametrize('mutation',[
 lambda r:r['native']['dependencies'].append({'name':'numpy','version':'unlocked'}),
 lambda r:r['native']['host'].update(isolated=False),
 lambda r:r['native']['host'].update(soabi='different'),
 lambda r:r['lazy_checks'].__setitem__(1,deepcopy(r['lazy_checks'][0])),
 lambda r:r['stages'][0].update(name='unrelated-stage'),
])
def test_native_lazy_and_required_command_contracts(evidence,mutation):
    root,candidate,path,value=evidence;mutation(value);write(path,value)
    with pytest.raises(ValueError):g.validate_platform(path,candidate)


def test_junit_identity_preserves_class_and_parameter_punctuation():
    assert g.xml_node('tests/test_nested.py::TestClass::test_member[arg::with.path]')==('tests.test_nested.TestClass','test_member[arg::with.path]')


def test_passing_junit_counts_do_not_hide_unrelated_test_nodes(evidence):
    root,candidate,path,value=evidence
    p=path.parent/'private/source-suite.xml';p.write_text('<testsuite><testcase classname="other" name="a"/><testcase classname="other" name="b"/></testsuite>')
    value['suites']['source']['junit']['sha256']=c.sha256_file(p);write(path,value)
    with pytest.raises(ValueError,match='node identities'):g.validate_platform(path,candidate)
