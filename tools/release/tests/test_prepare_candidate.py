from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import prepare_candidate as p


def test_complete_selected_manifest_includes_intended_untracked_source_and_detects_change(tmp_path):
    root = tmp_path / 'selected'; root.mkdir()
    (root / 'README.md').write_bytes(b'public\n')
    runtime = root / 'src/readmixer_py/new.py'; runtime.parent.mkdir(parents=True)
    runtime.write_bytes(b'VALUE = 1\n')
    manifest = p.selected_manifest(root)
    assert set(manifest) == {'README.md', 'src/readmixer_py/new.py'}
    p.require_unchanged(root, manifest)
    runtime.write_bytes(b'VALUE = 2\n')
    with pytest.raises(ValueError, match='changed'):
        p.require_unchanged(root, manifest)


@pytest.mark.parametrize('name', ['input/private.fastq', '.aws/credentials',
    'src/readmixer_py/__pycache__/x.pyc', 'dist/output.txt', 'src/readmixer_py/.env'])
def test_excluded_content_is_rejected_instead_of_silently_omitted(tmp_path, name):
    target = tmp_path / name; target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b'excluded')
    with pytest.raises(ValueError, match='excluded'):
        p.selected_manifest(tmp_path)


def test_linked_selected_input_is_rejected(tmp_path):
    (tmp_path / 'README.md').symlink_to('/etc/passwd')
    with pytest.raises(ValueError, match='linked'):
        p.selected_manifest(tmp_path)


def test_existing_preparation_destination_keeps_foreign_contents(tmp_path):
    output = tmp_path / 'existing'; output.mkdir()
    foreign = output / 'foreign'; foreign.write_text('preserved')
    with pytest.raises(ValueError, match='new'):
        p.build_candidate(tmp_path / 'source', tmp_path / 'environment', output, tmp_path / 'bundle', 1)
    assert foreign.read_text() == 'preserved'
