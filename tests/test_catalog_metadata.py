"""Catalog metadata shares the invocation ceiling through aliases and failures."""

from dataclasses import asdict
import weakref

import pytest

from readmixer_py import catalog as module
from readmixer_py import inspection
from readmixer_py.catalog import build_catalog
from readmixer_py.config import InspectConfig
from readmixer_py.errors import InputError, ResourceError
from readmixer_py.resources import Budget


FASTQ = b"@artificial\nAC\n+symbolic\n!!\n"


def pair(root):
    root.mkdir(parents=True, exist_ok=True)
    for mate in (1, 2):
        (root / f"sample_R{mate}.fq").write_bytes(FASTQ)


def empty(budget):
    assert budget.used_bytes == 0
    assert budget.used_metadata_bytes == 0
    assert budget.used_descriptors == 0
    assert budget.active_leases == 0


def test_shared_metadata_exact_peak_and_one_byte_over(tmp_path):
    pair(tmp_path)
    probe = Budget(64 << 20)
    with build_catalog(input_dir=tmp_path, budget=probe) as catalog:
        catalog_peak = catalog.peak_metadata_bytes
        peak = probe.peak_metadata_bytes
    empty(probe)
    assert peak > catalog_peak  # The budget registry is also admitted metadata.
    exact = Budget(64 << 20, metadata_limit_bytes=peak)
    with build_catalog(input_dir=tmp_path, budget=exact):
        pass
    empty(exact)
    short = Budget(64 << 20, metadata_limit_bytes=peak - 1)
    with pytest.raises(ResourceError) as caught:
        build_catalog(input_dir=tmp_path, budget=short)
    assert caught.value.__traceback__ is not None
    empty(short)
    local = Budget(64 << 20)
    with build_catalog(input_dir=tmp_path, budget=local, max_metadata_bytes=catalog_peak):
        pass
    with pytest.raises(ResourceError):
        build_catalog(input_dir=tmp_path, budget=local, max_metadata_bytes=catalog_peak - 1)
    empty(local)


def test_catalog_and_unrelated_owner_cannot_each_spend_shared_ceiling(tmp_path):
    pair(tmp_path)
    probe = Budget(64 << 20)
    with build_catalog(input_dir=tmp_path, budget=probe):
        pass
    budget = Budget(64 << 20, metadata_limit_bytes=probe.peak_metadata_bytes)
    with budget.acquire("other-owner", 4096, metadata_bytes=4096):
        with pytest.raises(ResourceError):
            build_catalog(input_dir=tmp_path, budget=budget)
        assert budget.used_metadata_bytes >= 4096
        assert budget.used_descriptors == 0
        assert budget.active_leases == 1
    empty(budget)


@pytest.mark.parametrize("alias_kind", ["sample", "source", "snapshot"])
def test_close_and_dispose_keep_retained_record_alias_charged(tmp_path, alias_kind):
    pair(tmp_path)
    budget = Budget(64 << 20)
    catalog = build_catalog(input_dir=tmp_path, budget=budget)
    sample = catalog.samples[0]
    aliases = {"sample": sample, "source": sample.r1, "snapshot": sample.r1.snapshot}
    alias = aliases[alias_kind]
    observed = weakref.ref(alias)
    del aliases, sample
    charge = budget.used_metadata_bytes
    catalog.close()
    assert budget.used_descriptors == 0
    assert budget.used_metadata_bytes == charge
    catalog.dispose()
    assert catalog.samples == () and catalog.root is None
    assert budget.used_metadata_bytes == charge
    del alias
    assert observed() is None
    empty(budget)


def test_dispose_waits_for_open_reader_descriptor_ownership(tmp_path):
    pair(tmp_path)
    budget = Budget(64 << 20)
    catalog = build_catalog(input_dir=tmp_path, budget=budget)
    source = catalog.samples[0].r1
    with pytest.raises(InputError, match="closed"):
        with catalog.open_input(source) as stream:
            catalog.dispose()
            assert budget.used_descriptors == 6
            assert stream.read() == FASTQ
    assert budget.used_descriptors == 0
    del source
    empty(budget)


def test_third_live_reader_is_rejected_without_losing_admission(tmp_path):
    pair(tmp_path)
    budget = Budget(64 << 20)
    with build_catalog(input_dir=tmp_path, budget=budget) as catalog:
        with catalog.open_input(catalog.samples[0].r1), catalog.open_input(catalog.samples[0].r2):
            with pytest.raises(ResourceError, match="two simultaneous"):
                with catalog.open_input(catalog.samples[0].r1):
                    pytest.fail("third reader must not open")
        assert budget.used_descriptors == 6
    empty(budget)


def test_discovery_frontier_and_dedup_metadata_is_released_after_construction(tmp_path):
    pair(tmp_path)
    baseline = Budget(64 << 20)
    with build_catalog(input_dir=tmp_path, budget=baseline) as catalog:
        retained = catalog.metadata_bytes
        before_peak = catalog.peak_metadata_bytes
    for index in range(24):
        folder = tmp_path / f"empty-{index:02d}"
        folder.mkdir()
        (folder / "ignored.txt").write_text("public artificial fixture")
    budget = Budget(64 << 20)
    with build_catalog(input_dir=tmp_path, budget=budget) as catalog:
        assert catalog.metadata_bytes == retained
        assert catalog.peak_metadata_bytes > before_peak + 24 * 512
        assert budget.used_metadata_bytes < budget.peak_metadata_bytes
    empty(budget)


@pytest.mark.parametrize("failure", ["orphan", "allocation", "root-check"])
def test_retained_failed_traceback_does_not_retain_catalog_graph(tmp_path, monkeypatch, failure):
    pair(tmp_path)
    budget = Budget(64 << 20)
    observed = []
    original = module._Builder.source

    def watch_source(self, relative):
        source = original(self, relative)
        observed.append(weakref.ref(source))
        if failure == "allocation":
            raise MemoryError("injected allocation failure")
        return source

    monkeypatch.setattr(module._Builder, "source", watch_source)
    if failure == "orphan":
        (tmp_path / "sample_R2.fq").unlink()
    elif failure == "root-check":
        def fail_root(self):
            raise InputError("injected root recheck failure")
        monkeypatch.setattr(module.SampleCatalog, "_check_root", fail_root)
    with pytest.raises((InputError, ResourceError)) as caught:
        build_catalog(input_dir=tmp_path, budget=budget)
    assert caught.value.__traceback__ is not None
    assert observed and all(reference() is None for reference in observed)
    empty(budget)


def test_encoded_limit_is_independent_of_shared_resident_metadata(tmp_path, monkeypatch):
    pair(tmp_path)
    probe = Budget(64 << 20)
    with build_catalog(input_dir=tmp_path, budget=probe) as catalog:
        encoded = catalog.encoded_bytes
    monkeypatch.setattr(module, "MAX_ENCODED_CATALOG_BYTES", encoded)
    with build_catalog(input_dir=tmp_path, budget=probe):
        pass
    monkeypatch.setattr(module, "MAX_ENCODED_CATALOG_BYTES", encoded - 1)
    with pytest.raises(ResourceError, match="Encoded catalog"):
        build_catalog(input_dir=tmp_path, budget=probe)
    empty(probe)


def capture_inspection_budget(monkeypatch):
    budgets = []
    original = inspection.operation_resources
    def capture(*args, **kwargs):
        plan, budget = original(*args, **kwargs)
        budgets.append(budget)
        return plan, budget
    monkeypatch.setattr(inspection, "operation_resources", capture)
    return budgets


@pytest.mark.parametrize("deep", [False, True])
def test_inspection_returned_report_holds_only_its_own_metadata(tmp_path, monkeypatch, deep):
    pair(tmp_path)
    budgets = capture_inspection_budget(monkeypatch)
    report = inspection.inspect(InspectConfig(input_dir=tmp_path, deep=deep, max_ram=256 << 20))
    budget = budgets[0]
    assert budget.used_metadata_bytes >= 8192
    assert budget.active_leases == 1
    assert budget.used_descriptors == 0
    assert "_metadata_owner" not in asdict(report)
    assert "metadata_owner" not in repr(report)
    observed = weakref.ref(report)
    del report
    assert observed() is None
    # The immutable coordinator baseline is not a live metadata owner.
    assert budget.used_metadata_bytes == budget.used_descriptors == budget.active_leases == 0
    assert budget.used_bytes == budget.baseline_bytes


def test_failed_deep_inspection_releases_metadata_with_traceback_held(tmp_path, monkeypatch):
    pair(tmp_path)
    (tmp_path / "sample_R2.fq").write_bytes(b"@artificial\nAC\n+\n!\n")
    budgets = capture_inspection_budget(monkeypatch)
    with pytest.raises(InputError) as caught:
        inspection.inspect(InspectConfig(input_dir=tmp_path, deep=True, max_ram=256 << 20))
    assert caught.value.__traceback__ is not None
    budget = budgets[0]
    assert budget.used_metadata_bytes == budget.used_descriptors == budget.active_leases == 0
    assert budget.used_bytes == budget.baseline_bytes


def test_inspection_report_owner_allocation_failure_releases_acquired_lease(tmp_path, monkeypatch):
    pair(tmp_path)
    budgets = capture_inspection_budget(monkeypatch)
    def fail_owner(lease):
        raise MemoryError("injected report ownership allocation")
    monkeypatch.setattr(inspection, "_MetadataOwner", fail_owner)
    with pytest.raises(ResourceError):
        inspection.inspect(InspectConfig(input_dir=tmp_path, max_ram=256 << 20))
    budget = budgets[0]
    assert budget.used_metadata_bytes == budget.used_descriptors == budget.active_leases == 0
    assert budget.used_bytes == budget.baseline_bytes
