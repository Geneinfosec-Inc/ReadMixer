"""Independent boundary and fault tests for initial admission primitives."""

from dataclasses import asdict, replace
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import pytest

from readmixer_py.errors import ConfigurationError, ResourceError
from readmixer_py import resources
from readmixer_py.resources import (
    Budget, CgroupMemory, GIB, MIB, ResourceObservations, U64_MAX,
    checked_add, checked_multiply, parse_cgroup_limit, resolve_resources,
    terminal_fits,
)


def observations(**changes):
    return replace(ResourceObservations(
        available_memory=10 * GIB, coordinator_rss=64 * MIB, cpu_count=8,
        descriptor_soft_limit=4096, open_descriptors=12, platform="linux",
    ), **changes)


def test_headroom_reserve_and_explicit_clamping():
    facts = observations()
    result = resolve_resources(observations=facts)
    assert result.effective_max_ram == 8 * GIB + 64 * MIB
    assert result.reserved_headroom == 2 * GIB
    assert result.terminal_bytes == 128 * MIB
    assert result.metadata_bytes == 64 * MIB
    requested = resolve_resources(20 * GIB, observations=facts)
    assert requested.effective_max_ram == result.effective_max_ram
    assert requested.request_clamped
    assert not result.request_clamped
    assert requested.to_dict()["envelope_qualified"] is False
    assert asdict(requested)["envelope_qualified"] is False


def test_all_cgroup_ancestor_limits_intersect_with_host_headroom():
    facts = observations(cgroups=(
        CgroupMemory(None, 900 * MIB, 2), CgroupMemory(2 * GIB, GIB, 2),
        CgroupMemory(4 * GIB, 3 * GIB + 512 * MIB, 1),
    ))
    result = resolve_resources(observations=facts)
    assert result.available_headroom == 512 * MIB
    assert result.reserved_headroom == 512 * MIB // 5
    assert result.effective_max_ram == 64 * MIB + 512 * MIB - 512 * MIB // 5
    assert "cgroup-v2:unlimited" in result.detection_sources
    assert "cgroup-v1:headroom" in result.detection_sources


def test_macos_ignores_cgroups_and_exhausted_linux_headroom_is_zero():
    facts = observations(cgroups=(CgroupMemory(100, 200),))
    assert resolve_resources(observations=facts).available_headroom == 0
    result = resolve_resources(observations=replace(facts, platform="darwin"))
    assert result.available_headroom == 10 * GIB
    assert "cgroup:not-applicable" in result.detection_sources


def test_inspection_does_not_require_an_execution_terminal():
    facts = observations(coordinator_rss=16 * MIB)
    result = resolve_resources(32 * MIB, observations=facts)
    assert result.terminal_bytes == 8 * MIB
    with pytest.raises(ResourceError, match="terminal allowance"):
        resolve_resources(32 * MIB, observations=facts, require_execution=True)
    with pytest.raises(ResourceError, match="baseline"):
        resolve_resources(15 * MIB, observations=facts)


def test_worker_request_does_not_change_canonical_terminal():
    one = resolve_resources(GIB, 1, observations=observations())
    many = resolve_resources(GIB, 100, observations=observations())
    assert one.terminal_bytes == many.terminal_bytes
    assert one.worker_limit == 1
    assert many.worker_limit == 8
    assert many.requested_workers == 100


@pytest.mark.parametrize("field,value", [("max_ram", 0), ("max_ram", 1.2), ("workers", 0), ("workers", True), ("max_ram", U64_MAX + 1)])
def test_invalid_resource_arguments(field, value):
    with pytest.raises(ConfigurationError):
        resolve_resources(**{field: value}, observations=observations())


def test_descriptor_allowance_and_conservative_fallback():
    assert resolve_resources(observations=observations()).descriptor_allowance == 1024
    result = resolve_resources(observations=observations(descriptor_soft_limit=100, open_descriptors=10))
    assert result.descriptor_allowance == 58
    result = resolve_resources(observations=observations(descriptor_soft_limit=None, open_descriptors=None))
    assert result.descriptor_allowance == 928
    facts = observations(descriptor_soft_limit=39, open_descriptors=0)
    assert resolve_resources(observations=facts).descriptor_allowance == 7
    with pytest.raises(ResourceError, match="descriptor allowance"):
        resolve_resources(observations=facts, require_execution=True)


def test_terminal_exact_boundaries():
    working = 32 * MIB + 64 * MIB + 128 * 32768
    assert terminal_fits(working, 32768, 64 * MIB)
    assert not terminal_fits(working - 1, 32768, 64 * MIB)
    assert not terminal_fits(128 * MIB, 32769, 64 * MIB)
    assert not terminal_fits(128 * MIB, 32768, 64 * MIB + 1)
    assert not terminal_fits(32 * MIB, 1, 0)
    with pytest.raises(ConfigurationError):
        terminal_fits(128 * MIB, -1, 0)


def test_execution_checks_maximum_frame_and_remaining_baseline():
    facts = observations()
    resolve_resources(GIB, observations=facts, require_execution=True)
    with pytest.raises(ResourceError, match="maximum-size pair"):
        resolve_resources(GIB, observations=facts, max_record_bytes=32 * MIB, require_execution=True)
    with pytest.raises(ConfigurationError, match="u32"):
        resolve_resources(GIB, observations=facts, max_record_bytes=1 << 32, require_execution=True)
    with pytest.raises(ResourceError, match="baseline and one terminal"):
        resolve_resources(256 * MIB, observations=observations(coordinator_rss=250 * MIB), require_execution=True)


@pytest.mark.parametrize("raw,version,expected", [("max\n", 2, None), ("9223372036854771712\n", 1, None), ("4096", 1, 4096), ("0", 2, 0)])
def test_cgroup_limit_values(raw, version, expected):
    assert parse_cgroup_limit(raw, version) == expected


@pytest.mark.parametrize("raw", ["-1", "1.5", "NaN", "18446744073709551616", "", "1 2"])
def test_cgroup_bad_limit(raw):
    with pytest.raises(ResourceError):
        parse_cgroup_limit(raw, 2)


def _mock_cgroup(tmp_path: Path, version: int):
    proc = tmp_path / "proc"
    (proc / "self").mkdir(parents=True)
    mount = tmp_path / "cgroup with space"
    leaf = mount / "parent" / "leaf"
    leaf.mkdir(parents=True)
    encoded_mount = str(mount).replace(" ", r"\040")
    if version == 2:
        membership = "0::/parent/leaf\n"
        mountinfo = f"1 0 0:1 / {encoded_mount} rw - cgroup2 cgroup rw\n"
        names = ("memory.max", "memory.current")
    else:
        membership = "8:memory:/parent/leaf\n"
        mountinfo = f"1 0 0:1 / {encoded_mount} rw - cgroup cgroup rw,memory\n"
        names = ("memory.limit_in_bytes", "memory.usage_in_bytes")
    (proc / "self/cgroup").write_text(membership)
    (proc / "self/mountinfo").write_text(mountinfo)
    for directory, limit, usage in ((leaf, 1000, 200), (leaf.parent, 1200, 1100), (mount, 5000, 2000)):
        (directory / names[0]).write_text(str(limit))
        (directory / names[1]).write_text(str(usage))
    return proc


@pytest.mark.parametrize("version", [1, 2])
def test_detector_walks_visible_ancestors_and_decodes_mount_escapes(tmp_path, version):
    detected = resources._cgroup_memory(_mock_cgroup(tmp_path, version))
    assert len(detected) == 3
    assert {group.version for group in detected} == {version}
    assert resolve_resources(observations=observations(cgroups=detected)).available_headroom == 100


def test_detector_bounds_observation_files_and_rejects_bad_usage(tmp_path):
    proc = _mock_cgroup(tmp_path, 2)
    (tmp_path / "cgroup with space/parent/leaf/memory.current").write_text("max")
    with pytest.raises(ResourceError, match="usage"):
        resources._cgroup_memory(proc)
    (proc / "self/cgroup").write_bytes(b"x" * (resources._OBSERVATION_FILE_LIMIT + 1))
    with pytest.raises(ResourceError, match="byte limit"):
        resources._cgroup_memory(proc)


def test_checked_counters_reject_overflow_and_negative_values():
    assert checked_add(U64_MAX, 0) == U64_MAX
    assert checked_multiply(U64_MAX, 1) == U64_MAX
    with pytest.raises(ResourceError):
        checked_add(U64_MAX, 1)
    with pytest.raises(ResourceError):
        checked_multiply(U64_MAX, 2)
    with pytest.raises(ConfigurationError):
        checked_add(-1, 1)


def registry_bytes(count):
    return resources.BUDGET_REGISTRY_BYTES + count * resources.LEASE_REGISTRY_BYTES if count else 0


def test_leases_never_oversubscribe_and_release_on_cancellation():
    budget = Budget(MIB, descriptors=3, baseline_bytes=10)
    with budget.acquire("parent", 20, 1):
        child_capacity = budget.remaining_bytes - resources.LEASE_REGISTRY_BYTES
        with pytest.raises(ResourceError):
            budget.acquire("worker", child_capacity + 1)
        with pytest.raises(ResourceError):
            budget.acquire("worker", 1, 3)
        with pytest.raises(KeyboardInterrupt):
            with budget.acquire("worker", child_capacity, 2):
                assert budget.used_bytes == MIB
                assert budget.used_descriptors == 3
                raise KeyboardInterrupt
        assert budget.used_bytes == 30 + registry_bytes(1)
        assert budget.used_descriptors == 1
    assert budget.used_bytes == 10
    assert budget.used_metadata_bytes == budget.used_descriptors == budget.active_leases == 0


def test_transfer_keeps_reservation_and_release_is_idempotent():
    budget = Budget(MIB)
    lease = budget.acquire("worker", MIB - registry_bytes(1))
    successor = lease.transfer("completed_result")
    assert successor.owner == "completed_result"
    assert lease.released
    assert budget.remaining_bytes == 0
    with pytest.raises(ResourceError):
        budget.acquire("worker", 1)
    lease.release()
    assert budget.remaining_bytes == 0
    successor.release()
    successor.release()
    assert budget.remaining_bytes == MIB
    with pytest.raises(ResourceError):
        lease.transfer("worker")


def test_allocation_failure_leaves_counters_unchanged(monkeypatch):
    budget = Budget(MIB, baseline_bytes=10)

    def fail(*args):
        raise MemoryError

    monkeypatch.setattr(resources, "Lease", fail)
    with pytest.raises(MemoryError):
        budget.acquire("native", 90, 1)
    assert budget.used_bytes == 10
    assert budget.used_metadata_bytes == budget.used_descriptors == budget.active_leases == 0


def test_lease_registry_itself_is_bounded():
    budget = Budget(MIB, max_leases=2)
    first = budget.acquire("first", 1)
    second = budget.acquire("second", 1)
    with pytest.raises(ResourceError, match="registry"):
        budget.acquire("third", 1)
    first.release()
    second.release()
    with pytest.raises(ConfigurationError):
        budget.acquire("empty", 0)


def test_concurrent_acquisition_is_atomic():
    budget = Budget(MIB, metadata_limit_bytes=registry_bytes(10))

    def acquire(_):
        try:
            return budget.acquire("worker", 1)
        except ResourceError:
            return None

    with ThreadPoolExecutor(max_workers=8) as executor:
        leases = list(executor.map(acquire, range(50)))
    assert sum(lease is not None for lease in leases) == 10
    assert budget.used_bytes == 10 + registry_bytes(10)
    assert budget.used_metadata_bytes == registry_bytes(10)
    for lease in leases:
        if lease is not None:
            lease.release()
    assert budget.used_bytes == budget.used_metadata_bytes == 0


def test_pressure_checkpoint_limits_future_admission_without_changing_topology():
    plan = resolve_resources(GIB, observations=observations())
    budget = Budget(plan.effective_max_ram, baseline_bytes=plan.coordinator_rss)
    original_terminal = plan.terminal_bytes
    original_metadata = budget.metadata_limit_bytes
    admitted = registry_bytes(1) + 80
    budget.checkpoint(plan, observations(available_memory=admitted * 5 // 4))
    assert budget.remaining_bytes == admitted
    with pytest.raises(ResourceError):
        budget.acquire("worker", 81)
    assert plan.terminal_bytes == original_terminal
    with budget.acquire("native", 80):
        with pytest.raises(ResourceError, match="external memory pressure"):
            budget.checkpoint(plan, observations(available_memory=0))
    assert plan.terminal_bytes == original_terminal
    assert budget.metadata_limit_bytes == original_metadata


def test_full_frame_u32_bound_uses_shared_config_limit():
    from readmixer_py.config import MAX_RECORD_BYTES
    plan = resolve_resources(GIB, observations=observations())
    with pytest.raises(ConfigurationError, match="complete frame u32"):
        plan.require_execution(MAX_RECORD_BYTES + 1)


def test_checkpoint_accounts_for_measured_rss_beyond_ledger():
    facts = observations(coordinator_rss=64 * MIB)
    plan = resolve_resources(256 * MIB, observations=facts)
    budget = Budget(plan.effective_max_ram, baseline_bytes=plan.coordinator_rss)
    budget.checkpoint(plan, replace(facts, coordinator_rss=200 * MIB))
    assert budget.remaining_bytes == 56 * MIB
    with pytest.raises(ResourceError):
        budget.acquire("worker", 56 * MIB + 1)
    with pytest.raises(ResourceError, match="external memory pressure"):
        budget.checkpoint(plan, replace(facts, coordinator_rss=257 * MIB))
    assert budget.remaining_bytes == 0


def test_transferred_result_outlives_producer_context():
    budget = Budget(MIB)
    with budget.acquire("producer", MIB - registry_bytes(1)) as original:
        result = original.transfer("consumer")
    assert budget.used_bytes == MIB
    assert budget.active_leases == 1
    with pytest.raises(ResourceError):
        budget.acquire("next_producer", 1)
    result.release()
    assert budget.used_bytes == 0


def test_transfer_allocation_failure_keeps_original_ownership(monkeypatch):
    budget = Budget(MIB)
    original = budget.acquire("producer", MIB - registry_bytes(1))

    def fail(*args):
        raise MemoryError

    monkeypatch.setattr(resources, "Lease", fail)
    with pytest.raises(MemoryError):
        original.transfer("consumer")
    assert not original.released
    assert budget.used_bytes == MIB
    assert budget.active_leases == 1
    original.release()
    assert budget.used_bytes == 0


def dimensions(budget):
    return (budget.used_bytes, budget.used_descriptors, budget.used_metadata_bytes,
            budget.active_leases, budget.peak_metadata_bytes)


def test_P11_T01_shared_metadata_exact_boundary_and_subset_accounting():
    overhead = registry_bytes(2)
    budget = Budget(MIB, metadata_limit_bytes=overhead + 900)
    first = budget.acquire('catalog', 2000, metadata_bytes=500)
    assert budget.used_bytes == 2000 + registry_bytes(1)
    assert budget.used_metadata_bytes == 500 + registry_bytes(1)
    before = dimensions(budget)
    with pytest.raises(ResourceError, match='metadata bytes'):
        budget.acquire('ownership', 1000, metadata_bytes=401)
    assert dimensions(budget) == before
    second = budget.acquire('ownership', 1000, metadata_bytes=400)
    assert budget.used_bytes == 3000 + overhead
    assert budget.used_metadata_bytes == budget.metadata_limit_bytes
    assert budget.remaining_metadata_bytes == 0
    peak = budget.peak_metadata_bytes
    first.release()
    second.release()
    assert dimensions(budget) == (0, 0, 0, 0, peak)


@pytest.mark.parametrize('value', [-1, True, 1.5, MIB // 16 + 1, U64_MAX + 1])
def test_P11_T01_invalid_shared_metadata_limits(value):
    with pytest.raises(ConfigurationError):
        Budget(MIB, metadata_limit_bytes=value)


@pytest.mark.parametrize('value', [-1, True, 1.5, 101, U64_MAX + 1])
def test_P11_T01_metadata_must_be_an_integer_subset(value):
    budget = Budget(MIB)
    before = dimensions(budget)
    with pytest.raises(ConfigurationError):
        budget.acquire('invalid', 100, metadata_bytes=value)
    assert dimensions(budget) == before


def test_P11_T01_metadata_default_and_bookkeeping_are_admitted():
    assert Budget(GIB).metadata_limit_bytes == 64 * MIB
    assert Budget(MIB).metadata_limit_bytes == MIB // 16
    for budget in (Budget(10), Budget(MIB, metadata_limit_bytes=0),
                   Budget(MIB, metadata_limit_bytes=registry_bytes(1) - 1)):
        with pytest.raises(ResourceError):
            budget.acquire('payload', 1)
        assert dimensions(budget) == (0, 0, 0, 0, 0)
    budget = Budget(MIB, metadata_limit_bytes=registry_bytes(1))
    with budget.acquire('payload', 1):
        assert budget.used_bytes == registry_bytes(1) + 1
        assert budget.used_metadata_bytes == budget.registry_bytes == registry_bytes(1)
    assert budget.registry_bytes == budget.used_metadata_bytes == 0


def test_P11_T02_resize_and_split_preserve_all_ownership_dimensions():
    budget = Budget(MIB, descriptors=5)
    parent = budget.acquire('parent', 4000, 3, metadata_bytes=2000)
    assert parent.resize(5000, 4, metadata_bytes=3000) is parent
    assert (parent.bytes, parent.descriptors, parent.metadata_bytes) == (5000, 4, 3000)
    before = dimensions(budget)
    child = parent.split('child', 2000, 2, metadata_bytes=1500)
    assert (parent.bytes, parent.descriptors, parent.metadata_bytes) == (3000, 2, 1500)
    assert (child.bytes, child.descriptors, child.metadata_bytes) == (2000, 2, 1500)
    assert budget.used_bytes == before[0] + resources.LEASE_REGISTRY_BYTES
    assert budget.used_metadata_bytes == before[2] + resources.LEASE_REGISTRY_BYTES
    successor = child.transfer('consumer')
    assert child.released and not successor.released
    assert successor.metadata_bytes == 1500
    child.release()
    parent.resize(0)
    assert parent.bytes == parent.descriptors == parent.metadata_bytes == 0
    assert not parent.released  # The owning handle still has admitted bookkeeping.
    parent.release()
    successor.release()
    assert budget.used_bytes == budget.used_metadata_bytes == budget.used_descriptors == 0


@pytest.mark.parametrize('amounts', [(4001, 0, 0), (1, 4, 0), (100, 0, 101),
                                    (2500, 0, 0), (100, 0, 2001)])
def test_P11_T02_invalid_split_does_not_mutate_parent(amounts):
    budget = Budget(MIB)
    parent = budget.acquire('parent', 4000, 3, metadata_bytes=2000)
    before = dimensions(budget)
    with pytest.raises(ConfigurationError):
        parent.split('child', amounts[0], amounts[1], metadata_bytes=amounts[2])
    assert dimensions(budget) == before
    assert (parent.bytes, parent.descriptors, parent.metadata_bytes) == (4000, 3, 2000)
    parent.release()


@pytest.mark.parametrize('dimension', ['ram', 'metadata', 'descriptors', 'registry'])
def test_P11_T02_failed_resize_or_split_preserves_original(dimension):
    budget = Budget(MIB, descriptors=2, max_leases=1 if dimension == 'registry' else 10,
                    metadata_limit_bytes=registry_bytes(1) + 500)
    parent = budget.acquire('parent', 1000, 1, metadata_bytes=500)
    before = dimensions(budget)
    with pytest.raises(ResourceError):
        if dimension == 'ram':
            parent.resize(MIB, 1, metadata_bytes=500)
        elif dimension == 'metadata':
            parent.resize(1000, 1, metadata_bytes=501)
        elif dimension == 'descriptors':
            parent.resize(1000, 3, metadata_bytes=500)
        else:
            parent.split('child', 100, metadata_bytes=100)
    assert dimensions(budget) == before
    assert (parent.bytes, parent.descriptors, parent.metadata_bytes) == (1000, 1, 500)
    parent.release()


@pytest.mark.parametrize('operation', ['acquire', 'transfer', 'resize', 'split', 'release'])
@pytest.mark.parametrize('after_commit', [False, True])
def test_P11_T02_interrupted_commit_has_one_owner_and_unchanged_counters(monkeypatch, operation, after_commit):
    budget = Budget(MIB)
    original = budget.acquire('original', 5000, 2, metadata_bytes=2000)
    before = dimensions(budget)
    real_publish = budget._publish_state

    def interrupted(state):
        if after_commit:
            real_publish(state)
        raise KeyboardInterrupt

    monkeypatch.setattr(budget, '_publish_state', interrupted)
    with pytest.raises(KeyboardInterrupt):
        if operation == 'acquire':
            budget.acquire('new', 1000, 1, metadata_bytes=1000)
        elif operation == 'transfer':
            original.transfer('new')
        elif operation == 'resize':
            original.resize(6000, 3, metadata_bytes=3000)
        elif operation == 'split':
            original.split('new', 1000, 1, metadata_bytes=1000)
        else:
            original.release()
    assert dimensions(budget) == before
    assert not original.released
    assert (original.owner, original.bytes, original.descriptors, original.metadata_bytes) == ('original', 5000, 2, 2000)
    monkeypatch.setattr(budget, '_publish_state', real_publish)
    original.release()
    assert budget.used_bytes == budget.used_metadata_bytes == budget.active_leases == 0


@pytest.mark.parametrize('operation', ['acquire', 'transfer', 'resize', 'split', 'release'])
def test_P11_T02_state_allocation_failure_preserves_original(monkeypatch, operation):
    budget = Budget(MIB)
    original = budget.acquire('original', 5000, 2, metadata_bytes=2000)
    before = dimensions(budget)
    real_state = resources._BudgetState

    def fail(*args):
        raise MemoryError

    monkeypatch.setattr(resources, '_BudgetState', fail)
    with pytest.raises(MemoryError):
        if operation == 'acquire':
            budget.acquire('new', 1000, 1, metadata_bytes=1000)
        elif operation == 'transfer':
            original.transfer('new')
        elif operation == 'resize':
            original.resize(6000, 3, metadata_bytes=3000)
        elif operation == 'split':
            original.split('new', 1000, 1, metadata_bytes=1000)
        else:
            original.release()
    assert dimensions(budget) == before
    assert not original.released
    assert (original.bytes, original.descriptors, original.metadata_bytes) == (5000, 2, 2000)
    monkeypatch.setattr(resources, '_BudgetState', real_state)
    original.release()


def test_P11_T02_concurrent_metadata_growth_is_atomic():
    budget = Budget(MIB, metadata_limit_bytes=registry_bytes(8) + 1000)
    leases = [budget.acquire('owner', 1000) for _ in range(8)]

    def grow(lease):
        try:
            lease.resize(1000, metadata_bytes=200)
            return True
        except ResourceError:
            return False

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(grow, leases))
    assert sum(results) == 5
    assert budget.used_metadata_bytes == budget.metadata_limit_bytes
    assert sum(lease.metadata_bytes for lease in leases) == 1000
    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(lambda lease: lease.release(), leases))
    assert budget.used_bytes == budget.used_metadata_bytes == budget.active_leases == 0


def test_P11_T02_released_handles_cannot_resize_or_split():
    budget = Budget(MIB)
    original = budget.acquire('original', 100, metadata_bytes=50)
    original.release()
    before = dimensions(budget)
    with pytest.raises(ResourceError):
        original.resize(200, metadata_bytes=100)
    with pytest.raises(ResourceError):
        original.split('new', 10, metadata_bytes=10)
    assert dimensions(budget) == before
