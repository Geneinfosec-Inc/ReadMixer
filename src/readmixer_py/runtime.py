"""Shared serial operation admission and safe destination creation."""

import os

from .errors import CancelledError, PublicationError
from .paths import exclude_input_tree, open_directory, validate_basename
from .resources import Budget, resolve_resources


def operation_resources(config, *, record_bytes=1 << 20, execution=False):
    checkpoint(config)
    plan = resolve_resources(config.max_ram, config.workers, require_execution=execution,
                             max_record_bytes=record_bytes)
    budget = Budget(plan.effective_max_ram, descriptors=plan.descriptor_allowance,
                    baseline_bytes=plan.coordinator_rss, metadata_limit_bytes=plan.metadata_bytes)
    return plan, budget


def checkpoint(config, budget=None, plan=None):
    if config.cancel is not None and config.cancel():
        raise CancelledError()
    if budget is not None:
        budget.checkpoint(plan)


def prepare_destination(path, *, input_root):
    exclude_input_tree(path, input_root)
    if not path.exists():
        with open_directory(path.parent) as parent:
            validate_basename(path.name)
            try:
                os.mkdir(path.name, 0o700, dir_fd=parent.fd)
                os.fsync(parent.fd)
            except FileExistsError:
                pass
            except OSError as exc:
                raise PublicationError('cannot create the destination directory') from exc
    with open_directory(path, private_destination=True, input_root=input_root):
        pass


def codec_description(level):
    return dict(format='bgzf', compression_level=level, payload_bytes=65280,
                max_member_bytes=65536, eof_bytes=28, digest_algorithm='sha256')


def execution_description(config, plan, compression=None, stage=None):
    # Requested concurrency is an upper bound; report the admitted partition.
    return dict(profile_version=1, memory_budget_bytes=plan.effective_max_ram,
                terminal_slice_bytes=plan.terminal_bytes, storage_mode=config.storage.value,
                workers_effective=stage.workers_effective if stage is not None else 1,
                cpu_budget=compression.actual_cpu_slots if compression else (stage.workers_effective if stage is not None else 1),
                descriptor_limit=plan.descriptor_allowance,
                compression_slots=compression.slots if compression else 0)


def stage_execution(config, plan, budget, store, cpu, *, operation, task_count, record_bytes, scatter=True, scatter_batch_bytes=64 << 20):
    """Admit one production scheduler while preserving coordinator progress space.

    Serial operation does not reserve dormant worker arenas. Large records still
    use the same bounded local sorter; workers process ordinary half-MiB runs.
    The placeholder protects scan, merge, and publication overlap while choosing
    the fixed partition, then their concrete owners acquire those reservations.
    """
    from contextlib import contextmanager
    from .stage_scheduler import StageScheduler
    from .errors import ResourceError

    @contextmanager
    def admitted():
        if cpu.effective_limit == 1 or not task_count:
            yield None
            return
        runway = max((256 if operation == 'mix' else 128) << 20, 12 * (2 * record_bytes + 40))
        metadata_runway = (8 if operation == 'mix' else 2) << 20
        # A single complete child needs two production job windows, its READY
        # baseline, the interpreter helper, and transport/control metadata.
        if (budget.remaining_bytes < runway + (224 << 20)
                or budget.metadata_limit_bytes - budget.used_metadata_bytes < (19 << 20)):
            yield None
            return
        from .paircodec import maximum_frame_bytes
        batch = scatter_batch_bytes
        while True:
            with budget.acquire('stage-coordinator-runway', runway, metadata_bytes=metadata_runway):
                scheduler = StageScheduler(budget, cpu, store.keys.run_id, operation=operation,
                    task_count=task_count, store=store, cancel=config.cancel, production=True,
                    terminal_bytes=plan.terminal_bytes if operation == 'mix' else 0,
                    parser_record_bytes=record_bytes if operation == 'mix' else 0,
                    parser_scatter=operation == 'mix' and scatter,
                    parser_batch_bytes=batch if operation == 'mix' and scatter else None)
            if scheduler.process_slots:
                break
            scheduler.close()
            if operation != 'mix' or not scatter or batch == maximum_frame_bytes(record_bytes):
                yield None
                return
            batch = max(maximum_frame_bytes(record_bytes), batch // 2)
        with scheduler:
            yield scheduler
    return admitted()
