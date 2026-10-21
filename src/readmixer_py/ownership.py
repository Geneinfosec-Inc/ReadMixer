"""Bounded authenticated ownership of registered outputs and successful links.

Only generated basenames and checked filesystem facts enter these private
records. Prepared tickets are bounded RAM intents; their ``committed`` property
reads the ledger's canonical append boundary, including interrupted handoffs.
"""

from contextlib import contextmanager
from dataclasses import dataclass
import re
import struct

from .errors import ConfigurationError, IntegrityError, ResourceError
from .ledger import PagedLedger

_MAGIC = b'RMPYOWN1'
_PREFIX = struct.Struct('>8sBBQH')
_FACTS = struct.Struct('>QQQ32sqq')
_BASENAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9._-]{0,254}', re.ASCII)
_U64 = (1 << 64) - 1
_I64_MIN, _I64_MAX = -(1 << 63), (1 << 63) - 1
MAX_RECORD_BYTES = 2048
INVENTORY, LINK = 1, 2


@dataclass(frozen=True, slots=True)
class OwnershipRecord:
    ordinal: int
    name: str
    device: int
    inode: int
    length: int
    digest: bytes
    mtime_ns: int
    ctime_ns: int


def _validate(record, *, error):
    if not isinstance(record, OwnershipRecord):
        raise error('ownership record has an invalid type')
    if not isinstance(record.name, str) or _BASENAME.fullmatch(record.name) is None:
        raise error('ownership record requires a bounded generated basename')
    for value in (record.ordinal, record.device, record.inode, record.length):
        if type(value) is not int or not 0 <= value <= _U64:
            raise error('ownership unsigned field exceeds its width')
    for value in (record.mtime_ns, record.ctime_ns):
        if type(value) is not int or not _I64_MIN <= value <= _I64_MAX:
            raise error('ownership timestamp exceeds its signed width')
    if type(record.digest) is not bytes or len(record.digest) != 32:
        raise error('ownership digest must be a complete SHA-256 value')


def encode_record(record, kind=INVENTORY):
    _validate(record, error=ConfigurationError)
    if type(kind) is not int or kind not in (INVENTORY, LINK):
        raise ConfigurationError('ownership record kind is invalid')
    name = record.name.encode('ascii')
    data = (_PREFIX.pack(_MAGIC, 1, kind, record.ordinal, len(name)) + name
            + _FACTS.pack(record.device, record.inode, record.length, record.digest,
                          record.mtime_ns, record.ctime_ns))
    if len(data) > MAX_RECORD_BYTES:
        raise ResourceError('ownership record exceeds its encoded allowance')
    return data


def decode_record(data, *, kind=None):
    if type(data) is not bytes or not _PREFIX.size + _FACTS.size + 1 <= len(data) <= MAX_RECORD_BYTES:
        raise IntegrityError('ownership record length is invalid')
    magic, version, observed_kind, ordinal, name_bytes = _PREFIX.unpack_from(data)
    if (magic != _MAGIC or version != 1 or observed_kind not in (INVENTORY, LINK)
            or kind is not None and observed_kind != kind):
        raise IntegrityError('ownership record version or kind is invalid')
    if not 1 <= name_bytes <= 255 or len(data) != _PREFIX.size + name_bytes + _FACTS.size:
        raise IntegrityError('ownership record basename length or trailing bytes are invalid')
    try:
        name = data[_PREFIX.size:_PREFIX.size + name_bytes].decode('ascii')
    except UnicodeError as exc:
        raise IntegrityError('ownership basename is not ASCII') from exc
    facts = _FACTS.unpack_from(data, _PREFIX.size + name_bytes)
    record = OwnershipRecord(ordinal, name, *facts)
    _validate(record, error=IntegrityError)
    return record


class AppendTicket:
    """At most one prepared ticket per lane; committed authority never reverts."""

    __slots__ = ('_journal', '_ledger', '_sequence', '_discarded', 'record', 'encoded', 'kind')

    def __init__(self, journal, ledger, record, kind):
        self._journal, self._ledger = journal, ledger
        self._sequence = ledger.owned_count
        self._discarded = False
        self.record, self.kind = record, kind
        self.encoded = encode_record(record, kind)

    @property
    def committed(self):
        return not self._discarded and self._ledger.owned_count > self._sequence


class OutputOwnershipJournal:
    """Two admitted ledgers; no whole-run path, token, or completion registry.

    Inventory registration follows dense creation ordinals, starting at zero.
    Final links follow the reverse inventory traversal. Both contracts are checked
    again against authenticated ledger positions before publication or cleanup.
    """

    def __init__(self, store, *, cancel=None, max_records=100_000):
        if type(max_records) is not int or not 1 <= max_records <= 100_000:
            raise ConfigurationError('ownership inventory limit must be in 1..100000')
        self.store, self.cancel, self.max_records = store, cancel, max_records
        self._lease = store.budget.acquire('output-journal', 16384, metadata_bytes=16384)
        self.inventory = self.links = None
        self._prepared = {INVENTORY: None, LINK: None}
        self._frozen = self._closed = False
        try:
            self.inventory = PagedLedger(store, page_bytes=65536, max_record_bytes=MAX_RECORD_BYTES,
                                         cancel=cancel, owns_records=True)
            self.links = PagedLedger(store, page_bytes=65536, max_record_bytes=MAX_RECORD_BYTES,
                                     cancel=cancel, owns_records=True)
        except BaseException as primary:
            try:
                if self.inventory is not None:
                    self.inventory.close()
            except BaseException as cleanup:
                primary.add_note(f'ownership journal construction cleanup failed: {type(cleanup).__name__}')
            finally:
                self._lease.release()
            raise

    @property
    def _inventory(self):
        return self.inventory

    @property
    def _links(self):
        return self.links

    @property
    def inventory_count(self):
        return self.inventory.owned_count

    @property
    def link_count(self):
        return self.links.owned_count

    def _check(self):
        if self._closed:
            raise ResourceError('output ownership journal is closed')

    def _prepare(self, record, kind):
        self._check()
        if kind == INVENTORY and self._frozen:
            raise ResourceError('output ownership inventory is frozen')
        if kind == LINK and not self._frozen:
            raise ResourceError('output inventory must freeze before final linking')
        ledger = self.inventory if kind == INVENTORY else self.links
        previous = self._prepared[kind]
        if previous is not None and not previous.committed and not previous._discarded:
            raise ResourceError('an output ownership handoff is already pending')
        _validate(record, error=ConfigurationError)
        if record.ordinal >= self.max_records or ledger.owned_count >= self.max_records:
            raise ResourceError('ownership record exceeds the admitted artifact inventory')
        if kind == LINK and ledger.owned_count >= self.inventory_count:
            raise IntegrityError('successful links exceed the registered inventory')
        expected = ledger.owned_count if kind == INVENTORY else self.inventory_count - ledger.owned_count - 1
        if record.ordinal != expected:
            raise IntegrityError('ownership handoff ordinal differs from its registration order')
        ticket = AppendTicket(self, ledger, record, kind)
        self._prepared[kind] = ticket
        return ticket

    def prepare_inventory(self, record):
        return self._prepare(record, INVENTORY)

    def prepare_link(self, record):
        return self._prepare(record, LINK)

    def commit(self, ticket):
        self._check()
        if not isinstance(ticket, AppendTicket) or ticket._journal is not self:
            raise ConfigurationError('ownership ticket belongs to another journal')
        if ticket.committed:
            return
        if self._prepared[ticket.kind] is not ticket or ticket._sequence != ticket._ledger.owned_count:
            raise IntegrityError('ownership ticket is not the current pending handoff')
        ticket._ledger.append(ticket.encoded)
        # The canonical ledger boundary already transferred authority. This
        # housekeeping assignment may be interrupted without losing ownership.
        self._prepared[ticket.kind] = None

    def discard(self, ticket):
        """Cancel only an uncommitted intent; never discard journal ownership."""
        if not isinstance(ticket, AppendTicket) or ticket._journal is not self:
            raise ConfigurationError('ownership ticket belongs to another journal')
        if not ticket.committed and self._prepared[ticket.kind] is ticket:
            ticket._discarded = True
            self._prepared[ticket.kind] = None

    def _decode(self, data, kind, position=None):
        record = decode_record(data, kind=kind)
        if record.ordinal >= self.max_records:
            raise IntegrityError('ownership ordinal exceeds the admitted artifact inventory')
        if position is not None:
            expected = position if kind == INVENTORY else self.inventory_count - position - 1
            if record.ordinal != expected:
                raise IntegrityError('ownership record ordinal differs from its ledger position')
        return record

    @contextmanager
    def _iterate(self, ledger, kind):
        self._check()
        decoded = None
        with ledger.open_reverse() as records:
            try:
                decoded = (self._decode(record, kind, ledger.owned_count - index - 1)
                           for index, record in enumerate(records))
                yield decoded
                # Early normal exit still validates every remaining record codec.
                for _ in decoded:
                    pass
            finally:
                if decoded is not None:
                    decoded.close()
                decoded = None

    def freeze_inventory(self):
        self._check()
        pending = self._prepared[INVENTORY]
        if pending is not None and not pending.committed:
            raise ResourceError('output inventory still has a pending handoff')
        with self._iterate(self.inventory, INVENTORY) as records:
            count = sum(1 for _ in records)
        if count != self.inventory_count:
            raise IntegrityError('ownership inventory count differs from committed handoffs')
        self._frozen = True

    def iter_inventory(self):
        if not self._frozen:
            raise ResourceError('output ownership inventory is not frozen')
        return self._iterate(self.inventory, INVENTORY)

    def iter_links(self):
        return self._iterate(self.links, LINK)

    def flush_links(self):
        self._check()
        self.links.flush()

    def validate_links(self):
        pending = self._prepared[LINK]
        if pending is not None and not pending.committed:
            raise ResourceError('final linking still has a pending handoff')
        if self.link_count != self.inventory_count:
            raise IntegrityError('successful links do not cover the registered inventory')
        with self._iterate(self.links, LINK) as records:
            count = sum(1 for _ in records)
        if count != self.link_count:
            raise IntegrityError('ownership link count differs from committed handoffs')

    def _drain(self, ledger, kind, callback):
        def decode(data, position=None):
            return self._decode(data, kind, position)

        ledger.drain_owned(lambda data, progress: callback(decode(data), progress), validate=decode)

    def drain_links(self, callback):
        self._drain(self.links, LINK, callback)

    def drain_inventory(self, callback):
        self._drain(self.inventory, INVENTORY, callback)

    def close(self):
        if self._closed:
            return
        if self.inventory.owned_remaining or self.links.owned_remaining:
            raise ResourceError('output ownership must be drained before close')
        self.links.close()
        self.inventory.close()
        self._prepared.clear()
        self._lease.release()
        self._closed = True

    def abandon(self):
        """Terminal teardown only: release RAM while preserving uncertain residue."""
        if self._closed:
            return
        primary = None
        for ledger in (self.links, self.inventory):
            try:
                ledger.abandon()
            except BaseException as error:
                if primary is None:
                    primary = error
        self._prepared.clear()
        self._lease.release()
        self._closed = True
        if primary is not None:
            raise primary
