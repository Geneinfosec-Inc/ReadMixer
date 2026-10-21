"""Worker scatter RPC: exact derived capabilities and acknowledged generations."""
import hashlib
import struct

from .crypto import pairs_identity
from .errors import IntegrityError, ResourceError
from .storage import serialize_segment_ref, deserialize_segment_ref
from .worker_protocol import (ScatterMessage, ScatterPhase as Phase, Chunk,
                              ChunkAck, ResultAck, MAX_CHUNK_BYTES)
from .worker_storage import encode_grant, decode_write_grant


def send_generation(cap, task, grant, number, payload, begin, send, receive, check, *, run_id):
    from .worker_scatter import WorkerScatterGeneration
    if (type(begin) is not ScatterMessage or begin.phase is not Phase.BEGIN or
            (begin.task_id, begin.grant_id, begin.batch) != (task, grant, number)):
        raise IntegrityError('scatter generation was not admitted')
    def rpc(phase, bucket, *, offset=0, amount=0, expected=None):
        message = ScatterMessage(task, grant, number, phase, bucket, offset, amount)
        send(message)
        reply = receive()
        if (type(reply) is not ScatterMessage or reply.phase is not expected or
                (reply.task_id, reply.grant_id, reply.batch, reply.bucket) != (task, grant, number, bucket)):
            raise IntegrityError('scatter grant or disk credit was not acknowledged')
        if phase in (Phase.DISK_REQUEST, Phase.DISK_COMMIT) and (reply.offset, reply.amount) != (offset, amount):
            raise IntegrityError('scatter disk credit differs from its request')
        return reply
    grants = None
    if cap.scatter_mode == 'external':
        grants = tuple(decode_write_grant(rpc(Phase.GRANT_REQUEST, bucket, expected=Phase.GRANT).payload)
                       for bucket in range(64))
        if any(value.task_id != task or value.ref.run_id != run_id for value in grants):
            raise IntegrityError('scatter capability belongs to another parser task or invocation')
    def result(bucket, maximum, value, count):
        if value is None:
            message = ScatterMessage(task, grant, number, Phase.RESULT, bucket)
        elif cap.scatter_mode == 'external':
            message = ScatterMessage(task, grant, number, Phase.RESULT, bucket,
                amount=value.decoded_bytes, records=count, maximum=maximum,
                digest=value.digest, payload=serialize_segment_ref(value))
        else:
            message = ScatterMessage(task, grant, number, Phase.RESULT, bucket,
                amount=len(value), records=count, maximum=maximum, digest=hashlib.sha256(value).digest())
        send(message)
        if value is not None and cap.scatter_mode == 'memory':
            ack = receive()
            if not isinstance(ack, ChunkAck) or (ack.task_id, ack.grant_id, ack.end) != (task, grant, 0):
                raise IntegrityError('scatter raw transfer was not admitted')
            for offset in range(0, len(value), MAX_CHUNK_BYTES):
                part = bytes(value[offset:offset + MAX_CHUNK_BYTES])
                send(Chunk(task, grant, offset, part))
                ack = receive()
                if not isinstance(ack, ChunkAck) or (ack.task_id, ack.grant_id, ack.end) != (task, grant, offset + len(part)):
                    raise IntegrityError('scatter raw transfer was not acknowledged')
        ack = receive()
        if not isinstance(ack, ResultAck) or (ack.task_id, ack.grant_id) != (task, grant):
            raise IntegrityError('scatter ownership transfer was not acknowledged')
    with WorkerScatterGeneration(grants, producer=cap.producer, batch=begin.offset,
            bucket_key=cap.bucket_key, max_record_bytes=cap.max_record_bytes, batch_bytes=cap.batch_bytes,
            credit=lambda b, o, a: rpc(Phase.DISK_REQUEST, b, offset=o, amount=a, expected=Phase.DISK_CREDIT),
            commit=lambda b, o, a: rpc(Phase.DISK_COMMIT, b, offset=o, amount=a, expected=Phase.DISK_COMMITTED),
            checkpoint=check) as generation:
        cursor = 0
        while cursor < len(payload):
            length, = struct.unpack_from('<I', payload, cursor)
            generation.append(payload[cursor:cursor + length])
            cursor += length
        generation.finish(result)
    send(ScatterMessage(task, grant, number, Phase.DONE))
    ack = receive()
    if not isinstance(ack, ResultAck) or (ack.task_id, ack.grant_id) != (task, grant):
        raise IntegrityError('complete scatter generation was not acknowledged')


class ScatterReceiver:
    """A single active generation; ownership is journaled before sender release."""
    def __init__(self, source, store, manager, inventory, completed, generation):
        self.source, self.store, self.manager, self.inventory = source, store, manager, inventory
        self.completed, self.generation = completed, generation
        self.grants = [None] * 64
        self.requested = self.returned = 0
        self.result = self.output = None
        self.received = self.bytes = self.count = 0

    def _validate(self, message):
        if self.source.header is None or message.batch != self.source.header.batch:
            raise IntegrityError('scatter message refers to an unadmitted generation')

    def message(self, message, task, grant):
        self._validate(message)
        phase, bucket = message.phase, message.bucket
        if phase is Phase.GRANT_REQUEST:
            if self.manager is None or bucket != self.requested or self.returned or self.result is not None:
                raise IntegrityError('scatter writer grant is outside its generation')
            # Manager grant IDs are an operation-wide monotonic namespace.
            identity = self.manager.next_grant_id()
            issued = self.manager.issue_write(task, identity, 1,
                pairs_identity((bucket,), self.source.capability.producer, self.generation),
                self.source.capability.batch_bytes, self.source.capability.batch_bytes // 48)
            self.grants[bucket] = issued
            self.requested += 1
            return ScatterMessage(task, grant, message.batch, Phase.GRANT, bucket, payload=encode_grant(issued))
        if phase in (Phase.DISK_REQUEST, Phase.DISK_COMMIT):
            if self.manager is None or self.requested != 64 or self.result is not None or bucket < self.returned:
                raise IntegrityError('scatter write is outside its active writer set')
            issued = self.grants[bucket]
            if phase is Phase.DISK_REQUEST:
                self.manager.authorize_write(issued, message.offset, message.amount)
                response = Phase.DISK_CREDIT
            else:
                self.manager.commit_write(issued, message.offset, message.amount)
                response = Phase.DISK_COMMITTED
            return ScatterMessage(task, grant, message.batch, response, bucket, message.offset, message.amount)
        if phase is Phase.RESULT:
            if (bucket != self.returned or self.result is not None or
                    self.manager is not None and self.requested != 64 or
                    message.amount > self.source.header.output_bytes - self.bytes or
                    message.records > self.source.header.records - self.count):
                raise IntegrityError('scatter returned an unexpected bucket or statistics')
            if not message.amount:
                if message.payload or message.digest != bytes(32):
                    raise IntegrityError('empty scatter result contains unrelated ownership')
                if self.manager is not None:
                    self.manager.discard_empty_write(self.grants[bucket])
                self.returned += 1
                return ResultAck(task, grant)
            if self.manager is not None:
                ref = deserialize_segment_ref(message.payload)
                if (ref.decoded_bytes, ref.item_count, ref.digest) != (message.amount, message.records, message.digest):
                    raise IntegrityError('scatter sealed statistics differ from its capability')
                ref = self.manager.accept_write(self.grants[bucket], ref)
                self._accept_ref(message, ref)
                self.manager.take_write(self.grants[bucket])
                self.returned += 1
                return ResultAck(task, grant)
            if message.payload:
                raise IntegrityError('memory scatter attempted an external handoff')
            self.result, self.received = message, 0
            self.output = bytearray(message.amount)
            return ChunkAck(task, grant, 0)
        if phase is Phase.DONE:
            if self.returned != 64 or self.result is not None or (self.count, self.bytes) != (self.source.header.records, self.source.header.output_bytes):
                raise IntegrityError('scatter generation ended without complete statistics')
            self.source.finish_scatter()
            self.completed(self.count, self.bytes)
            self.grants[:] = [None] * 64
            return ResultAck(task, grant)
        raise IntegrityError('scatter response is invalid on a worker channel')

    def chunk(self, message):
        header = self.result
        if header is None or message.offset != self.received or len(message.payload) > header.amount - self.received:
            raise IntegrityError('scatter raw chunk exceeds its bounded result')
        self.output[self.received:self.received + len(message.payload)] = message.payload
        self.received += len(message.payload)
        if self.received != header.amount:
            return False
        if hashlib.sha256(self.output).digest() != header.digest:
            raise IntegrityError('scatter raw payload did not authenticate')
        with self.store.new_segment(1, pairs_identity((header.bucket,), self.source.capability.producer, self.generation)) as writer:
            writer.append(self.output, item_count=header.records)
            ref = writer.seal()
        # store retains its global entry if any subsequent handoff fails.
        self._accept_ref(header, ref)
        self.returned += 1
        self.output = self.result = None
        return True

    def _accept_ref(self, header, ref):
        count = maximum = 0
        with self.store.open_verified(ref) as reader:
            cursor = 0
            while cursor < ref.decoded_bytes:
                prefix = reader.read(4)
                if len(prefix) != 4:
                    raise IntegrityError('scatter frame prefix is truncated')
                length, = struct.unpack('<I', prefix)
                if not 48 <= length <= min(header.maximum, ref.decoded_bytes - cursor):
                    raise IntegrityError('scatter frame exceeds its admitted maximum')
                frame = bytearray(prefix)
                while len(frame) < length:
                    part = reader.read(min(1 << 20, length - len(frame)))
                    if not part:
                        raise IntegrityError('scatter frame is truncated')
                    frame.extend(part)
                self.source.accept_scatter_frame(frame, header.bucket)
                maximum = max(maximum, length)
                cursor += length
                count += 1
        if count != header.records or maximum != header.maximum:
            raise IntegrityError('scatter bucket statistics differ from its frames')
        self.inventory.import_batch_reference(header.bucket, self.source.capability.producer,
                                              self.generation, maximum, ref)
        self.bytes += ref.decoded_bytes
        self.count += count
