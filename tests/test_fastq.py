"""Independent byte fixtures for Plans 02's framing, codec and pairing contracts."""
from __future__ import annotations

import gzip
import hashlib
import io
import random
import unittest

from readmixer_py.errors import CancelledError, ConfigurationError, InputError, ResourceError
from readmixer_py.fastq import PairedReader, RecordFramer, header_identity, open_decoded_stream


def canonical(view):
    return b"".join(bytes(line) + b"\n" for line in view.lines)


def framer(data, *, mode="ingress", limit=1024 * 1024, chunk=65536):
    return RecordFramer(io.BytesIO(data), line_mode=mode,
                        max_record_bytes=limit, read_chunk_bytes=chunk)


class ForwardOnly(io.BytesIO):
    """Short reads expose framing boundaries without permitting a second pass."""

    def __init__(self, data, chunk=7):
        super().__init__(data)
        self.chunk = chunk
        self.read_calls = 0
        self.largest_request = 0

    def seek(self, *args):
        raise AssertionError("input must never seek")

    def readinto(self, destination):
        piece = self.read(len(destination))
        destination[:len(piece)] = piece
        return len(piece)

    def read(self, size=-1):
        if not 0 <= size <= 65536:
            raise AssertionError("unbounded source read")
        self.read_calls += 1
        self.largest_request = max(size, self.largest_request)
        return super().read(min(size, self.chunk))


class FramerTests(unittest.TestCase):
    def test_lossless_opaque_metadata_and_crlf(self):
        source = b"@id/1 long \xff header\r\nAC\x00\x81\r\n+entire \xfe annotation\r\n!~\x00\x81\r\n"
        expected = b"@id/1 long \xff header\nAC\x00\x81\n+entire \xfe annotation\n!~\x00\x81\n"
        for chunk in (1, 2, 3, 7, 65536):
            with self.subTest(chunk=chunk):
                reader = framer(source, chunk=chunk)
                view = reader.next_record()
                self.assertEqual(canonical(view), expected)
                self.assertEqual(view.normalized_bytes, len(expected))
                self.assertIsNone(reader.next_record())
                self.assertEqual(reader.record_count, 1)
                self.assertEqual(reader.decoded_bytes, len(source))
                self.assertEqual(reader.sha256, hashlib.sha256(expected).hexdigest())

    def test_AUD06_long_opaque_header_and_plus_are_lossless(self):
        # The predecessor truncated descriptions beyond 100 bytes. Independent
        # byte fixtures exceed both that cutoff and typical small read buffers.
        header = b"@long-id/1 " + (b"opaque \xff\x80\x00 metadata," * 410) + b" header-end"
        plus = b"+" + (b"plus \xfe\x81\x00 annotation;" * 410) + b" plus-end"
        self.assertGreater(len(header), 8192)
        self.assertGreater(len(plus), 8192)
        expected = header + b"\nACGT\n" + plus + b"\n!~#!\n"
        original = header + b"\r\nACGT\r\n" + plus + b"\r\n!~#!\r\n"
        for chunk in (7, 257, 65536):
            with self.subTest(chunk=chunk):
                reader = framer(original, limit=len(expected), chunk=chunk)
                record = reader.next_record()
                self.assertEqual(bytes(record.header), header)
                self.assertEqual(bytes(record.plus), plus)
                self.assertEqual(canonical(record), expected)
                self.assertEqual(record.normalized_bytes, len(expected))
                self.assertIsNone(reader.next_record())
                self.assertEqual(reader.normalized_bytes, len(expected))
                self.assertEqual(reader.sha256, hashlib.sha256(expected).hexdigest())

    def test_final_standalone_cr_survives_canonical_reread(self):
        original = b"@id\r\nA\rB\r\n+metadata\r\n!!\r"
        expected = b"@id\nA\rB\n+metadata\n!!\r\n"
        ingress = framer(original)
        self.assertEqual(canonical(ingress.next_record()), expected)
        self.assertIsNone(ingress.next_record())
        canonical_reader = framer(expected, mode="canonical_lf")
        self.assertEqual(canonical(canonical_reader.next_record()), expected)
        self.assertIsNone(canonical_reader.next_record())
        with self.assertRaises(InputError):
            framer(expected, mode="ingress").next_record()

    def test_crlf_strip_once_and_standalone_cr_payload(self):
        data = b"@id\r\r\nA\r\r\n+\r\r\n!\r\r\n"
        expected = b"@id\r\nA\r\n+\r\n!\r\n"
        self.assertEqual(canonical(framer(data).next_record()), expected)
        self.assertEqual(canonical(framer(data, mode="canonical_lf").next_record()), data)

    def test_empty_sequence_and_quality_physical_lines(self):
        reader = framer(b"@id\n\n+\n\n", chunk=1)
        self.assertEqual(canonical(reader.next_record()), b"@id\n\n+\n\n")
        self.assertIsNone(reader.next_record())
        for invalid in (b"@id\n\n+\n", b"@id\n\n+", b"@id\n", b"@id"):
            with self.subTest(invalid=invalid), self.assertRaises(InputError):
                framer(invalid).next_record()

    def test_nonempty_quality_at_eof_and_canonical_requires_lf(self):
        data = b"@id\nAC\n+\n!!"
        self.assertEqual(canonical(framer(data).next_record()), data + b"\n")
        with self.assertRaises(InputError):
            framer(data, mode="canonical_lf").next_record()

    def test_invalid_physical_record_grammar(self):
        for data in (b"id\nAC\n+\n!!\n", b"@id\nAC\n-\n!!\n",
                     b"@id\nAC\n+\n!\n", b"@id\nAC\nGT\n+\n!!!!\n",
                     b"\nAC\n+\n!!\n"):
            with self.subTest(data=data), self.assertRaises(InputError):
                framer(data).next_record()

    def test_normalized_exact_limit_with_raw_crlf_and_overflow_probe(self):
        expected = b"@id\nAC\n+long meta\n!!\n"
        for raw in (expected, expected.replace(b"\n", b"\r\n"), expected[:-1]):
            for chunk in (1, 3, 65536):
                with self.subTest(raw=raw, chunk=chunk):
                    reader = framer(raw, limit=len(expected), chunk=chunk)
                    self.assertEqual(canonical(reader.next_record()), expected)
                    self.assertIsNone(reader.next_record())
                    with self.assertRaises(ResourceError):
                        framer(raw, limit=len(expected) - 1, chunk=chunk).next_record()
        # A final CR at EOF contributes to the payload, unlike a CRLF delimiter.
        raw = b"@id\nAC\n+\n!\r"
        expected = raw + b"\n"
        self.assertEqual(canonical(framer(raw, limit=len(expected)).next_record()), expected)
        with self.assertRaises(ResourceError):
            framer(raw, limit=len(expected) - 1).next_record()

    def test_long_missing_newline_fails_before_large_allocation(self):
        raw = ForwardOnly(b"@" + b"x" * 1_000_000, chunk=65536)
        reader = RecordFramer(raw, line_mode="ingress", max_record_bytes=64)
        with self.assertRaises(ResourceError):
            reader.next_record()
        self.assertLessEqual(raw.tell(), 69)
        self.assertEqual(len(reader._buffer), 69)

    def test_backing_allocation_reused_and_views_expire(self):
        expected = b"@id\nAC\n+\n!!\n"
        reader = framer(expected * 100, limit=len(expected), chunk=3)
        backing = id(reader._buffer)
        previous = None
        for _ in range(100):
            view = reader.next_record()
            self.assertEqual(canonical(view), expected)
            self.assertEqual(id(reader._buffer), backing)
            self.assertTrue(view.header.readonly)
            if previous is not None:
                with self.assertRaises(RuntimeError):
                    _ = previous.header
            previous = view
        self.assertIsNone(reader.next_record())
        with self.assertRaises(RuntimeError):
            _ = previous.normalized_bytes

    def test_independent_opaque_corpus_across_buffer_boundaries(self):
        rng = random.Random(230923)
        alphabet = bytes(value for value in range(256) if value != 10)
        originals = []
        expected = []
        for index in range(120):
            size = rng.randrange(80)
            header = b"@id-" + str(index).encode() + b" " + bytes(rng.choices(alphabet, k=25))
            sequence = bytes(rng.choices(alphabet, k=size))
            plus = b"+" + bytes(rng.choices(alphabet, k=31))
            quality = bytes(rng.choices(alphabet, k=size))
            lines = (header, sequence, plus, quality)
            # CRLF framing independently gives every generated payload, even a
            # payload whose final byte is itself CR, an unambiguous delimiter.
            originals.append(b"".join(line + b"\r\n" for line in lines))
            expected.append(b"".join(line + b"\n" for line in lines))
        limit = max(map(len, expected))
        for chunk in (1, 2, 7, 64, 65536):
            reader = framer(b"".join(originals), limit=limit, chunk=chunk)
            for record in expected:
                self.assertEqual(canonical(reader.next_record()), record)
            self.assertIsNone(reader.next_record())
            self.assertEqual(reader.sha256, hashlib.sha256(b"".join(expected)).hexdigest())

    def test_line_mode_is_explicit_and_frame_limit_checked(self):
        with self.assertRaises(TypeError):
            RecordFramer(io.BytesIO())
        for value in (None, "auto", "crlf"):
            with self.assertRaises(ConfigurationError):
                framer(b"", mode=value)
        for limit in (0, -1, True, 2**31, 1.5):
            with self.assertRaises(ConfigurationError):
                framer(b"", limit=limit)

    def test_empty_input_and_digest(self):
        for limit in (1, 5, 1024 * 1024):
            reader = framer(b"", limit=limit)
            self.assertIsNone(reader.next_record())
            self.assertEqual(reader.sha256, hashlib.sha256(b"").hexdigest())

    def test_cancellation_during_one_long_record(self):
        reader = framer(b"@" + b"x" * 100_000 + b"\nA\n+\n!\n", chunk=32)
        polls = 0

        def cancel():
            nonlocal polls
            polls += 1
            return polls > 20

        with self.assertRaises(CancelledError):
            reader.next_record(cancel)
        self.assertLess(reader.decoded_bytes, 100_000)
        self.assertEqual(reader.record_count, 0)

    def test_counter_overflow_rejects_record(self):
        reader = framer(b"@id\nA\n+\n!\n")
        reader.record_count = (1 << 64) - 1
        with self.assertRaises(ResourceError):
            reader.next_record()


class GzipTests(unittest.TestCase):
    @staticmethod
    def decode(raw, *, advertised=False, source_chunk=7, output_chunk=17):
        source = ForwardOnly(raw, source_chunk)
        with open_decoded_stream(source, advertised_gzip=advertised) as decoded:
            output = bytearray()
            while piece := decoded.read(output_chunk):
                output.extend(piece)
            physical, uncompressed, codec = decoded.physical_bytes, decoded.decoded_bytes, decoded.codec
        assert not source.closed
        return bytes(output), physical, uncompressed, codec

    def test_magic_selected_and_mislabelled_gzip_rejected(self):
        payload = b"@id\nAC\n+\n!!\n"
        for raw, advertised, codec in ((payload, False, "plain"),
                                       (gzip.compress(payload), False, "gzip"),
                                       (gzip.compress(payload), True, "gzip"),
                                       (b"", False, "plain")):
            decoded, physical, uncompressed, detected = self.decode(raw, advertised=advertised)
            self.assertEqual((decoded, physical, uncompressed, detected),
                             (payload if raw else b"", len(raw), len(payload) if raw else 0, codec))
        for raw in (b"", b"@id\nAC\n+\n!!\n", b"\x1f"):
            with self.assertRaises(InputError):
                self.decode(raw, advertised=True)

    def test_concatenated_members_and_empty_members(self):
        payload = b"@id\nAC\n+\n!!\n" * 2
        raw = b"".join(gzip.compress(piece, mtime=0) for piece in
                       (b"", payload[:3], b"", payload[3:-1], payload[-1:], b""))
        for source_chunk in (1, 2, 7, 65536):
            for output_chunk in (1, 2, 65536):
                with self.subTest(source_chunk=source_chunk, output_chunk=output_chunk):
                    decoded, physical, uncompressed, _ = self.decode(
                        raw, source_chunk=source_chunk, output_chunk=output_chunk)
                    self.assertEqual((decoded, physical, uncompressed), (payload, len(raw), len(payload)))

    def test_gzip_crc_size_truncation_and_all_trailing_junk_fail(self):
        valid = gzip.compress(b"@id\nAC\n+\n!!\n", mtime=0)
        corrupt_crc, corrupt_size = bytearray(valid), bytearray(valid)
        corrupt_crc[-8] ^= 1
        corrupt_size[-4] ^= 1
        malformed = [bytes(corrupt_crc), bytes(corrupt_size)]
        malformed += [valid[:cut] for cut in range(2, len(valid))]
        malformed += [valid + junk for junk in (b"\x00", b"\x00" * 20, b"junk", b"\x1f", b"\x1f\x8b")]
        for raw in malformed:
            with self.subTest(raw=raw), self.assertRaises(InputError):
                self.decode(raw, source_chunk=3)

    def test_random_and_high_expansion_members_stay_bounded(self):
        random_bytes = random.Random(12345).randbytes(100_003)
        payload = b"A" * 200_003 + random_bytes
        raw = gzip.compress(payload, mtime=0)
        result = self.decode(raw, source_chunk=137, output_chunk=65536)
        self.assertEqual(result[:3], (payload, len(raw), len(payload)))

    def test_cancellation_during_empty_gzip_member_run(self):
        raw = ForwardOnly(gzip.compress(b"") * 1000, chunk=5)
        decoded = open_decoded_stream(raw)
        polls = 0

        def cancel():
            nonlocal polls
            polls += 1
            return polls > 30

        with self.assertRaises(CancelledError):
            decoded.readinto(bytearray(2), cancel)
        self.assertLess(decoded.physical_bytes, 1000)


class PairTests(unittest.TestCase):
    @staticmethod
    def make_pair(left, right, index=0):
        def record(header):
            return header + b"\nAC\n+meta\n!!\n"
        return PairedReader(framer(record(left)), framer(record(right)), sample_index=index)

    def test_role_grammar_acceptance(self):
        for left, right in (
            (b"@id", b"@id"), (b"@id/1", b"@id/2"),
            (b"@id 1:N:0:AT", b"@id 2:Y:0:AT"),
            (b"@id meta/1", b"@id meta/2"),
            (b"@id metadata 1", b"@id metadata 2"),
            (b"@id/1 1:N:0:AT tail/1", b"@id/2 2:Y:0:AT 2"),
            (b"@id/1", b"@id"), (b"@\xff\t\v\fmetadata/1", b"@\xff metadata/2"),
            (b"@id unrecognized 2:N:not-first end", b"@id first 1:not-first end"),
        ):
            with self.subTest(left=left, right=right):
                reader = self.make_pair(left, right, 42)
                pair = reader.next_pair()
                self.assertEqual((pair.sample_index, pair.original_pair_index), (42, 0))
                self.assertEqual(pair.frame_bytes, 40 + pair.normalized_bytes)
                self.assertIsNone(reader.next_pair())
                self.assertEqual(reader.report().pair_count, 1)
                self.assertTrue(reader.report().complete)

    def test_conflicts_wrong_roles_and_bases_fail_privately(self):
        for left, right in (
            (b"@private-a/1", b"@private-b/2"),
            (b"@private/2", b"@private/1"),
            (b"@private/1 2:N:0", b"@private/2"),
            (b"@private 1:N:0 tail/2", b"@private/2"),
            (b"@private/1", b"@private/2 1"),
            (b"@/1", b"@/2"), (b"@", b"@"),
        ):
            with self.subTest(left=left, right=right):
                reader = self.make_pair(left, right)
                with self.assertRaises(InputError) as caught:
                    reader.next_pair()
                self.assertNotIn("private", str(caught.exception))
                self.assertEqual(reader.pair_count, 0)

    def test_empty_and_one_sided_eof(self):
        reader = PairedReader(framer(b""), framer(b""), sample_index=0)
        self.assertIsNone(reader.next_pair())
        self.assertEqual(reader.report().pair_count, 0)
        self.assertTrue(reader.report().complete)
        record = b"@id\nA\n+\n!\n"
        for left, right in ((b"", record), (record, b"")):
            with self.assertRaises(InputError):
                PairedReader(framer(left), framer(right), sample_index=0).next_pair()

    def test_duplicate_header_text_has_distinct_ordinals_and_exact_digests(self):
        left = b"@id/1\nAC\n+full\n!!\n"
        right = b"@id/2\r\nAC\r\n+full\r\n!!\r\n"
        r1_raw, r2_raw = ForwardOnly(left * 3, 3), ForwardOnly(gzip.compress(right * 3), 5)
        r1_stream, r2_stream = open_decoded_stream(r1_raw), open_decoded_stream(r2_raw)
        reader = PairedReader(RecordFramer(r1_stream, line_mode="ingress"),
                              RecordFramer(r2_stream, line_mode="ingress"), sample_index=3)
        for expected in range(3):
            pair = reader.next_pair()
            self.assertEqual(pair.original_pair_index, expected)
        self.assertIsNone(reader.next_pair())
        report = reader.report()
        self.assertEqual((report.r1_records, report.r2_records, report.pair_count), (3, 3, 3))
        self.assertEqual(report.r1_sha256, hashlib.sha256(left * 3).hexdigest())
        self.assertEqual(report.r2_sha256, hashlib.sha256(right.replace(b"\r\n", b"\n") * 3).hexdigest())
        self.assertEqual(report.r1_normalized_bytes, len(left) * 3)
        self.assertEqual(report.r2_normalized_bytes, len(left) * 3)
        self.assertEqual(report.r2_decoded_bytes, len(right) * 3)
        self.assertEqual(report.r1_physical_bytes, len(left) * 3)
        self.assertEqual(report.r2_physical_bytes, len(gzip.compress(right * 3)))

    def test_gzip_trailer_must_validate_before_pair_reader_is_complete(self):
        left, right = b"@id/1\nA\n+\n!\n", b"@id/2\nA\n+\n!\n"
        bad = gzip.compress(left) + b"\x00"
        reader = PairedReader(RecordFramer(open_decoded_stream(io.BytesIO(bad)), line_mode="ingress"),
                              framer(right), sample_index=0)
        with self.assertRaises(InputError):
            while reader.next_pair() is not None:
                pass
        self.assertFalse(reader.complete)

    def test_failure_has_numeric_context_without_record_bytes(self):
        reader = PairedReader(framer(b"private-record\nA\n+\n!\n"), framer(b""), sample_index=19)
        with self.assertRaises(InputError) as caught:
            reader.next_pair()
        self.assertIn("sample 19, mate 1, pair 0", str(caught.exception))
        self.assertNotIn("private-record", str(caught.exception))
        self.assertIsInstance(caught.exception.__cause__, InputError)

    def test_sample_identity_and_ordinal_bounds(self):
        for index in (-1, 2**32, True, 0.1):
            with self.assertRaises(ConfigurationError):
                self.make_pair(b"@id/1", b"@id/2", index)
        reader = self.make_pair(b"@id/1", b"@id/2", 2**32 - 1)
        reader.pair_count = 2**64 - 1
        with self.assertRaises(ResourceError):
            reader.next_pair()


if __name__ == "__main__":
    unittest.main()
