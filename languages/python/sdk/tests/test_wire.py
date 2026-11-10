"""The codec: round trips, and the refusals the protocol names."""

import io
import unittest

from daedalus import T, WireError
from daedalus import wire


class _Schema(wire.Schema):
    pass


class WireTests(unittest.TestCase):
    def round_trip(self, ty, value):
        out = wire.Writer()
        wire.encode_value(ty, value, _Schema(codecs=["fixture.custom"]), out)
        inp = wire.Reader(bytes(out.buf))
        back = wire.decode_value(ty, inp, _Schema(codecs=["fixture.custom"]))
        inp.finish()
        return back

    def test_u64_max_round_trips(self):
        self.assertEqual(self.round_trip(T.u64, 2**64 - 1), 2**64 - 1)

    def test_a_negative_u64_is_a_typed_error(self):
        with self.assertRaises(WireError):
            self.round_trip(T.u64, -1)

    def test_out_of_range_u32_is_a_typed_error(self):
        with self.assertRaises(WireError):
            self.round_trip(T.u32, 2**32)

    def test_bytes_round_trip_and_memoryview_encodes(self):
        self.assertEqual(self.round_trip(T.bytes, b"\x00\x01\xff"), b"\x00\x01\xff")
        out = wire.Writer()
        wire.encode_value(T.bytes, memoryview(b"abc"), _Schema(), out)
        self.assertEqual(bytes(out.buf)[4:], b"abc")

    def test_compound_values_round_trip(self):
        ty = T.struct(("name", T.string), ("count", T.i64))
        self.assertEqual(self.round_trip(ty, {"name": "demo", "count": 3}), {"name": "demo", "count": 3})
        enum = T.enum(("idle", None), ("busy", T.u32))
        self.assertEqual(self.round_trip(enum, ("busy", 7)), ("busy", 7))
        self.assertEqual(self.round_trip(T.optional(T.i64), None), None)
        self.assertEqual(self.round_trip(T.list(T.u32), [1, 2, 3]), [1, 2, 3])
        self.assertEqual(self.round_trip(T.tuple(T.string, T.bool), ("x", True)), ("x", True))
        self.assertEqual(self.round_trip(T.map(T.string, T.i64), {"a": 1}), {"a": 1})

    def test_an_opaque_value_needs_a_codec(self):
        out = wire.Writer()
        with self.assertRaises(WireError):
            wire.encode_value(T.opaque("fixture.custom"), b"x", _Schema(), out)

    def test_a_layout_hash_matches_the_rust_constant(self):
        self.assertEqual(wire.LAYOUT_HASH, 16986761858374539365)

    def test_a_type_nested_past_the_limit_is_refused(self):
        ty = T.bool
        for _ in range(40):
            ty = T.optional(ty)
        with self.assertRaises(WireError):
            wire.encode_type(ty, wire.Writer())

    def test_a_packet_round_trips_and_a_clean_end_is_none(self):
        stream = io.BytesIO()
        wire.write_packet(stream, b"one")
        wire.write_packet(stream, b"")
        stream.seek(0)
        self.assertEqual(wire.read_packet(stream), b"one")
        self.assertEqual(wire.read_packet(stream), b"")
        self.assertIsNone(wire.read_packet(stream))

    def test_a_length_over_the_cap_is_refused(self):
        stream = io.BytesIO((17 * 1024 * 1024).to_bytes(4, "little"))
        with self.assertRaises(WireError):
            wire.read_packet(stream, cap=16 * 1024 * 1024)


if __name__ == "__main__":
    unittest.main()
