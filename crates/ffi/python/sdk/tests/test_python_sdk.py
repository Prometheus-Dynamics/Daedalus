from __future__ import annotations

import sys
import unittest
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import json

from daedalus_ffi import (
    Config,
    State,
    adapter,
    bytes_payload,
    from_wire,
    node,
    plugin,
    to_wire,
    type_key,
    u64,
    validate_descriptor,
)

U64_MAX = (1 << 64) - 1
# What the Rust host serializes for `WireValue::UInt(u64::MAX)`.
UINT_MAX_JSON = '{"kind":"uint","value":18446744073709551615}'


@dataclass
class ScaleConfig(Config):
    factor: int = Config.port(default=2)


class AccumState(State):
    total: int = 0


@type_key("test.Point")
@dataclass
class Point:
    x: float
    y: float


class Mode(Enum):
    FAST = "fast"


@type_key("test.External")
@dataclass
class External:
    raw: str


class PythonSdkTests(unittest.TestCase):
    def test_infers_schema_from_type_hints_dataclasses_and_markers(self) -> None:
        @node(id="shape", inputs=["point", "mode", "items", "maybe", ScaleConfig], outputs=["summary"])
        def shape(point: Point, mode: Mode, items: list[int], maybe: int | None, config: ScaleConfig) -> str:
            return "ok"

        @node(id="accum", inputs=["value"], outputs=["sum"], state=AccumState)
        def accum(value: int, state: AccumState) -> int:
            return value

        descriptor = plugin("test_plugin", [shape, accum]).type_contract(
            "test.Point", ["host_read", "worker_write"]
        ).artifact("_bundle/src/plugin.py").descriptor()

        validate_descriptor(descriptor)
        nodes = {node["id"]: node for node in descriptor["schema"]["nodes"]}
        self.assertEqual(nodes["shape"]["inputs"][0]["type_key"], "test.Point")
        self.assertEqual(nodes["shape"]["inputs"][2]["ty"], {"List": {"Scalar": "Int"}})
        self.assertEqual(nodes["shape"]["inputs"][3]["ty"], {"Optional": {"Scalar": "Int"}})
        self.assertTrue(nodes["accum"]["stateful"])
        self.assertEqual(descriptor["metadata"]["package_builder"], "daedalus_ffi.python")

    def test_payload_helpers_and_adapters_are_recorded(self) -> None:
        convert = adapter("test.external_to_int", External, int)(lambda value: int(value.raw))

        @node(id="payload_len", inputs=["payload"], outputs=["len"], access="view", transport="memoryview")
        def payload_len(payload: memoryview) -> int:
            return len(payload)

        @node(id="cow", inputs=["payload"], outputs=["payload"], access="modify")
        def cow(payload: bytes_payload.CowView) -> bytes_payload.CowView:
            return payload

        descriptor = plugin("payload_plugin", [payload_len, cow]).adapter(convert).transport(
            memoryview=True, mmap=True
        ).descriptor()

        validate_descriptor(descriptor)
        nodes = {node["id"]: node for node in descriptor["schema"]["nodes"]}
        self.assertEqual(nodes["payload_len"]["inputs"][0]["access"], "view")
        self.assertEqual(nodes["payload_len"]["metadata"]["transport"], "memoryview")
        self.assertEqual(nodes["cow"]["inputs"][0]["ty"], {"Scalar": "Bytes"})
        self.assertEqual(descriptor["metadata"]["adapters"], ["test.external_to_int"])

    def test_descriptor_validation_rejects_backend_shape_errors(self) -> None:
        @node(id="add", inputs=["a", "b"], outputs=["out"])
        def add(a: int, b: int) -> int:
            return a + b

        with self.assertRaisesRegex(ValueError, "duplicate"):
            plugin("bad_plugin", [add, add]).descriptor()

        descriptor = plugin("bad_plugin", [add]).descriptor()
        descriptor["backends"].pop("add")
        with self.assertRaisesRegex(ValueError, "missing backend"):
            validate_descriptor(descriptor)

        descriptor = plugin("bad_plugin", [add]).descriptor()
        descriptor["backends"]["extra"] = descriptor["backends"]["add"]
        with self.assertRaisesRegex(ValueError, "unexpected backend"):
            validate_descriptor(descriptor)

    def test_rejects_bad_access_residency_layout_and_type_contracts(self) -> None:
        @node(id="bad_access", inputs=["value"], outputs=["out"], access="project")
        def bad_access(value: int) -> int:
            return value

        with self.assertRaisesRegex(ValueError, "unsupported access"):
            plugin("bad_plugin", [bad_access]).descriptor()

        @node(id="bad_residency", inputs=["value"], outputs=["out"], residency="disk")
        def bad_residency(value: int) -> int:
            return value

        with self.assertRaisesRegex(ValueError, "unsupported residency"):
            plugin("bad_plugin", [bad_residency]).descriptor()

        @node(id="bad_layout", inputs=["value"], outputs=["out"], layout="rgba8-hwc")
        def bad_layout(value: int) -> int:
            return value

        with self.assertRaisesRegex(ValueError, "layout requires residency"):
            plugin("bad_plugin", [bad_layout]).descriptor()

        with self.assertRaisesRegex(ValueError, "type_key"):
            plugin("bad_plugin", []).type_contract("", ["host_read"])

        with self.assertRaisesRegex(ValueError, "unsupported boundary"):
            plugin("bad_plugin", []).type_contract("test.Bad", ["teleport"])

    def test_unsupported_annotations_fail_before_descriptor_write(self) -> None:
        @node(id="bad", inputs=["value"], outputs=["out"])
        def bad(value, /):  # type: ignore[no-untyped-def]
            return value

        with self.assertRaisesRegex(TypeError, "input `value` is not a function parameter|missing"):
            plugin("bad_plugin", [bad]).descriptor()


    def test_unsigned_integers_round_trip_as_uint(self) -> None:
        self.assertEqual(json.dumps(to_wire(U64_MAX), separators=(",", ":")), UINT_MAX_JSON)
        self.assertEqual(from_wire(json.loads(UINT_MAX_JSON)), U64_MAX)
        self.assertEqual(to_wire(5, u64), {"kind": "uint", "value": 5})
        self.assertEqual(to_wire(5), {"kind": "int", "value": 5})
        self.assertEqual(to_wire(1 << 63), {"kind": "uint", "value": 1 << 63})
        self.assertEqual(
            to_wire([1, 2], list[u64]),
            {"kind": "list", "value": [{"kind": "uint", "value": 1}, {"kind": "uint", "value": 2}]},
        )
        nested = {"big": [U64_MAX, -1], "raw": b"\x01", "none": None, "ok": True, "f": 0.5}
        self.assertEqual(from_wire(json.loads(json.dumps(to_wire(nested)))), {**nested, "big": [U64_MAX, -1]})
        for value, ty in [(-1, u64), (U64_MAX + 1, None), (-(1 << 63) - 1, None)]:
            with self.assertRaisesRegex(ValueError, "out of range"):
                to_wire(value, ty)

        @node(id="count", inputs=["value"], outputs=["out"])
        def count(value: u64) -> u64:
            return value

        port = plugin("u64_plugin", [count]).descriptor()["schema"]["nodes"][0]["inputs"][0]
        self.assertEqual(port["ty"], {"Scalar": "U64"})


if __name__ == "__main__":
    unittest.main()
