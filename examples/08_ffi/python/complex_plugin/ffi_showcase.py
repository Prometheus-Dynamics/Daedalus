from dataclasses import dataclass
from enum import Enum

from daedalus_ffi import Config, State, adapter, bytes_payload, event, gpu, image, node, plugin, type_key


@dataclass
class ScaleConfig(Config):
    factor: int = Config.port(default=2, min=1, max=16, policy="clamp")


@dataclass
class AccumState(State):
    sum: int = 0


@type_key("ffi.showcase.Point")
@dataclass
class Point:
    x: float
    y: float


class Mode(Enum):
    FAST = "fast"
    PRECISE = "precise"


@type_key("ffi.showcase.InternalCount")
@dataclass
class InternalCount:
    value: int


@type_key("ffi.external.LegacyCount")
@dataclass
class ExternalLegacyCount:
    raw: str


@adapter(id="ffi.showcase.internal_count_to_i64", source=InternalCount, target=int)
def internal_count_to_i64(value: InternalCount) -> int:
    return value.value


@adapter(id="ffi.showcase.external_legacy_count_to_i64", source=ExternalLegacyCount, target=int)
def external_legacy_count_to_i64(value: ExternalLegacyCount) -> int:
    return int(value.raw)


@node(id="scalar_add", inputs=["a", "b"], outputs=["out"])
def scalar_add(a: int, b: int) -> int:
    return a + b


@node(id="split_sign", inputs=["value"], outputs=["positive", "negative"])
def split_sign(value: int) -> tuple[int, int]:
    return value, -value


@node(id="scale", inputs=["value", ScaleConfig], outputs=["out"])
def scale(value: int, config: ScaleConfig) -> int:
    return value * config.factor


@node(id="accumulate", inputs=["value"], outputs=["sum"], state=AccumState)
def accumulate(value: int, state: AccumState) -> int:
    state.sum += value
    return state.sum


@node(id="bytes_len", inputs=["payload"], outputs=["len"])
def bytes_len(payload: bytes_payload.View) -> int:
    return len(payload)


@node(id="image_boost", inputs=["rgba8"], outputs=["rgba8"])
def image_boost(rgba8: image.Rgba8) -> image.Rgba8:
    return rgba8.map_pixels(lambda r, g, b, a: (min(r + 8, 255), g, b, a))


@node(id="shape_summary", inputs=["point", "mode", "maybe", "items", "labels", "pair", "unit"], outputs=["summary"])
def shape_summary(
    point: Point,
    mode: Mode,
    maybe: int | None,
    items: list[int],
    labels: dict[str, int],
    pair: tuple[int, bool],
    unit: None,
) -> str:
    value = 0 if maybe is None else maybe
    return f"{mode.value}:{point.x:.1f},{point.y:.1f}:{value}:{len(items)}:{len(labels)}:{pair[0]}"


@node(id="emit_event", inputs=["message"], outputs=["ok"])
def emit_event(message: str, ctx: event.Context) -> bool:
    ctx.info("showcase.event", message)
    return True


@node(id="capability_add", capability="Add", inputs=["a", "b"], outputs=["out"])
def capability_add(a: int, b: int) -> int:
    return a + b


@node(id="checked_divide", inputs=["a", "b"], outputs=["out"])
def checked_divide(a: int, b: int) -> int:
    if b == 0:
        raise event.TypedError("division_by_zero", "b must not be zero")
    return a // b


@node(id="array_dynamic_sum", inputs=["values"], outputs=["sum"])
def array_dynamic_sum(values: list[int]) -> int:
    return sum(values)


@node(id="node_io_complex", inputs=["point", "weights", "metadata"], outputs=["score", "label", "point"])
def node_io_complex(point: Point, weights: list[float], metadata: dict[str, str]) -> tuple[float, str, Point]:
    return point.x + point.y + sum(weights), metadata.get("label", "unlabeled"), point


@node(id="gpu_tint", inputs=["rgba8"], outputs=["rgba8"], residency="gpu", layout="rgba8-hwc")
def gpu_tint(rgba8: gpu.ImageRgba8) -> gpu.ImageRgba8:
    return rgba8.dispatch("ffi_showcase_tint")


@node(id="internal_adapter_consume", inputs=["count"], outputs=["out"])
def internal_adapter_consume(count: int) -> int:
    return count + 1


@node(id="external_adapter_consume", inputs=["count"], outputs=["out"])
def external_adapter_consume(count: int) -> int:
    return count * 2


@node(id="zero_copy_len", inputs=["frame"], outputs=["len"], access="view", transport="memoryview")
def zero_copy_len(frame: memoryview) -> int:
    return len(frame)


@node(id="shared_ref_len", inputs=["frame"], outputs=["len"], access="read", transport="memoryview")
def shared_ref_len(frame: bytes_payload.SharedView) -> int:
    return len(frame)


@node(id="cow_append_marker", inputs=["frame"], outputs=["frame"], access="modify", transport="mmap")
def cow_append_marker(frame: bytes_payload.CowView) -> bytes_payload.CowView:
    return frame.with_appended(bytes([255]))


@node(id="mutable_brighten", inputs=["rgba8"], outputs=["rgba8"], access="modify", transport="mmap")
def mutable_brighten(rgba8: image.MutableRgba8) -> image.MutableRgba8:
    rgba8.map_pixels_in_place(lambda r, g, b, a: (min(r + 1, 255), g, b, a))
    return rgba8


@node(id="owned_bytes_len", inputs=["blob"], outputs=["len"], access="move")
def owned_bytes_len(blob: bytes) -> int:
    return len(blob)


showcase_plugin = plugin("ffi_showcase", nodes=[
    scalar_add,
    split_sign,
    scale,
    accumulate,
    bytes_len,
    image_boost,
    shape_summary,
    emit_event,
    capability_add,
    checked_divide,
    array_dynamic_sum,
    node_io_complex,
    gpu_tint,
    internal_adapter_consume,
    external_adapter_consume,
    zero_copy_len,
    shared_ref_len,
    cow_append_marker,
    mutable_brighten,
    owned_bytes_len,
]).type_contract("ffi.showcase.Point", capabilities=["host_read", "worker_write"]).artifact(
    "_bundle/src/ffi_showcase.py"
).adapter(internal_count_to_i64).adapter(external_legacy_count_to_i64).transport(
    memoryview=True,
    mmap=True,
)
