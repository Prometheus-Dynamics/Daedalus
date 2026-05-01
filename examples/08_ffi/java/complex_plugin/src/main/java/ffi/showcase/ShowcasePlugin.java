package ffi.showcase;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import dev.daedalus.plugin.BoundaryContract;
import dev.daedalus.plugin.Adapter;
import dev.daedalus.plugin.BytesView;
import dev.daedalus.plugin.CowBytes;
import dev.daedalus.plugin.DaedalusPlugin;
import dev.daedalus.plugin.EventContext;
import dev.daedalus.plugin.GpuRgba8Image;
import dev.daedalus.plugin.Input;
import dev.daedalus.plugin.MutableBytes;
import dev.daedalus.plugin.MutableRgba8Image;
import dev.daedalus.plugin.Node;
import dev.daedalus.plugin.Outputs;
import dev.daedalus.plugin.Rgba8Image;
import dev.daedalus.plugin.SharedBytes;
import dev.daedalus.plugin.State;
import dev.daedalus.plugin.Tuple2;
import dev.daedalus.plugin.TypeKey;
import dev.daedalus.plugin.TypedPluginException;

@DaedalusPlugin(
    id = "ffi_showcase",
    boundaryContracts = {@BoundaryContract(typeKey = "ffi.showcase.Point", capabilities = {"host_read", "worker_write"})},
    artifacts = {"_bundle/java/ffi-showcase.jar", "_bundle/native/any/libffi_showcase_jni.so"})
public final class ShowcasePlugin {
  public enum Mode {
    FAST,
    PRECISE
  }

  public static final class AccumState {
    public long sum;
  }

  public record ScaleConfig(@Input(defaultLong = 2, min = 1, max = 16, policy = "clamp") long factor) {}

  @TypeKey("ffi.showcase.Point")
  public record Point(double x, double y) {}

  @TypeKey("ffi.showcase.InternalCount")
  public record InternalCount(long value) {}

  @TypeKey("ffi.external.LegacyCount")
  public record ExternalLegacyCount(String raw) {}

  @Adapter(id = "ffi.showcase.internal_count_to_i64", source = InternalCount.class, target = Long.class)
  public static long internalCountToI64(InternalCount value) {
    return value.value();
  }

  @Adapter(id = "ffi.showcase.external_legacy_count_to_i64", source = ExternalLegacyCount.class, target = Long.class)
  public static long externalLegacyCountToI64(ExternalLegacyCount value) {
    return Long.parseLong(value.raw());
  }

  @Node(id = "scalar_add", inputs = {"a", "b"}, outputs = {"out"})
  public static long scalarAdd(long a, long b) {
    return a + b;
  }

  @Node(id = "split_sign", inputs = {"value"}, outputs = {"positive", "negative"})
  public static Outputs splitSign(long value) {
    return Outputs.of("positive", value, "negative", -value);
  }

  @Node(id = "scale", inputs = {"value", "config"}, outputs = {"out"})
  public static long scale(long value, ScaleConfig config) {
    return value * config.factor();
  }

  @Node(id = "accumulate", inputs = {"value"}, outputs = {"sum"}, state = AccumState.class)
  public static long accumulate(long value, @State AccumState state) {
    state.sum += value;
    return state.sum;
  }

  @Node(id = "bytes_len", inputs = {"payload"}, outputs = {"len"})
  public static long bytesLen(BytesView payload) {
    return payload.length();
  }

  @Node(id = "image_boost", inputs = {"rgba8"}, outputs = {"rgba8"})
  public static Rgba8Image imageBoost(Rgba8Image rgba8) {
    return rgba8.mapPixels((r, g, b, a) -> new int[] {Math.min(r + 8, 255), g, b, a});
  }

  @Node(id = "shape_summary", inputs = {"point", "mode", "maybe", "items", "labels", "pair", "unit"}, outputs = {"summary"})
  public static String shapeSummary(
      Point point,
      Mode mode,
      Optional<Long> maybe,
      List<Long> items,
      Map<String, Long> labels,
      Tuple2<Long, Boolean> pair,
      Void unit) {
    return "%s:%.1f,%.1f:%d:%d:%d:%d"
        .formatted(mode.name().toLowerCase(), point.x(), point.y(), maybe.orElse(0L), items.size(), labels.size(), pair.first());
  }

  @Node(id = "emit_event", inputs = {"message"}, outputs = {"ok"})
  public static boolean emitEvent(String message, EventContext context) {
    context.info("showcase.event", message);
    return true;
  }

  @Node(id = "capability_add", capability = "Add", inputs = {"a", "b"}, outputs = {"out"})
  public static long capabilityAdd(long a, long b) {
    return a + b;
  }

  @Node(id = "checked_divide", inputs = {"a", "b"}, outputs = {"out"})
  public static long checkedDivide(long a, long b) {
    if (b == 0) {
      throw new TypedPluginException("division_by_zero", "b must not be zero");
    }
    return a / b;
  }

  @Node(id = "array_dynamic_sum", inputs = {"values"}, outputs = {"sum"})
  public static long arrayDynamicSum(List<Long> values) {
    return values.stream().mapToLong(Long::longValue).sum();
  }

  @Node(id = "node_io_complex", inputs = {"point", "weights", "metadata"}, outputs = {"score", "label", "point"})
  public static Outputs nodeIoComplex(Point point, List<Double> weights, Map<String, String> metadata) {
    double weightSum = weights.stream().mapToDouble(Double::doubleValue).sum();
    return Outputs.of("score", point.x() + point.y() + weightSum, "label", metadata.getOrDefault("label", "unlabeled"), "point", point);
  }

  @Node(id = "gpu_tint", inputs = {"rgba8"}, outputs = {"rgba8"}, residency = "gpu", layout = "rgba8-hwc")
  public static GpuRgba8Image gpuTint(GpuRgba8Image rgba8) {
    return rgba8.dispatch("ffi_showcase_tint");
  }

  @Node(id = "internal_adapter_consume", inputs = {"count"}, outputs = {"out"})
  public static long internalAdapterConsume(long count) {
    return count + 1;
  }

  @Node(id = "external_adapter_consume", inputs = {"count"}, outputs = {"out"})
  public static long externalAdapterConsume(long count) {
    return count * 2;
  }

  @Node(id = "zero_copy_len", inputs = {"frame"}, outputs = {"len"}, access = "view")
  public static long zeroCopyLen(java.nio.ByteBuffer frame) {
    return frame.remaining();
  }

  @Node(id = "shared_ref_len", inputs = {"frame"}, outputs = {"len"}, access = "read")
  public static long sharedRefLen(SharedBytes frame) {
    return frame.length();
  }

  @Node(id = "cow_append_marker", inputs = {"frame"}, outputs = {"frame"}, access = "modify")
  public static CowBytes cowAppendMarker(CowBytes frame) {
    return frame.withAppended((byte) 255);
  }

  @Node(id = "mutable_brighten", inputs = {"rgba8"}, outputs = {"rgba8"}, access = "modify")
  public static MutableRgba8Image mutableBrighten(MutableRgba8Image rgba8) {
    return rgba8.mapPixelsInPlace((r, g, b, a) -> new int[] {Math.min(r + 1, 255), g, b, a});
  }

  @Node(id = "owned_bytes_len", inputs = {"blob"}, outputs = {"len"}, access = "move")
  public static long ownedBytesLen(MutableBytes blob) {
    return blob.length();
  }
}
