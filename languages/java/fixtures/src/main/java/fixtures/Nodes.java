package fixtures;

import dev.daedalus.worker.Context;
import dev.daedalus.worker.Node;
import dev.daedalus.worker.NodeException;
import dev.daedalus.worker.Wire;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** The thirteen canonical fixture nodes. Their ports and types match worker-conformance exactly. */
public final class Nodes {
    private Nodes() {}

    @Node(id = "fixture.scalar_add", inputs = {"a:i64", "b:i64"}, outputs = {"out:i64"})
    public static Object scalarAdd(Map<String, Object> in, Context cx) {
        return (Long) in.get("a") + (Long) in.get("b");
    }

    @Node(id = "fixture.bytes_echo", inputs = {"in:bytes"}, outputs = {"out:bytes"})
    public static Object bytesEcho(Map<String, Object> in, Context cx) {
        return in.get("in");
    }

    @Node(id = "fixture.struct_record", inputs = {"in:struct{name:string,count:i64}"}, outputs = {"out:struct{name:string,count:i64}"})
    public static Object structRecord(Map<String, Object> in, Context cx) {
        return in.get("in");
    }

    @Node(id = "fixture.enum_choice", inputs = {"in:enum{idle,busy:u32}"}, outputs = {"out:enum{idle,busy:u32}"})
    public static Object enumChoice(Map<String, Object> in, Context cx) {
        return in.get("in");
    }

    @Node(id = "fixture.optional_value", inputs = {"in:optional<i64>"}, outputs = {"out:optional<i64>"})
    public static Object optionalValue(Map<String, Object> in, Context cx) {
        return in.get("in");
    }

    @Node(id = "fixture.list_reverse", inputs = {"in:list<u32>"}, outputs = {"out:list<u32>"})
    public static Object listReverse(Map<String, Object> in, Context cx) {
        List<Object> items = new ArrayList<>((List<?>) in.get("in"));
        Collections.reverse(items);
        return items;
    }

    @Node(id = "fixture.tuple_pair", inputs = {"in:tuple<string,bool>"}, outputs = {"out:tuple<string,bool>"})
    public static Object tuplePair(Map<String, Object> in, Context cx) {
        return in.get("in");
    }

    @Node(id = "fixture.map_pairs", inputs = {"in:map<string,i64>"}, outputs = {"out:map<string,i64>"})
    public static Object mapPairs(Map<String, Object> in, Context cx) {
        return in.get("in");
    }

    @Node(id = "fixture.multi_output", inputs = {"x:u32"}, outputs = {"a:u32", "b:u32"})
    public static Object multiOutput(Map<String, Object> in, Context cx) {
        long x = (Long) in.get("x");
        return new Object[] {x, (x * 2) & 0xFFFFFFFFL};
    }

    @Node(id = "fixture.raw_io", inputs = {"payload:bytes"}, outputs = {"out:bytes"})
    public static Object rawIo(Map<String, Object> in, Context cx) {
        byte[] data = (byte[]) in.get("payload");
        byte[] out = new byte[data.length];
        for (int i = 0; i < data.length; i++) out[i] = data[data.length - 1 - i];
        return out;
    }

    @Node(id = "fixture.stateful_counter", inputs = {"delta:i64"}, outputs = {"total:i64"}, state = "i64")
    public static Object statefulCounter(Map<String, Object> in, Context cx) {
        long current = cx.state() == null ? 0L : (Long) cx.state();
        long total = current + (Long) in.get("delta");
        cx.setState(total);
        return total;
    }

    @Node(id = "fixture.custom_type_key", inputs = {"in:opaque(fixture.custom)"}, outputs = {"out:opaque(fixture.custom)"})
    public static Object customTypeKey(Map<String, Object> in, Context cx) {
        return in.get("in");
    }

    @Node(id = "fixture.failure_diagnostic", inputs = {"value:i64"}, outputs = {"out:i64"})
    public static Object failureDiagnostic(Map<String, Object> in, Context cx) {
        cx.detail("diagnostic detail: the value is rejected, ");
        cx.detail("and this is the rest of it");
        cx.faultCode(77);
        throw new NodeException("node_error", "diagnostic: the value is rejected");
    }
}
