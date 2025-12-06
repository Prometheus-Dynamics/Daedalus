package daedalus.annotations;

import daedalus.manifest.ManifestBuilders;
import daedalus.manifest.NodeDef;
import daedalus.manifest.Plugin;
import daedalus.manifest.Types;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class AnnotationEmitter {
  private AnnotationEmitter() {}

  public static void registerAnnotated(Plugin plugin, String classpath, Class<?>... classes)
      throws Exception {
    for (Class<?> cls : classes) {
      for (Method m : cls.getDeclaredMethods()) {
        Node nodeAnn = m.getAnnotation(Node.class);
        if (nodeAnn == null) continue;
        if (!Modifier.isStatic(m.getModifiers()) || !Modifier.isPublic(m.getModifiers())) {
          throw new IllegalArgumentException(
              "@Node method must be public static: " + cls.getName() + "#" + m.getName());
        }

        NodeDef def =
            new NodeDef(nodeAnn.id()).javaEntrypoint(classpath, cls.getName(), m.getName());
        if (!nodeAnn.label().isEmpty()) def.label = nodeAnn.label();
        if (nodeAnn.rawIo()) def.raw_io = true;
        if (nodeAnn.stateful()) def.stateful = true;
        if (!nodeAnn.capability().isEmpty()) def.capability = nodeAnn.capability();
        if (!nodeAnn.defaultCompute().isEmpty()) def.default_compute = nodeAnn.defaultCompute();
        if (nodeAnn.featureFlags().length > 0)
          def.feature_flags.addAll(Arrays.asList(nodeAnn.featureFlags()));

        for (Meta meta : nodeAnn.metadata()) {
          def.metadata.put(meta.key(), meta.value());
        }

        for (SyncGroup g : nodeAnn.syncGroups()) {
          List<String> ports = Arrays.asList(g.ports());
          boolean isShorthand =
              g.name().isEmpty()
                  && g.policy() == daedalus.manifest.SyncPolicy.AllReady
                  && g.backpressure() == daedalus.manifest.BackpressureStrategy.None
                  && g.capacity() < 0;
          if (isShorthand) {
            def.sync_groups.add(new ArrayList<>(ports));
          } else {
            String name = g.name().isEmpty() ? null : g.name();
            Integer cap = g.capacity() >= 0 ? g.capacity() : null;
            daedalus.manifest.BackpressureStrategy bp =
                g.backpressure() == daedalus.manifest.BackpressureStrategy.None
                    ? null
                    : g.backpressure();
            def.sync_groups.add(ManifestBuilders.syncGroup(name, ports, g.policy(), bp, cap));
          }
        }

        State st = m.getAnnotation(State.class);
        if (st != null) {
          def.stateful = true;
          Map<String, Object> spec = new LinkedHashMap<>();
          spec.put("ty", resolveTyRef(st.tyRef()));
          def.state = spec;
        }

        // Inputs come from parameter order.
        for (Parameter p : m.getParameters()) {
          In in = p.getAnnotation(In.class);
          if (in == null) continue;
          Map<String, Object> ty = resolveTy(in.scalar(), in.tyRef());
          Object constValue = defaultValue(in);
          String source = in.source().isEmpty() ? null : in.source();
          def.input(ManifestBuilders.port(in.name(), ty, constValue, source));
        }
        if (def.inputs.isEmpty()) {
          Inputs inputsAnn = m.getAnnotation(Inputs.class);
          if (inputsAnn != null) {
            for (InputPort in : inputsAnn.value()) {
              Map<String, Object> ty = resolveTy(in.scalar(), in.tyRef());
              Object constValue = defaultValue(in);
              String source = in.source().isEmpty() ? null : in.source();
              def.input(ManifestBuilders.port(in.name(), ty, constValue, source));
            }
          }
        }

        // Outputs are explicit and ordered by index.
        Out[] outs = m.getAnnotationsByType(Out.class);
        Arrays.sort(outs, Comparator.comparingInt(Out::index));
        for (Out out : outs) {
          Map<String, Object> ty = resolveTy(out.scalar(), out.tyRef());
          def.output(ManifestBuilders.port(out.name(), ty));
        }

        plugin.register(def);
      }
    }
  }

  private static Object defaultValue(In in) {
    return defaultValue(in.defaultKind(), in.defaultInt(), in.defaultFloat(), in.defaultBool(), in.defaultString());
  }

  private static Object defaultValue(InputPort in) {
    return defaultValue(in.defaultKind(), in.defaultInt(), in.defaultFloat(), in.defaultBool(), in.defaultString());
  }

  private static Object defaultValue(
      DefaultKind kind, long defaultInt, double defaultFloat, boolean defaultBool, String defaultString) {
    switch (kind) {
      case None:
        return null;
      case Int:
        return defaultInt;
      case Float:
        return defaultFloat;
      case Bool:
        return defaultBool;
      case String:
        return defaultString;
      default:
        return null;
    }
  }

  private static Map<String, Object> resolveTy(ScalarType scalar, String tyRef) throws Exception {
    if (tyRef != null && !tyRef.isEmpty()) {
      return resolveTyRef(tyRef);
    }
    switch (scalar) {
      case Unit:
        return Types.unitTy();
      case Bool:
        return Types.boolTy();
      case Int:
        return Types.intTy();
      case Float:
        return Types.floatTy();
      case String:
        return Types.stringTy();
      case Bytes:
        return Types.bytesTy();
      default:
        throw new IllegalArgumentException("missing type (scalar or tyRef)");
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> resolveTyRef(String ref) throws Exception {
    int idx = ref.indexOf('#');
    if (idx <= 0 || idx >= ref.length() - 1) {
      throw new IllegalArgumentException("tyRef must be 'ClassName#method': " + ref);
    }
    String clsName = ref.substring(0, idx);
    String method = ref.substring(idx + 1);
    Class<?> cls = Class.forName(clsName);
    Method m = cls.getDeclaredMethod(method);
    if (!Modifier.isStatic(m.getModifiers())) {
      throw new IllegalArgumentException("tyRef must reference a static method: " + ref);
    }
    Object out = m.invoke(null);
    if (!(out instanceof Map)) {
      throw new IllegalArgumentException("tyRef method must return Map: " + ref);
    }
    return (Map<String, Object>) out;
  }
}
