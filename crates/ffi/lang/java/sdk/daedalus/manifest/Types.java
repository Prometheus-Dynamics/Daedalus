package daedalus.manifest;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class Types {
  private Types() {}

  public static Map<String, Object> intTy() {
    return scalar("Int");
  }

  public static Map<String, Object> floatTy() {
    return scalar("Float");
  }

  public static Map<String, Object> boolTy() {
    return scalar("Bool");
  }

  public static Map<String, Object> stringTy() {
    return scalar("String");
  }

  public static Map<String, Object> bytesTy() {
    return scalar("Bytes");
  }

  public static Map<String, Object> unitTy() {
    return scalar("Unit");
  }

  public static Map<String, Object> scalar(String name) {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("Scalar", name);
    return out;
  }

  public static Map<String, Object> optional(Map<String, Object> inner) {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("Optional", inner);
    return out;
  }

  public static Map<String, Object> list(Map<String, Object> inner) {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("List", inner);
    return out;
  }

  public static Map<String, Object> tuple(List<Map<String, Object>> items) {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("Tuple", items);
    return out;
  }

  public static Map<String, Object> map(Map<String, Object> k, Map<String, Object> v) {
    Map<String, Object> out = new LinkedHashMap<>();
    List<Object> items = new ArrayList<>(2);
    items.add(k);
    items.add(v);
    out.put("Map", items);
    return out;
  }

  public static Map<String, Object> structTy(List<Field> fields) {
    Map<String, Object> out = new LinkedHashMap<>();
    List<Object> fs = new ArrayList<>(fields.size());
    for (Field f : fields) {
      Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("name", f.name);
      entry.put("ty", f.ty);
      fs.add(entry);
    }
    out.put("Struct", fs);
    return out;
  }

  public static Map<String, Object> enumTy(List<Variant> variants) {
    Map<String, Object> out = new LinkedHashMap<>();
    List<Object> vs = new ArrayList<>(variants.size());
    for (Variant v : variants) {
      Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("name", v.name);
      entry.put("ty", v.ty);
      vs.add(entry);
    }
    out.put("Enum", vs);
    return out;
  }

  public static final class Field {
    public final String name;
    public final Map<String, Object> ty;

    public Field(String name, Map<String, Object> ty) {
      this.name = name;
      this.ty = ty;
    }
  }

  public static final class Variant {
    public final String name;
    public final Object ty;

    public Variant(String name, Object ty) {
      this.name = name;
      this.ty = ty;
    }
  }
}

