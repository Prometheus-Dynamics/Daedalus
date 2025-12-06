package daedalus.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface In {
  String name();

  ScalarType scalar() default ScalarType.Unset;

  // Optional "ClassName#staticMethod" returning a TypeExpr map (see daedalus.manifest.Types helpers).
  String tyRef() default "";

  String source() default "";

  DefaultKind defaultKind() default DefaultKind.None;

  long defaultInt() default 0;

  double defaultFloat() default 0.0;

  boolean defaultBool() default false;

  String defaultString() default "";
}

