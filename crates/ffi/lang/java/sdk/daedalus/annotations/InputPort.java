package daedalus.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface InputPort {
  String name();

  ScalarType scalar() default ScalarType.Unset;

  String tyRef() default "";

  String source() default "";

  DefaultKind defaultKind() default DefaultKind.None;

  long defaultInt() default 0;

  double defaultFloat() default 0.0;

  boolean defaultBool() default false;

  String defaultString() default "";
}
