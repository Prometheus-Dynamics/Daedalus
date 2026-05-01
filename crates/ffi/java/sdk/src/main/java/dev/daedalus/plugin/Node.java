package dev.daedalus.plugin;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface Node {
  String id();
  String[] inputs() default {};
  String[] outputs() default {};
  String capability() default "";
  Class<?> state() default Void.class;
  String access() default "read";
  String residency() default "";
  String layout() default "";
}
