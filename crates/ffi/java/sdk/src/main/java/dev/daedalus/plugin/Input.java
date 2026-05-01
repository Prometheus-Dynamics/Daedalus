package dev.daedalus.plugin;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface Input {
  long defaultLong() default 0;
  long min() default Long.MIN_VALUE;
  long max() default Long.MAX_VALUE;
  String policy() default "";
}
