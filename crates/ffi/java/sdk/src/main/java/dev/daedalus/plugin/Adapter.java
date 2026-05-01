package dev.daedalus.plugin;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface Adapter {
  String id();
  Class<?> source();
  Class<?> target();
}
