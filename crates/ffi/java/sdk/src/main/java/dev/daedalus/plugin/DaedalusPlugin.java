package dev.daedalus.plugin;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface DaedalusPlugin {
  String id();
  BoundaryContract[] boundaryContracts() default {};
  String[] artifacts() default {};
}
