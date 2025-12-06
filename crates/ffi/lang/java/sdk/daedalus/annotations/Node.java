package daedalus.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Node {
  String id();

  String label() default "";

  boolean rawIo() default false;

  boolean stateful() default false;

  String capability() default "";

  String defaultCompute() default "";

  String[] featureFlags() default {};

  Meta[] metadata() default {};

  SyncGroup[] syncGroups() default {};
}

