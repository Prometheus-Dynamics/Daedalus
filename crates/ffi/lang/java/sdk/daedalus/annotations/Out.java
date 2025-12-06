package daedalus.annotations;

import java.lang.annotation.Repeatable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Repeatable(Outs.class)
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Out {
  int index();

  String name();

  ScalarType scalar() default ScalarType.Unset;

  String tyRef() default "";
}

