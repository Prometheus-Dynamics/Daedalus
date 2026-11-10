package dev.daedalus.worker;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares a node on a static method of signature {@code Object f(Map<String, Object> inputs,
 * Context cx)}. Ports are {@code "name:type"} strings; the type syntax is in {@link Wire#parseType}.
 * A method returns its one output's value, or an {@code Object[]} with one value per output, in order.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Node {
    String id();

    String[] inputs() default {};

    String[] outputs() default {};

    /** The state's type, or empty when the node keeps no state. */
    String state() default "";
}
