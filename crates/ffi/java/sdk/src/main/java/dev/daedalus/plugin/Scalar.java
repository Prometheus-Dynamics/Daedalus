package dev.daedalus.plugin;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares the exact Daedalus scalar width of a numeric port when the Java carrier type cannot
 * express it, e.g. {@code @Scalar("u32") long count}. On a node method it types the single output.
 * Values are Rust scalar names: i8, i16, i32, i64, isize, u8, u16, u32, u64, usize, f32, f64.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.PARAMETER, ElementType.METHOD})
public @interface Scalar {
  String value();
}
