package dev.daedalus.service;

import dev.daedalus.wire.Types.Ty;

/**
 * A value with its type. The service has no port declarations to type a value by, so each value
 * carries its type tree, then the value as that type. {@code value} is in the worker codec's form
 * (see {@code dev.daedalus.wire.Values}).
 */
public record TypedValue(Ty ty, Object value) {}
