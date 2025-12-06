package daedalus.annotations;

import daedalus.manifest.BackpressureStrategy;
import daedalus.manifest.SyncPolicy;

public @interface SyncGroup {
  String name() default "";

  String[] ports();

  SyncPolicy policy() default SyncPolicy.AllReady;

  BackpressureStrategy backpressure() default BackpressureStrategy.None;

  int capacity() default -1;
}

