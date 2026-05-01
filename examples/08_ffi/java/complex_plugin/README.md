# Java FFI Complex Plugin

`src/main/java/ffi/showcase/ShowcasePlugin.java` mirrors the Rust baseline through annotations,
records, explicit state classes, package metadata, and typed runtime errors.

Package build:

```bash
javac -d build/classes/java/main \
  $(find ../../../../crates/ffi/java/sdk/src/main/java src/main/java -name '*.java') \
  build-package.java
java -cp build/classes/java/main:. BuildPackage
```

How close to Rust: Java needs annotations for port names, state classes, package artifacts, and
native library paths. Records make struct/config declarations close to Rust data types. The
annotations and package builder now come from the reusable SDK source under `crates/ffi/java/sdk`.

Additional requested node shapes are included in `ShowcasePlugin.java`: `arrayDynamicSum`,
`nodeIoComplex`, `gpuTint`, `internalAdapterConsume`, `externalAdapterConsume`, `zeroCopyLen`,
`sharedRefLen`, `cowAppendMarker`, `mutableBrighten`, and `ownedBytesLen`.
