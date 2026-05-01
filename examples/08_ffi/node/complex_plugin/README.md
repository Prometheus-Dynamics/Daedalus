# Node FFI Complex Plugin

`src/plugin.ts` mirrors the Rust baseline through the TypeScript builder API. The example keeps
schemas explicit so SDK output is deterministic and easy to snapshot.

Package build:

```bash
npm run build:plugin
```

How close to Rust: TypeScript needs explicit builder schemas for most ports. Function bodies stay
close to Rust once the node metadata is declared.

Additional requested node shapes are included in `src/plugin.ts`: `arrayDynamicSum`,
`nodeIoComplex`, `gpuTint`, `internalAdapterConsume`, `externalAdapterConsume`, `zeroCopyLen`,
`sharedRefLen`, `cowAppendMarker`, `mutableBrighten`, and `ownedBytesLen`.
