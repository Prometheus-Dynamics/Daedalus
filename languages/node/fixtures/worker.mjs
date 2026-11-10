// The Node fixture worker: serves the canonical fixture nodes on stdin and stdout.
import { NODES, serve } from "../sdk/src/index.js";
import "./nodes.mjs";

serve(NODES.filter((declared) => declared.id.startsWith("fixture.")));
