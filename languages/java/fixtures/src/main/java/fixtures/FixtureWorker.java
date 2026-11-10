package fixtures;

import dev.daedalus.worker.Worker;

/** The Java fixture worker: serves the canonical fixture nodes on stdin and stdout. */
public final class FixtureWorker {
    private FixtureWorker() {}

    public static void main(String[] args) throws Exception {
        Worker.serve(Nodes.class);
    }
}
