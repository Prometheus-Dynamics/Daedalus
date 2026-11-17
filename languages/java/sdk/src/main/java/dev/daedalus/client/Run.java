package dev.daedalus.client;

import dev.daedalus.service.Model.CarryReport;
import dev.daedalus.service.Model.Inspection;
import dev.daedalus.service.Model.StepSummary;
import dev.daedalus.service.Protocol.StopMode;
import dev.daedalus.service.TypedValue;
import java.util.Optional;

/**
 * A run the host started, and the calls that act on it. Each call is the same one {@link Client} has
 * with the run's number. Thread-safety is the client's: see {@link Client}.
 */
public final class Run {
    private final Client client;
    private final long id;

    Run(Client client, long id) {
        this.client = client;
        this.id = id;
    }

    /** The host's number for the run. */
    public long id() {
        return id;
    }

    /** Hands a value to an input port. */
    public Client.Fed feed(String port, TypedValue value) throws ClientException {
        return client.feed(id, port, value);
    }

    /** Runs {@code count} ticks. */
    public StepSummary step(long count) throws ClientException {
        return client.step(id, count);
    }

    /** Takes the oldest value out of an output port, or empty when the port has none. */
    public Optional<TypedValue> take(String port) throws ClientException {
        return client.take(id, port);
    }

    /** Asks the run to stop. Its watches are told why. */
    public void cancel() throws ClientException {
        client.cancel(id);
    }

    /** Stops the run now or after what is waiting. */
    public Client.Stopped stop(StopMode mode) throws ClientException {
        return client.stop(id, mode);
    }

    /** Replaces the run's graph with a stored one, and says what happened to each node's state. */
    public CarryReport reload(String graph) throws ClientException {
        return client.reload(id, graph);
    }

    /** What the host knows of the run. */
    public Inspection.Run inspect() throws ClientException {
        return client.inspectRun(id);
    }

    /** The run's telemetry. */
    public Inspection.Telemetry inspectTelemetry() throws ClientException {
        return client.inspectTelemetry(id);
    }
}
