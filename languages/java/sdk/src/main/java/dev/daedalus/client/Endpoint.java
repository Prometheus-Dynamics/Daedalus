package dev.daedalus.client;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;

/**
 * Where the daemon listens: a unix socket, or TCP on the loopback interface. The service does not
 * listen on the network, so a TCP address that is not loopback is refused with
 * {@link IllegalArgumentException} when the endpoint is made.
 */
public sealed interface Endpoint permits Endpoint.Unix, Endpoint.Tcp {
    /** A unix socket at {@code path}. */
    record Unix(Path path) implements Endpoint {
        public Unix {
            if (path == null) throw new IllegalArgumentException("a socket path is needed");
        }

        @Override public String toString() { return "unix:" + path; }
    }

    /** TCP on a loopback address. */
    record Tcp(InetSocketAddress address) implements Endpoint {
        public Tcp {
            if (address == null || address.getAddress() == null || !address.getAddress().isLoopbackAddress()) {
                throw new IllegalArgumentException(address + " is not a loopback address");
            }
        }

        @Override public String toString() { return "tcp:" + address.getAddress().getHostAddress() + ":" + address.getPort(); }
    }

    /**
     * Reads the endpoint's text form: {@code unix:PATH} or {@code tcp:ADDRESS:PORT} with a literal
     * address (for example {@code tcp:127.0.0.1:4100} or {@code tcp:[::1]:4100}).
     */
    static Endpoint parse(String text) {
        if (text.startsWith("unix:")) return new Unix(Path.of(text.substring(5)));
        if (text.startsWith("tcp:")) {
            String rest = text.substring(4);
            int colon = rest.lastIndexOf(':');
            if (colon < 0) throw new IllegalArgumentException("a tcp endpoint is tcp:ADDRESS:PORT: " + text);
            String host = rest.substring(0, colon);
            if (host.startsWith("[") && host.endsWith("]")) host = host.substring(1, host.length() - 1);
            int port;
            try {
                port = Integer.parseInt(rest.substring(colon + 1));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("no port in " + text, e);
            }
            try {
                return new Tcp(new InetSocketAddress(InetAddress.getByName(host), port));
            } catch (java.io.IOException e) {
                throw new IllegalArgumentException("no address in " + text, e);
            }
        }
        throw new IllegalArgumentException("`" + text + "` is no endpoint: unix:PATH or tcp:ADDRESS:PORT");
    }
}
