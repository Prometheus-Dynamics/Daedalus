package dev.daedalus.client;

import dev.daedalus.service.Protocol.ErrorCode;
import dev.daedalus.service.Protocol.ServiceError;

/**
 * Why a client call failed. The policy: every call that can fail says so with this checked exception,
 * and nothing else is thrown for a failure of the connection or of the host's answer. A host's refusal
 * is {@link Kind#SERVICE}, and it carries the service code and the message.
 */
public class ClientException extends Exception {
    private static final long serialVersionUID = 1L;

    /** What went wrong. */
    public enum Kind {
        /** The host refused the request. {@link #code()} and {@link #error()} say why. */
        SERVICE,
        /** There is no connection. Nothing was sent, or the connection dropped with the request safe to resend. */
        DISCONNECTED,
        /** The connection dropped after the request was written: the host may or may not have acted on it. */
        UNKNOWN_DELIVERY,
        /** The host speaks another protocol version or layout, or needs something this client lacks. */
        REFUSED,
        /** A message could not be read or written. */
        WIRE,
        /** A request larger than the host reads. */
        TOO_LARGE,
        /** The socket failed. */
        IO,
        /** A session name is empty or longer than the bound. */
        BAD_SESSION,
        /** The host answered with a reply that does not belong to the op. */
        UNEXPECTED,
        /** The calling thread was interrupted while it waited. */
        INTERRUPTED
    }

    private final Kind kind;
    private final transient ServiceError error;

    ClientException(Kind kind, String message) {
        this(kind, null, message, null);
    }

    ClientException(Kind kind, String message, Throwable cause) {
        this(kind, null, message, cause);
    }

    private ClientException(Kind kind, ServiceError error, String message, Throwable cause) {
        super(message, cause);
        this.kind = kind;
        this.error = error;
    }

    /** A host's refusal, with its code and message. */
    static ClientException service(ServiceError error) {
        return new ClientException(Kind.SERVICE, error, error.code().label() + ": " + error.message(), null);
    }

    public Kind kind() {
        return kind;
    }

    /** The service code, for a {@link Kind#SERVICE} failure; null otherwise. */
    public ErrorCode code() {
        return error == null ? null : error.code();
    }

    /** The host's whole refusal, for a {@link Kind#SERVICE} failure; null otherwise. */
    public ServiceError error() {
        return error;
    }
}
