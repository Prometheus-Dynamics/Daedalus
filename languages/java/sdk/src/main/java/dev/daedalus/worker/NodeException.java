package dev.daedalus.worker;

/** Raise from a node body to fault the run with this code and message. */
public final class NodeException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final String code;

    public NodeException(String code, String message) {
        super(message);
        if (!Wire.FAULT_CODES.containsKey(code)) {
            throw new IllegalArgumentException("unknown fault code " + code);
        }
        this.code = code;
    }

    public String code() {
        return code;
    }
}
