package ch.usi.inf.examples.simple_topology.common.api.model;

import java.io.Serial;
import java.io.Serializable;

public record SimpleEnclaveRequest(String message) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public SimpleEnclaveRequest {
        if (message == null || message.isEmpty()) {
            throw new IllegalArgumentException(
                "Message cannot be null or empty"
            );
        }
    }
}
