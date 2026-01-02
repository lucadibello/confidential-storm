package ch.usi.inf.examples.simple_topology.common.api.model;

import java.io.Serial;
import java.io.Serializable;

public record SimpleEnclaveResponse(String response) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public SimpleEnclaveResponse {
        if (response == null || response.isBlank()) {
            throw new IllegalArgumentException(
                "Response cannot be null or blank"
            );
        }
    }
}
