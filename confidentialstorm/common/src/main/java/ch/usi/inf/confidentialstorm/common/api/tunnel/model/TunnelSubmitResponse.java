package ch.usi.inf.confidentialstorm.common.api.tunnel.model;

import java.io.Serial;
import java.io.Serializable;

/**
 * Response indicating whether the tuple was accepted into the tunnel's outbound buffer.
 *
 * @param accepted true if the tuple was buffered for the next batch
 */
public record TunnelSubmitResponse(boolean accepted) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
}
