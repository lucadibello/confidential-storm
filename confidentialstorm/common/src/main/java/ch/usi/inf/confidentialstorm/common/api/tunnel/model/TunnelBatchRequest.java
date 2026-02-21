package ch.usi.inf.confidentialstorm.common.api.tunnel.model;

import java.io.Serial;
import java.io.Serializable;

/**
 * Request to retrieve the next transmit batch from the tunnel's outbound buffer.
 *
 * @param epochNumber the epoch sequence number for AAD binding
 */
public record TunnelBatchRequest(long epochNumber) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
}
