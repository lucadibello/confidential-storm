package ch.usi.inf.confidentialstorm.common.api.tunnel.model;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Response containing the encrypted batch blob ready for transmission.
 *
 * @param encryptedBatch the entire batch encrypted as a single EncryptedValue
 */
public record TunnelBatchResponse(EncryptedValue encryptedBatch) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public TunnelBatchResponse {
        Objects.requireNonNull(encryptedBatch, "encryptedBatch cannot be null");
    }
}
