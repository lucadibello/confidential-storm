package ch.usi.inf.confidentialstorm.common.api.tunnel.model;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Request to decrypt and unpack a received batch inside the enclave.
 *
 * @param encryptedBatch the encrypted batch blob received from the tunnel sender
 */
public record TunnelReceiveRequest(EncryptedValue encryptedBatch) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public TunnelReceiveRequest {
        Objects.requireNonNull(encryptedBatch, "encryptedBatch cannot be null");
    }
}
