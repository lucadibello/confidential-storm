package ch.usi.inf.confidentialstorm.common.api.tunnel.model;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Request to submit a tuple's encrypted fields for buffered transmission through a cloaked tunnel.
 *
 * @param tupleFields the encrypted fields of the tuple to transmit
 */
public record TunnelSubmitRequest(List<EncryptedValue> tupleFields) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public TunnelSubmitRequest {
        Objects.requireNonNull(tupleFields, "tupleFields cannot be null");
    }
}
