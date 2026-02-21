package ch.usi.inf.confidentialstorm.common.api.tunnel.model;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Response containing the real tuples extracted from a decrypted batch.
 * Each element is a list of encrypted fields representing one tuple.
 *
 * @param realTuples list of tuple field lists (only real tuples, dummies discarded)
 */
public record TunnelReceiveResponse(List<List<EncryptedValue>> realTuples) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public TunnelReceiveResponse {
        Objects.requireNonNull(realTuples, "realTuples cannot be null");
    }
}
