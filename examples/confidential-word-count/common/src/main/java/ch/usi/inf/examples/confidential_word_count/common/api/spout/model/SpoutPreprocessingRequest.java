package ch.usi.inf.examples.confidential_word_count.common.api.spout.model;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Request for the SpoutPreprocessingService to set up routing for a joke entry.
 * Tuple format: (payload, userId)
 *
 * @param payload Encrypted payload (body, category, id, rating)
 * @param userId  Encrypted user ID
 */
public record SpoutPreprocessingRequest(EncryptedValue payload, EncryptedValue userId) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public SpoutPreprocessingRequest {
        Objects.requireNonNull(payload, "Payload cannot be null");
        Objects.requireNonNull(userId, "UserId cannot be null");
    }
}
