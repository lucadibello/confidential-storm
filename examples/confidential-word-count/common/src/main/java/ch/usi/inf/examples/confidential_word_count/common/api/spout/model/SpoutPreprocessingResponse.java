package ch.usi.inf.examples.confidential_word_count.common.api.spout.model;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Response from the SpoutPreprocessingService after setting up routing.
 * Tuple format: (payload, userId)
 *
 * @param payload Encrypted payload (body, category, id, rating) with updated AAD
 * @param userId  Encrypted user ID with updated AAD
 */
public record SpoutPreprocessingResponse(EncryptedValue payload, EncryptedValue userId) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public SpoutPreprocessingResponse {
        Objects.requireNonNull(payload, "Payload cannot be null");
        Objects.requireNonNull(userId, "UserId cannot be null");
    }
}
