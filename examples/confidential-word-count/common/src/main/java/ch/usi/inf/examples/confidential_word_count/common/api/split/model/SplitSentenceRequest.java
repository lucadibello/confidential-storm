package ch.usi.inf.examples.confidential_word_count.common.api.split.model;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Request for the SplitSentenceService to split a joke entry into words.
 * Tuple format: (payload, userId)
 *
 * @param payload Encrypted payload containing the joke body (body, category, id, rating)
 * @param userId  Encrypted user ID
 */
public record SplitSentenceRequest(EncryptedValue payload, EncryptedValue userId) implements Serializable {
    @Serial
    private static final long serialVersionUID = 2L;

    public SplitSentenceRequest {
        Objects.requireNonNull(payload, "Encrypted payload cannot be null");
        Objects.requireNonNull(userId, "Encrypted userId cannot be null");
    }
}
