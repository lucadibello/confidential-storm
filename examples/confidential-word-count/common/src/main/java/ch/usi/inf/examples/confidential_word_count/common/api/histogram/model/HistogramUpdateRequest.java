package ch.usi.inf.examples.confidential_word_count.common.api.histogram.model;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Request to update the histogram with a word count contribution.
 * Tuple format: (word, count, userId)
 *
 * @param word   Encrypted word
 * @param count  Encrypted count (clamped)
 * @param userId Encrypted user ID
 */
public record HistogramUpdateRequest(EncryptedValue word, EncryptedValue count, EncryptedValue userId) implements Serializable {
    @Serial
    private static final long serialVersionUID = 2L;

    public HistogramUpdateRequest {
        Objects.requireNonNull(word, "Encrypted word cannot be null");
        Objects.requireNonNull(count, "Encrypted count cannot be null");
        Objects.requireNonNull(userId, "Encrypted userId cannot be null");
    }
}
