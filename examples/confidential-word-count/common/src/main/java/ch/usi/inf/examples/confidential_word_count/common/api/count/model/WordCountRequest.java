package ch.usi.inf.examples.confidential_word_count.common.api.count.model;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Request to count a word for a specific user.
 * Tuple format: (word, userId)
 *
 * @param word   Encrypted word
 * @param userId Encrypted user ID
 */
public record WordCountRequest(EncryptedValue word, EncryptedValue userId) implements Serializable {
    @Serial
    private static final long serialVersionUID = 2L;

    public WordCountRequest {
        Objects.requireNonNull(word, "Encrypted word cannot be null");
        Objects.requireNonNull(userId, "Encrypted userId cannot be null");
    }
}
