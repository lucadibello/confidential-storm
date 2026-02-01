package ch.usi.inf.confidentialstorm.common.api.model;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Request for checking and clamping user contribution.
 * Tuple format: (word, count, userId)
 *
 * @param word   Encrypted word/key
 * @param count  Encrypted count value
 * @param userId Encrypted user ID
 */
public record UserContributionBoundingRequest(EncryptedValue word, EncryptedValue count, EncryptedValue userId) implements Serializable {
    @Serial
    private static final long serialVersionUID = 2L;

    public UserContributionBoundingRequest {
        Objects.requireNonNull(word, "Word cannot be null");
        Objects.requireNonNull(count, "Count cannot be null");
        Objects.requireNonNull(userId, "UserId cannot be null");
    }
}
