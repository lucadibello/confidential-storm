package ch.usi.inf.examples.confidential_word_count.common.api.count.model;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Response containing a word count for a specific user.
 * Tuple format: (word, count, userId, routingKey)
 *
 * @param word       Encrypted word
 * @param count      Encrypted count
 * @param userId     Encrypted user ID
 * @param routingKey Routing key for fieldsGrouping (hash of user:userId|word:word)
 */
public record WordCountResponse(EncryptedValue word, EncryptedValue count, EncryptedValue userId, byte[] routingKey)
        implements WordCountBaseRequest {
    @Serial
    private static final long serialVersionUID = 2L;

    public WordCountResponse {
        Objects.requireNonNull(word, "Encrypted word cannot be null");
        Objects.requireNonNull(count, "Encrypted count cannot be null");
        Objects.requireNonNull(userId, "Encrypted userId cannot be null");
        Objects.requireNonNull(routingKey, "Routing key cannot be null");
    }
}
