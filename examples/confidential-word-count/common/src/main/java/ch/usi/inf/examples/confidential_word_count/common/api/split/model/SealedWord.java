package ch.usi.inf.examples.confidential_word_count.common.api.split.model;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Represents a sealed (encrypted) word with its associated encrypted userId.
 * Tuple format: (word, count, userId, routingKey)
 *
 * @param word       Encrypted word
 * @param count      Encrypted count
 * @param userId     Encrypted user ID
 * @param routingKey Routing key for fieldsGrouping (hash of user:userId|word:word)
 */
public record SealedWord(EncryptedValue word, EncryptedValue count, EncryptedValue userId, byte[] routingKey) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public SealedWord {
        Objects.requireNonNull(word, "Encrypted word cannot be null");
        Objects.requireNonNull(count, "Encrypted count cannot be null");
        Objects.requireNonNull(userId, "Encrypted userId cannot be null");
        Objects.requireNonNull(routingKey, "Routing key cannot be null");
    }
}
