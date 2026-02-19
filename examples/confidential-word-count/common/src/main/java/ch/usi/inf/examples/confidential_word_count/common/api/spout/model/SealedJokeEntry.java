package ch.usi.inf.examples.confidential_word_count.common.api.spout.model;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Represents a sealed (encrypted) joke entry with separately encrypted userId and payload.
 * <p>
 * Tuple format: (payload, userId)
 * <p>
 * This design allows services to decrypt only the userId when needed for DP operations,
 * without having to decrypt the full payload.
 *
 * @param payload Encrypted payload (decrypted content: {"body": ..., "category": ..., "id": ..., "rating": ...})
 * @param userId  Encrypted user ID (decrypted content: {"user_id": <id>})
 */
public record SealedJokeEntry(EncryptedValue payload, EncryptedValue userId) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public SealedJokeEntry {
        Objects.requireNonNull(payload, "Encrypted payload cannot be null");
        Objects.requireNonNull(userId, "Encrypted userId cannot be null");
    }
}
