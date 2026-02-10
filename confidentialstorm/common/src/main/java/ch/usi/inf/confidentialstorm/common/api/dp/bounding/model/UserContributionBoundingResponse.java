package ch.usi.inf.confidentialstorm.common.api.dp.bounding.model;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

import javax.annotation.Nullable;
import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Response from the contribution bounding service.
 * Tuple format when authorized: (word, clampedCount, userId)
 */
public class UserContributionBoundingResponse implements Serializable {
    @Serial
    private static final long serialVersionUID = 2L;

    /**
     * The encrypted word (re-encrypted with new AAD).
     */
    private final EncryptedValue word;

    /**
     * The actual contribution count associated with the word, clamped to the allowed limit.
     */
    private final EncryptedValue clampedCount;

    /**
     * The encrypted user ID (re-encrypted with new AAD).
     */
    private final EncryptedValue userId;

    /**
     * Key used for routing the contribution to the correct partition in the downstream processing.
     */
    private final byte[] routingKey;

    /**
     * Private constructor to initialize the UserContributionBoundingResponse.
     *
     * @param word         The encrypted word, or null if dropped.
     * @param clampedCount The encrypted clamped count, or null if the contribution was dropped.
     * @param userId       The encrypted user ID, or null if dropped.
     */
    private UserContributionBoundingResponse(@Nullable EncryptedValue word,
                                             @Nullable EncryptedValue clampedCount,
                                             @Nullable EncryptedValue userId,
                                             @Nullable byte[] routingKey
    ) {
        this.word = word;
        this.clampedCount = clampedCount;
        this.userId = userId;
        this.routingKey = routingKey;
    }

    /**
     * Gets the encrypted word.
     *
     * @return The encrypted word, or null if the contribution was dropped.
     */
    @Nullable
    public EncryptedValue word() {
        return word;
    }

    /**
     * Gets the clamped count of the user contribution.
     *
     * @return The encrypted clamped count, or null if the contribution was dropped.
     */
    @Nullable
    public EncryptedValue clampedCount() {
        return clampedCount;
    }

    /**
     * Gets the encrypted user ID.
     *
     * @return The encrypted user ID, or null if the contribution was dropped.
     */
    @Nullable
    public EncryptedValue userId() {
        return userId;
    }

    /**
    * Gets the routing key for the contribution.
    *
    * @return The routing key, or null if the contribution was dropped.
    */
    @Nullable
    public byte[] routingKey() {
        return routingKey;
    }

    /**
     * Factory method to create a dropped user contribution response.
     *
     * @return A UserContributionBoundingResponse indicating a dropped contribution.
     */
    public static UserContributionBoundingResponse dropped() {
        return new UserContributionBoundingResponse(null, null, null, null);
    }

    /**
     * Factory method to create an authorized user contribution response.
     *
     * @param word   The encrypted word (re-encrypted with new AAD).
     * @param count  The encrypted clamped count associated with the word.
     * @param userId The encrypted user ID (re-encrypted with new AAD).
     * @return A UserContributionBoundingResponse indicating an authorized contribution.
     */
    public static UserContributionBoundingResponse authorized(EncryptedValue word, EncryptedValue count,
                                                              EncryptedValue userId, byte[] routingKey) {
        Objects.requireNonNull(word, "Word cannot be null for authorized response");
        Objects.requireNonNull(count, "Count cannot be null for authorized response");
        Objects.requireNonNull(userId, "UserId cannot be null for authorized response");
        return new UserContributionBoundingResponse(word, count, userId, routingKey);
    }

    /**
     * Checks if the user contribution was dropped.
     *
     * @return True if the contribution was dropped, false otherwise.
     */
    public boolean isDropped() {
        return this.clampedCount == null;
    }
}
