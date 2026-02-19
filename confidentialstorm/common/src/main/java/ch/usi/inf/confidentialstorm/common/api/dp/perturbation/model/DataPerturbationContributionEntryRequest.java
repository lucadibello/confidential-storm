package ch.usi.inf.confidentialstorm.common.api.dp.perturbation.model;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

import java.io.Serial;
import java.util.Objects;

/**
 * Request to add a user contribution to the data perturbation mechanism.
 * This request contains the encrypted user ID, the encrypted word/key, and the encrypted clamped count.
 */
public final class DataPerturbationContributionEntryRequest extends DataPerturbationRequest {

    @Serial
    private static final long serialVersionUID = 1L;

    private final EncryptedValue userId;
    private final EncryptedValue word;
    private final EncryptedValue clampedCount;

    /**
     * Constructs a new DataPerturbationContributionEntryRequest.
     *
     * @param userId       the encrypted user ID
     * @param word         the encrypted word/key
     * @param clampedCount the encrypted clamped contribution count
     * @throws IllegalArgumentException if any parameter is null
     */
    public DataPerturbationContributionEntryRequest(EncryptedValue userId, EncryptedValue word, EncryptedValue clampedCount) {
        if (userId == null || word == null || clampedCount == null) {
            throw new IllegalArgumentException("All fields must be non-null");
        }
        this.userId = userId;
        this.word = word;
        this.clampedCount = clampedCount;
    }

    /**
     * Gets the encrypted user ID.
     *
     * @return the encrypted user ID
     */
    public EncryptedValue userId() {
        return userId;
    }

    /**
     * Gets the encrypted word/key.
     *
     * @return the encrypted word
     */
    public EncryptedValue word() {
        return word;
    }

    /**
     * Gets the encrypted clamped contribution count.
     *
     * @return the encrypted clamped count
     */
    public EncryptedValue clampedCount() {
        return clampedCount;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (DataPerturbationContributionEntryRequest) obj;
        return Objects.equals(this.userId, that.userId) &&
                Objects.equals(this.word, that.word) &&
                Objects.equals(this.clampedCount, that.clampedCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, word, clampedCount);
    }

    @Override
    public String toString() {
        return "DataPerturbationContributionEntryRequest[" +
                "userId=" + userId + ", " +
                "word=" + word + ", " +
                "clampedCount=" + clampedCount + ']';
    }

}
