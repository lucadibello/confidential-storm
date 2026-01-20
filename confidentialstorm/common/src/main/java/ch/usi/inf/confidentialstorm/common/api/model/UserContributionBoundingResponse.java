package ch.usi.inf.confidentialstorm.common.api.model;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

import javax.annotation.Nullable;
import java.io.Serial;
import java.io.Serializable;

public class UserContributionBoundingResponse implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * The actual contribution count associated with the word, clamped to the allowed limit.
     */
    private final EncryptedValue clampedCount;

    /**
     * Private constructor to initialize the UserContributionBoundingResponse.
     *
     * @param clampedCount The encrypted clamped count, or null if the contribution was dropped.
     */
    private UserContributionBoundingResponse(@Nullable EncryptedValue clampedCount) {
        this.clampedCount = clampedCount;
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
     * Factory method to create a dropped user contribution response.
     * @return A UserContributionBoundingResponse indicating a dropped contribution.
     */
    public static UserContributionBoundingResponse dropped() {
        return new UserContributionBoundingResponse(null);
    }

    /**
     * Factory method to create an authorized user contribution response.
     * @param count The encrypted count associated with the word.
     * @return A UserContributionBoundingResponse indicating an authorized contribution.
     */
    public static UserContributionBoundingResponse authorized(EncryptedValue count) {
        return new UserContributionBoundingResponse(count);
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
