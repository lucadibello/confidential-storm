package ch.usi.inf.confidentialstorm.enclave.dp;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

/**
 * Tracks per-user contribution counts and enforces a hard maximum.
 * This encapsulates the bounding logic so applications can reuse it across services.
 */
public final class UserContributionLimiter {
    private final Map<Object, Long> counts = new HashMap<>();

    /**
     * Per-user contribution limiter enforcing C-bounded contributions.
     * Critical for maintaining L1 = C × Lm sensitivity assumption.
     *
     * @param userId           identifier of the user. If null, the contribution is always allowed (event-level privacy rather than user-level privacy)
     * @param maxContributions maximum contributions per user (C from Section 3.2).
     * @return true if the contribution is accepted, false if it exceeds the bounds
     */
    public boolean allow(@Nullable Object userId, long maxContributions) {
        if (userId == null) {
            return true;
        }

        long currentCount = counts.merge(userId, 1L, Long::sum);
        return currentCount <= maxContributions;
    }

    /**
     * Per-user contribution limiter enforcing C-bounded contributions.
     * Critical for maintaining L1 = C × Lm sensitivity assumption.
     *
     * @param userId           identifier of the user. If null, the contribution is always allowed (event-level privacy rather than user-level privacy)
     * @param contributions    number of contributions to add for the user
     * @param maxContributions maximum contributions per user (C from Section 3.2).
     * @return the number of accepted contributions (could be less than requested if it exceeds the bounds)
     */
    public long allow(@Nullable Object userId, long contributions, long maxContributions) {
        if (userId == null) {
            return 0;
        }

        long currentCount = counts.getOrDefault(userId, 0L);
        if (currentCount + contributions <= maxContributions) {
            counts.put(userId, currentCount + contributions);
            return contributions;
        } else {
            return maxContributions - currentCount;
        }
    }

    public long getUserCount(@Nullable Object userId) {
        if (userId == null) {
            return 0;
        }
        return counts.getOrDefault(userId, 0L);
    }
}
