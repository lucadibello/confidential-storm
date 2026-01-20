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
     * Records a contribution for user and returns whether it is within the bound or not.
     *
     * @param userId           identifier of the user. If null, the contribution is always allowed (event-level privacy rather than user-level privacy)
     * @param maxContributions maximum allowed contributions per user
     * @return true if the contribution is accepted, false if it exceeds the bounds
     */
    public boolean allow(@Nullable Object userId, long maxContributions) {
        if (userId == null) {
            return true;
        }

        final long[] newCount = new long[1];
        counts.compute(userId, (k, v) -> {
            long updated = v == null ? 1L : v + 1L;
            newCount[0] = updated;
            return updated;
        });
        return newCount[0] <= maxContributions;
    }
}
