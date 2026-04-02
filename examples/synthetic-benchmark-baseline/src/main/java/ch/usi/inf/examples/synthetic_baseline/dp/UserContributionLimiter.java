package ch.usi.inf.examples.synthetic_baseline.dp;

import java.util.HashMap;
import java.util.Map;

/**
 * Tracks per-user contribution counts and enforces a hard maximum.
 * (copied from confidentialstorm/enclave)
 */
public final class UserContributionLimiter {
    private final Map<Object, Long> counts = new HashMap<>();

    /**
     * Per-user contribution limiter enforcing C-bounded contributions.
     *
     * @param userId           identifier of the user
     * @param maxContributions maximum contributions per user (C from Section 3.2)
     * @return true if the contribution is accepted, false if it exceeds the bounds
     */
    public boolean allow(Object userId, long maxContributions) {
        if (userId == null) {
            return true;
        }
        long currentCount = counts.merge(userId, 1L, Long::sum);
        return currentCount <= maxContributions;
    }
}
