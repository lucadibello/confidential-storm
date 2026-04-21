package ch.usi.inf.confidentialstorm.enclave.util;

import org.apache.commons.math3.util.FastMath;

/**
 * Utility class for Differential Privacy (DP) calculations.
 * Includes methods for converting DP parameters and calculating noise scales.
 */
public class DPUtil {

    /**
     * Number of candidate delta' values explored when deriving per-round key-selection budget.
     */
    private static final int KEY_SELECTION_BUDGET_SEARCH_STEPS = 1024;

    /**
     * Holder for per-round privacy budget obtained from composition.
     */
    public static final class PerRoundBudget {
        private final double epsilon;
        private final double delta;

        public PerRoundBudget(double epsilon, double delta) {
            this.epsilon = epsilon;
            this.delta = delta;
        }

        public double epsilon() {
            return epsilon;
        }

        public double delta() {
            return delta;
        }
    }

    /**
     * Converts an (epsilon, delta)-DP guarantee into an equivalent rho-zCDP guarantee.
     * <p>
     * Uses tight Renyi DP conversion [Bun & Steinke 2016] via binary search.
     * Finds the maximum rho such that rho-zCDP implies (eps, delta)-DP, which
     * minimizes the noise needed while satisfying the privacy constraint.
     *
     * @param eps   target epsilon (privacy loss parameter)
     * @param delta target delta (failure probability in (eps, delta)-DP)
     * @return rho parameter for zCDP satisfying the requested (eps, delta)-DP
     */
    public static double cdpRho(double eps, double delta) {
        if (eps < 0 || delta <= 0) {
            throw new IllegalArgumentException(
                "epsilon must be non-negative and delta must be positive"
            );
        }
        if (delta >= 1) return 0.0;

        double rho_min = 0.0;
        double rho_max = eps + 1;

        // Binary search: find max rho where cdpDelta(rho, eps) <= delta
        // Higher rho means weaker privacy and higher delta, so we want largest valid rho
        for (int i = 0; i < 1000; i++) {
            double rho = (rho_min + rho_max) / 2;
            if (cdpDelta(rho, eps) <= delta) {
                rho_min = rho; // rho satisfies constraint, try larger
            } else {
                rho_max = rho; // rho too weak, try smaller
            }
        }
        return rho_min;
    }

    /**
     * Converts a total key-selection privacy budget into a per-round budget using
     * C-fold advanced composition (Section 4.4 of the DP-SQLP paper).
     * <p>
     * We search over {@code deltaPrime} in:
     * <pre>
     *   delta_total = rounds * delta_round + deltaPrime
     * </pre>
     * and maximize the feasible per-round epsilon from the advanced composition bound:
     * <pre>
     *   epsilon_total >= sqrt(2 * rounds * ln(1 / deltaPrime)) * epsilon_round
     *                   + rounds * epsilon_round * (exp(epsilon_round) - 1)
     * </pre>
     *
     * @param epsilonTotal total epsilon budget allocated to key selection (epsilon_k)
     * @param deltaTotal   total delta budget allocated to key selection (delta_k)
     * @param rounds       maximum number of key-selection rounds per user (C)
     * @return per-round budget for one Algorithm-1 execution
     */
    public static PerRoundBudget keySelectionPerRoundBudget(
        double epsilonTotal,
        double deltaTotal,
        long rounds
    ) {
        if (epsilonTotal < 0 || deltaTotal <= 0) {
            throw new IllegalArgumentException(
                "epsilonTotal must be non-negative and deltaTotal must be positive"
            );
        }
        if (rounds <= 0) {
            throw new IllegalArgumentException("rounds must be positive");
        }
        if (rounds == 1) {
            return new PerRoundBudget(epsilonTotal, deltaTotal);
        }
        if (epsilonTotal == 0.0) {
            return new PerRoundBudget(0.0, deltaTotal / rounds);
        }

        double bestEpsilonRound = 0.0;
        double bestDeltaRound = deltaTotal / rounds;
        double bestScore = -1.0;

        // Empirically tighter than fixing deltaPrime to a constant fraction.
        for (int i = 1; i < KEY_SELECTION_BUDGET_SEARCH_STEPS; i++) {
            double fraction = (double) i / (double) KEY_SELECTION_BUDGET_SEARCH_STEPS; // in (0,1)
            double deltaPrime = deltaTotal * fraction;
            double deltaRound = (deltaTotal - deltaPrime) / rounds;
            if (deltaRound <= 0) {
                continue;
            }

            double epsilonRound = solveRoundEpsilonWithAdvancedComposition(
                epsilonTotal,
                rounds,
                deltaPrime
            );

            // Score candidate with a closed-form rho implied by the standard zCDP->DP
            // conversion epsilon = rho + 2*sqrt(rho*ln(1/delta)).
            // This avoids expensive nested cdpRho() calls while capturing epsilon/delta trade-off.
            double score = rhoFromDpUpperBound(epsilonRound, deltaRound);

            if (score > bestScore) {
                bestScore = score;
                bestEpsilonRound = epsilonRound;
                bestDeltaRound = deltaRound;
            }
        }

        return new PerRoundBudget(bestEpsilonRound, bestDeltaRound);
    }

    /**
     * Computes the delta that corresponds to a given rho-zCDP and epsilon bound.
     * <p>
     * Uses Renyi DP conversion: optimizes over Renyi order alpha to find tightest delta
     * such that rho-zCDP implies (eps, delta)-DP.
     * <p>
     * Formula: delta = exp((alpha-1)(alpha*rho-eps) + alpha*ln(1-1/alpha)) / (alpha-1)
     *
     * @param rho zCDP privacy parameter
     * @param eps epsilon bound
     * @return the resulting delta value
     */
    private static double cdpDelta(double rho, double eps) {
        if (rho < 0 || eps < 0) {
            throw new IllegalArgumentException(
                "rho and epsilon must be non-negative"
            );
        }
        if (rho == 0) return 0.0;

        double amin = 1.01;
        double amax = (eps + 1) / (2 * rho) + 2;

        // Binary search for optimal alpha (minimize delta by finding root of derivative)
        for (int i = 0; i < 1000; i++) {
            double alpha = (amin + amax) / 2;
            double derivative =
                (2 * alpha - 1) * rho - eps + FastMath.log1p(-1.0 / alpha);
            if (derivative < 0) {
                amin = alpha;
            } else {
                amax = alpha;
            }
        }

        double alpha = (amin + amax) / 2;
        double delta =
            FastMath.exp(
                (alpha - 1) * (alpha * rho - eps) +
                    alpha * FastMath.log1p(-1.0 / alpha)
            ) /
            (alpha - 1.0);
        return FastMath.min(delta, 1.0);
    }

    /**
     * Advanced-composition epsilon for {@code rounds} compositions of
     * ({@code epsilonRound}, {@code deltaRound})-DP with free parameter
     * {@code deltaPrime}.
     */
    private static double advancedCompositionEpsilon(
        double epsilonRound,
        long rounds,
        double deltaPrime
    ) {
        double firstTerm =
            FastMath.sqrt(2.0 * rounds * FastMath.log(1.0 / deltaPrime)) *
            epsilonRound;
        double secondTerm =
            rounds * epsilonRound * (FastMath.exp(epsilonRound) - 1.0);
        return firstTerm + secondTerm;
    }

    /**
     * Solves for the largest per-round epsilon satisfying the advanced
     * composition bound for fixed total epsilon and deltaPrime.
     */
    private static double solveRoundEpsilonWithAdvancedComposition(
        double epsilonTotal,
        long rounds,
        double deltaPrime
    ) {
        double lower = 0.0;
        double upper = epsilonTotal;

        if (advancedCompositionEpsilon(upper, rounds, deltaPrime) <= epsilonTotal) {
            return upper;
        }

        for (int i = 0; i < 200; i++) {
            double mid = (lower + upper) / 2.0;
            if (advancedCompositionEpsilon(mid, rounds, deltaPrime) <= epsilonTotal) {
                lower = mid;
            } else {
                upper = mid;
            }
        }
        return lower;
    }

    /**
     * Closed-form rho recovered from the standard zCDP -> (epsilon, delta)-DP upper bound:
     * epsilon = rho + 2*sqrt(rho*ln(1/delta)).
     */
    private static double rhoFromDpUpperBound(double epsilon, double delta) {
        if (epsilon <= 0 || delta <= 0 || delta >= 1) {
            return 0.0;
        }
        double logInvDelta = FastMath.log(1.0 / delta);
        double sqrtLogInvDelta = FastMath.sqrt(logInvDelta);
        double y = FastMath.sqrt(logInvDelta + epsilon) - sqrtLogInvDelta;
        return y * y;
    }

    /**
     * Computes the Gaussian sigma needed for binary tree aggregation under rho-zCDP.
     * <p>
     * Formula from Theorem C.1: rho = (L^2 * ceil(log2(T))) / (2 * sigma^2)
     * Rearranged: sigma = L * sqrt(ceil(log2(T)) / (2 * rho))
     *
     * @param rho zCDP privacy parameter
     * @param T   number of releases/time steps supported by the tree
     * @param L   per-user L1 sensitivity
     * @return standard deviation for the Gaussian noise
     */
    public static double calculateSigma(double rho, double T, double L) {
        // use ceiling of log base 2 for tree height calculation as per theorem C.1
        double log2T = FastMath.ceil(FastMath.log(T) / FastMath.log(2));
        return FastMath.sqrt((log2T * L * L) / (2 * rho));
    }

    /**
     * Returns the user-level L1 sensitivity C * L_m.
     *
     * @param maxContributionsPerUser maximum contributions per user (C)
     * @param perRecordClamp          per-record clamp value (L_m)
     * @return the L1 sensitivity
     */
    public static double l1Sensitivity(
        long maxContributionsPerUser,
        double perRecordClamp
    ) {
        return maxContributionsPerUser * perRecordClamp;
    }
}
