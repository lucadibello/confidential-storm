package ch.usi.inf.confidentialstorm.enclave.util;

import org.apache.commons.math3.util.FastMath;

/**
 * Utility class for Differential Privacy (DP) calculations.
 * Includes methods for converting DP parameters and calculating noise scales.
 */
public class DPUtil {

  /**
   * Number of candidate delta' values explored when deriving per-round
   * key-selection budget.
   */
  private static final int KEY_SELECTION_BUDGET_SEARCH_STEPS = 1024;

  /**
   * Holder for per-round privacy budget obtained from composition.
   */
  public static final record PerRoundBudget(double epsilon, double delta) {
  }

  /**
   * Converts an (epsilon, delta)-DP guarantee into an equivalent rho-zCDP
   * guarantee.
   * <p>
   * Uses tight Renyi DP conversion [Bun & Steinke 2016] via binary search to find
   * the maximum rho such that rho-zCDP implies (eps, delta)-DP, which minimizes
   * the noise needed while satisfying the privacy constraint, maximizing utility.
   * <p>
   * source: <a href=
   * "https://github.com/IBM/discrete-gaussian-differential-privacy/blob/cb190d2a990a78eff6e21159203bc888e095f01b/cdp2adp.py#L104-L119">cdp2adp.py</a>
   *
   * @param eps   target epsilon (privacy loss parameter)
   * @param delta target delta (failure probability in (eps, delta)-DP)
   * @return rho parameter for zCDP satisfying the requested (eps, delta)-DP
   */
  public static double cdpRho(double eps, double delta) {
    // verify input bounds: epsilon must be non-negative and delta must be in (0, 1)
    if (eps < 0 || delta <= 0) {
      throw new IllegalArgumentException(
          "epsilon must be non-negative and delta must be positive");
    }
    if (delta >= 1)
      return 0.0;

    double rho_min = 0.0;
    double rho_max = eps + 1;

    // Binary search: find max rho where cdpDelta(rho, eps) <= delta
    // Higher rho means weaker privacy and higher delta, so we want
    // largest valid rho
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
   * Computes the delta that corresponds to a given rho-zCDP and epsilon bound.
   * Used by the binary search in {@link #cdpRho(double, double)} to find the
   * optimal rho for a target (eps, delta)-DP guarantee.
   * <p>
   * Source: <a href=
   * "https://github.com/IBM/discrete-gaussian-differential-privacy/blob/cb190d2a990a78eff6e21159203bc888e095f01b/cdp2adp.py#L74-L102">cdp2adp.py</a>
   *
   * @param rho zCDP privacy parameter
   * @param eps epsilon bound
   * @return the resulting delta value
   */
  private static double cdpDelta(double rho, double eps) {
    if (rho < 0 || eps < 0) {
      throw new IllegalArgumentException(
          "rho and epsilon must be non-negative");
    }
    if (rho == 0)
      return 0.0;

    double amin = 1.01;
    double amax = (eps + 1) / (2 * rho) + 2;

    // Binary search for optimal alpha (minimize delta by finding root of
    // derivative)
    for (int i = 0; i < 1000; i++) {
      double alpha = (amin + amax) / 2;
      double derivative = (2 * alpha - 1) * rho - eps + FastMath.log1p(-1.0 / alpha);
      if (derivative < 0) {
        amin = alpha;
      } else {
        amax = alpha;
      }
    }

    double alpha = (amin + amax) / 2;
    double delta = FastMath.exp(
        (alpha - 1) * (alpha * rho - eps) +
            alpha * FastMath.log1p(-1.0 / alpha))
        /
        (alpha - 1.0);
    return FastMath.min(delta, 1.0);
  }

  /**
   * Converts a total key-selection privacy budget into a per-round budget using
   * C-fold advanced composition (Section 4.4 of the DP-SQLP paper).
   * <p>
   * We search over {@code deltaPrime} in:
   * 
   * <pre>
   * delta_total = rounds * delta_round + deltaPrime
   * </pre>
   * 
   * and maximize the feasible per-round epsilon from the advanced composition
   * bound:
   * 
   * <pre>
   * epsilon_total >= sqrt(2 * rounds * ln(1 / deltaPrime)) * epsilon_round
   *     + rounds * epsilon_round * (exp(epsilon_round) - 1)
   * </pre>
   *
   * @param epsilonTotal total epsilon budget allocated to key selection
   *                     (epsilon_k)
   * @param deltaTotal   total delta budget allocated to key selection (delta_k)
   * @param C            maximum number of key-selection rounds per user (C)
   * @return per-round budget for one Algorithm-1 execution
   */
  public static PerRoundBudget keySelectionPerRoundBudget(
      double epsilonTotal,
      double deltaTotal,
      long C) {
    // check key-selection privacy budget bounds
    if (epsilonTotal < 0 || deltaTotal <= 0) {
      throw new IllegalArgumentException(
          "epsilonTotal must be non-negative and deltaTotal must be positive");
    }
    // ensure C is positive to avoid division by zero and negative rounds
    if (C <= 0) {
      throw new IllegalArgumentException("rounds must be positive");
    }
    // if only one round, no K-Fold composition needed => return total budget
    // as per-round budget
    if (C == 1) {
      return new PerRoundBudget(epsilonTotal, deltaTotal);
    }
    // if epsilonTotal is zero, no composition cost for epsilon,
    // so allocate all delta
    if (epsilonTotal == 0.0) {
      return new PerRoundBudget(0.0, deltaTotal / C);
    }

    // initial values for best per-round budget and score
    double bestEpsilonRound = 0.0;
    double bestDeltaRound = deltaTotal / C;
    double bestScore = -1.0;

    // linear search over candidate deltaPrime values to find the one that maximizes
    // the per-round budget under the advanced composition constraint, which
    // minimizes noise and maximizes utility
    for (int i = 1; i < KEY_SELECTION_BUDGET_SEARCH_STEPS; i++) {
      // tune deltaPrime as a fraction of the total delta budget over the search
      // steps: deltaPrime \in (0, deltaTotal)
      double fraction = (double) i / (double) KEY_SELECTION_BUDGET_SEARCH_STEPS; // in (0,1)
      double deltaPrime = deltaTotal * fraction; // in (0, deltaTotal)
      // compute the remaining delta budget for the C rounds
      double deltaRound = (deltaTotal - deltaPrime) / C;
      if (deltaRound <= 0) {
        continue;
      }

      // compute the larget per-round epsilonRound that satisfies the K-Fold advanced
      // composition with K = C rounds.
      double epsilonRound = solveRoundEpsilonWithAdvancedComposition(
          epsilonTotal,
          C,
          deltaPrime);

      // Score the candidate per-round budget using the rho implied by the standard
      // zCDP->DP conversion.
      // NOTE: since sigma_k scales with 1/sqrt(rho), maximizing rho minimizes noise
      // and maximizes utility.
      double score = rhoFromDpUpperBound(epsilonRound, deltaRound);

      // update best per-round budget if this candidate has a better score (higher
      // rho)
      if (score > bestScore) {
        bestScore = score;
        bestEpsilonRound = epsilonRound;
        bestDeltaRound = deltaRound;
      }
    }

    // return the best per-round budget found across the deltaPrime search
    return new PerRoundBudget(bestEpsilonRound, bestDeltaRound);
  }

  /**
   * Advanced-composition epsilon for {@code rounds} compositions of
   * ({@code epsilonRound}, {@code deltaRound})-DP with free parameter
   * {@code deltaPrime}.
   *
   * @param epsilonRound per-round epsilon
   * @param k            number of rounds/compositions
   * @param deltaPrime   free slack variable for delta in the advanced composition
   *                     bound
   */
  private static double adaptiveKFoldCompositionEpsilon(
      double epsilonRound,
      long k,
      double deltaPrime) {
    return FastMath.sqrt(2.0 * k * FastMath.log(1.0 / deltaPrime)) * epsilonRound +
        k * epsilonRound * (FastMath.exp(epsilonRound) - 1.0);
  }

  /**
   * Find the larger per-round epsilon^(k) such that the K-Fold adaptive
   * composition cot of k round is at most epsilonTotal, maximizing the per-round
   * privacy budget and minimizing the noise needed for each round (maximizing
   * utility)
   *
   * @param epsilonTotal total epsilon budget to be allocated across rounds
   * @param k            number of rounds/compositions
   * @param deltaPrime   free slack variable for delta in the advanced composition
   */
  private static double solveRoundEpsilonWithAdvancedComposition(
      double epsilonTotal,
      long k,
      double deltaPrime) {
    // local variables for binary search
    double lower = 0.0;
    double upper = epsilonTotal;

    // if already satisfied at upper bound, return immediately to avoid unnecessary
    // search
    if (adaptiveKFoldCompositionEpsilon(upper, k, deltaPrime) <= epsilonTotal) {
      return upper;
    }

    // binary search over 200 steps to find the largest epsilon^(k) such that the
    // composition cost is at most epsilonTotal
    for (int i = 0; i < 200; i++) {
      double mid = (lower + upper) / 2.0;
      if (adaptiveKFoldCompositionEpsilon(mid, k, deltaPrime) <= epsilonTotal) {
        lower = mid; // satisfies constraint, try larger
      } else {
        upper = mid; // too large, try smaller
      }
    }
    return lower;
  }

  /**
   * Closed-form rho recovered from the standard zCDP -> (epsilon, delta)-DP
   * conversion:
   * rho = (sqrt(epsilon + log(1/delta)) - sqrt(log(1/delta)))^2
   */
  private static double rhoFromDpUpperBound(double epsilon, double delta) {
    // check bounds
    if (epsilon <= 0 || delta <= 0 || delta >= 1) {
      return 0.0;
    }

    // compute rho from the closed-form formula for rho
    return FastMath.pow(
        FastMath.sqrt(epsilon + FastMath.log(1.0 / delta)) -
            FastMath.sqrt(FastMath.log(1.0 / delta)),
        2);

  }

  /**
   * Computes the Gaussian sigma needed for binary tree aggregation under
   * rho-zCDP.
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
      double perRecordClamp) {
    return maxContributionsPerUser * perRecordClamp;
  }

  /**
   * Computes the accuracy parameter beta for one execution of Algorithm 1
   * using the closed-form formula derived from the privacy analysis of
   * Theorem 3.1 in the DP-SQLP paper:
   * 
   * <pre>
   * beta = (alpha * deltaRound) / (exp(epsilonRound) + 1)
   * </pre>
   *
   * @param epsilonRound per-round epsilon allocated to one Algorithm 1 run
   * @param deltaRound   per-round delta allocated to one Algorithm 1 run
   * @param alpha        fraction of deltaRound reserved for the threshold
   *                     failure term, in (0, 1)
   * @return the accuracy parameter beta
   */
  public static double computeBeta(
      double epsilonRound,
      double deltaRound,
      double alpha) {
    // ensure that epsilonRound is non-negative, deltaRound is positive, and alpha
    // is in (0, 1)
    if (epsilonRound < 0 || deltaRound <= 0) {
      throw new IllegalArgumentException(
          "epsilonRound must be non-negative and deltaRound must be positive");
    }
    if (alpha <= 0 || alpha >= 1) {
      throw new IllegalArgumentException("alpha must lie in (0, 1)");
    }
    // compute beta using derived closed-form formula
    return (alpha * deltaRound) / (FastMath.exp(epsilonRound) + 1.0);
  }

  /**
   * Returns the Gaussian-noise share of the per-round delta budget under
   * the pre-allocation approach:
   * 
   * <pre>
   * delta^(r) = (1 - alpha) * deltaRound // gaussian noise share
   *             + alpha * deltaRound     // threshold failure share
   * </pre>
   * <p>
   * This is the delta against which sigma_k must be calibrated so that the
   * total per-round privacy cost remains (epsilonRound, deltaRound)-DP once
   * the threshold-failure term consumes alpha * deltaRound via
   * {@link #computeBeta(double, double, double)}.
   */
  public static double gaussianShareDelta(double deltaRound, double alpha) {
    // ensure valid bounds
    if (deltaRound <= 0) {
      throw new IllegalArgumentException("deltaRound must be positive");
    }
    if (alpha <= 0 || alpha >= 1) {
      throw new IllegalArgumentException("alpha must lie in (0, 1)");
    }
    // return the Gaussian noise share of the remaining delta budget after reserving
    // alpha * deltaRound for the threshold failure term beta.
    return (1.0 - alpha) * deltaRound;
  }
}
