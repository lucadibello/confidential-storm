package ch.usi.inf.examples.microbatch_baseline.dp;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.util.FastMath;

/**
 * Utility class for Differential Privacy (DP) calculations.
 * (copy of confidentialstorm/enclave)
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
   * Per-round key-selection budget plus the $\rho$ used to calibrate
   * $\sigma_k$, as derived from one of the {@link CompositionMode} strategies.
   *
   * @param epsilon the per-round epsilon allocated to one Algorithm 1 run
   * @param delta   the per-round delta allocated to one Algorithm 1 run
   * @param rho     the per-round zCDP parameter against which $\sigma_k$ is
   *                calibrated
   */
  public static final record KeySelectionRoundBudget(double epsilon, double delta, double rho) {
  }

  /**
   * Fully derived noise/threshold parameters for one calibration of
   * {@link StreamingDPMechanism}.
   * Captures everything needed to construct the mechanism plus the intermediate
   * diagnostics callers may want to log.
   *
   * @param epsilonKeyRound per-round key-selection epsilon
   * @param deltaKeyRound   per-round key-selection delta
   * @param rhoKey          per-round key-selection zCDP parameter
   * @param sigmaKey        Gaussian noise scale for key selection (Algorithm 1)
   * @param rhoHist         histogram-release zCDP parameter
   * @param sigmaHist       Gaussian noise scale for histogram release (Algorithm 2)
   * @param beta            accuracy parameter of Algorithm 1
   * @param thresholdQuantile $\Phi^{-1}(1 - \beta)$ of the standard normal
   */
  public static final record DpCalibration(
      double epsilonKeyRound,
      double deltaKeyRound,
      double rhoKey,
      double sigmaKey,
      double rhoHist,
      double sigmaHist,
      double beta,
      double thresholdQuantile) {
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
   * Converts a total key-selection privacy budget into a per-round budget using
   * the homogeneous closed form of the Kairouz-Oh-Viswanath optimal $k$-fold
   * composition theorem (Theorem 3.5 of {@code kairouz2015composition}). This is
   * the "empirically tighter variant of advanced composition" referenced in
   * Section 4.4 of the DP-SQLP paper and gives a strictly smaller composition
   * cost than {@link #keySelectionPerRoundBudget(double, double, long)} for the
   * same target $(\varepsilon_k, \delta_k)$.
   * <p>
   * For $k$ independent $(\varepsilon, \delta)$-DP mechanisms and any slack
   * $\widetilde{\delta} \in (0, 1)$, the composition satisfies
   * $(\varepsilon_g, \delta_g)$-DP with
   *
   * <pre>
   * eps_g = min{
   *   k * eps,
   *   (e^eps - 1) * k * eps / (e^eps + 1) + eps * sqrt(2k * ln(e + sqrt(k*eps^2)/dPrime)),
   *   (e^eps - 1) * k * eps / (e^eps + 1) + eps * sqrt(2k * ln(1/dPrime))
   * }
   * delta_g = 1 - (1 - delta)^k * (1 - dPrime)
   * </pre>
   *
   * As in {@link #keySelectionPerRoundBudget(double, double, long)} we search
   * over the slack $\widetilde{\delta}$ and, for each candidate, solve the
   * per-round
   * $\varepsilon$ that saturates the bound. The score is the closed-form $\rho$
   * proxy (smaller $\sigma$ for larger $\rho$), so the returned per-round budget
   * is the one that minimizes the Gaussian-noise scale of Algorithm 1.
   *
   * @param epsilonTotal total epsilon budget allocated to key selection
   *                     (epsilon_k)
   * @param deltaTotal   total delta budget allocated to key selection (delta_k)
   * @param C            maximum number of key-selection rounds per user (C)
   * @return per-round budget for one Algorithm-1 execution
   */
  public static PerRoundBudget keySelectionPerRoundBudgetOptimal(
      double epsilonTotal,
      double deltaTotal,
      long C) {
    if (epsilonTotal < 0 || deltaTotal <= 0) {
      throw new IllegalArgumentException(
          "epsilonTotal must be non-negative and deltaTotal must be positive");
    }
    if (C <= 0) {
      throw new IllegalArgumentException("rounds must be positive");
    }
    if (C == 1) {
      return new PerRoundBudget(epsilonTotal, deltaTotal);
    }
    if (epsilonTotal == 0.0) {
      return new PerRoundBudget(0.0, deltaTotal / C);
    }

    double bestEpsilonRound = 0.0;
    double bestDeltaRound = deltaTotal / C;
    double bestScore = -1.0;

    for (int i = 1; i < KEY_SELECTION_BUDGET_SEARCH_STEPS; i++) {
      // dPrime in (0, deltaTotal)
      double fraction = (double) i / (double) KEY_SELECTION_BUDGET_SEARCH_STEPS;
      double dPrime = deltaTotal * fraction;
      if (dPrime <= 0.0 || dPrime >= 1.0) {
        continue;
      }

      // Numerically stable inversion of delta_g = 1 - (1-delta)^k * (1-dPrime):
      // log(1-delta_round) = (log1p(-deltaTotal) - log1p(-dPrime)) / k
      // delta_round = -expm1((log1p(-deltaTotal) - log1p(-dPrime)) / k)
      double logRatio = FastMath.log1p(-deltaTotal) - FastMath.log1p(-dPrime);
      double deltaRound = -FastMath.expm1(logRatio / (double) C);
      if (!Double.isFinite(deltaRound) || deltaRound <= 0.0) {
        continue;
      }

      double epsilonRound = solveRoundEpsilonWithOptimalComposition(
          epsilonTotal, C, dPrime);
      if (epsilonRound <= 0.0) {
        continue;
      }

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
   * Homogeneous closed-form of the Kairouz-Oh-Viswanath composition bound
   * (Theorem 3.4 of {@code kairouz2015composition}): epsilon under k-fold
   * composition of an $(\varepsilon, \cdot)$-DP mechanism with slack
   * $\widetilde{\delta} = $ {@code dPrime}. We take the minimum of the three
   * upper bounds Kairouz et al. give (one is just sequential composition, the
   * other two are tighter for moderate epsilon).
   */
  private static double optimalKFoldCompositionEpsilon(
      double epsilonRound,
      long k,
      double dPrime) {
    // sanity check bounds
    if (epsilonRound <= 0.0) {
      return 0.0;
    }

    // precompute some repetitive terms

    // ((e^eps - 1) k eps) / (e^eps + 1) -- leading term in second and third bounds
    double expEps = FastMath.exp(epsilonRound);
    double leading = (expEps - 1.0) * epsilonRound * k / (expEps + 1.0);
    // k * eps ^2 -- term inside the second and third bound logarithm
    double kEpsSquared = k * epsilonRound * epsilonRound;

    // Compute each bound
    // A) Plain sequential composition: k * epsilonRound
    double boundA = k * epsilonRound;
    // B) ((e^eps -1) k eps)/(e^eps+1) + eps * sqrt(2k log(e+(sqrt(k eps^2)/
    // delta'))
    double boundB = leading
        + epsilonRound * FastMath.sqrt(
            2.0 * k * FastMath.log(FastMath.E + FastMath.sqrt(kEpsSquared) / dPrime));
    // C) ((e^eps -1) k eps)/(e^eps+1) + eps * sqrt(2k log(1/ delta'))
    double boundC = leading
        + epsilonRound * FastMath.sqrt(2.0 * k * FastMath.log(1.0 / dPrime));

    // select minimum of three bounds: min{A,B,C}
    return FastMath.min(boundA, FastMath.min(boundB, boundC));
  }

  /**
   * Find the largest per-round epsilon such that the Kairouz-Oh-Viswanath
   * composition cost over $C$ rounds is at most {@code epsilonTotal}.
   */
  private static double solveRoundEpsilonWithOptimalComposition(
      double epsilonTotal,
      long k,
      double dPrime) {
    double lower = 0.0;
    double upper = epsilonTotal;
    if (optimalKFoldCompositionEpsilon(upper, k, dPrime) <= epsilonTotal) {
      return upper;
    }
    for (int i = 0; i < 200; i++) {
      double mid = (lower + upper) / 2.0;
      if (optimalKFoldCompositionEpsilon(mid, k, dPrime) <= epsilonTotal) {
        lower = mid;
      } else {
        upper = mid;
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

  /**
   * Computes the threshold quantile $\Phi^{-1}(1 - \beta)$ of the standard
   * normal distribution used to scale the time-dependent key-selection
   * threshold $\tau_{tr_i} = \sqrt{\lambda^2_{tr_i}} \cdot \Phi^{-1}(1 - \beta)$.
   *
   * @param beta the accuracy parameter of Algorithm 1, in (0, 1)
   * @return the standard-normal quantile $\Phi^{-1}(1 - \beta)$
   */
  public static double thresholdQuantile(double beta) {
    if (beta <= 0.0 || beta >= 1.0) {
      throw new IllegalArgumentException("beta must lie in (0, 1); got " + beta);
    }
    return new NormalDistribution(0.0, 1.0).inverseCumulativeProbability(1.0 - beta);
  }

  /**
   * Derives the per-round key-selection budget (epsilon_round, delta_round) and
   * the per-round $\rho$ used to calibrate $\sigma_k$, for the requested
   * {@link CompositionMode}.
   * <p>
   * In all three modes the per-round delta is later split via the privacy-tight
   * pre-allocation into a Gaussian-noise share $(1-\alpha)\delta^{(r)}$ and a
   * threshold-failure share $\alpha\delta^{(r)}$ (see {@link #computeBeta} and
   * {@link #gaussianShareDelta}). The modes differ only in how the per-round
   * $\rho$ is obtained:
   * <ul>
   * <li>{@link CompositionMode#DWORK_ANALYTICAL} / {@link CompositionMode#OPTIMAL_KOV}:
   * compose in $(\varepsilon, \delta)$-DP to obtain (epsilon_round,
   * delta_round), then convert the Gaussian share to $\rho$ via
   * {@link #cdpRho(double, double)}.</li>
   * <li>{@link CompositionMode#ZCDP_LINEAR}: convert the total
   * $(\varepsilon_k, \delta_k)$ directly to $\rho_k$ and split linearly across
   * the $C$ rounds ($\rho^{(r)} = \rho_k / C$, $\delta^{(r)} = \delta_k / C$),
   * recovering epsilon_round from $\rho^{(r)}$ for the beta accounting.</li>
   * </ul>
   *
   * @param composition the C-fold composition theorem to apply
   * @param epsilonK    total epsilon budget for key selection
   * @param deltaK      total delta budget for key selection
   * @param C           maximum number of key-selection rounds per user
   * @param alpha       fraction of the per-round delta reserved for the
   *                    threshold-failure cost, in (0, 1)
   * @return the per-round key-selection budget including the calibration $\rho$
   */
  public static KeySelectionRoundBudget keySelectionRoundBudget(
      CompositionMode composition,
      double epsilonK,
      double deltaK,
      long C,
      double alpha) {
    switch (composition) {
      case DWORK_ANALYTICAL -> {
        // Dwork-Rothblum-Vadhan advanced composition, then convert the Gaussian
        // share of the per-round delta to rho.
        PerRoundBudget b = keySelectionPerRoundBudget(epsilonK, deltaK, C);
        double rho = cdpRho(b.epsilon(), gaussianShareDelta(b.delta(), alpha));
        return new KeySelectionRoundBudget(b.epsilon(), b.delta(), rho);
      }
      case OPTIMAL_KOV -> {
        // Kairouz-Oh-Viswanath optimal composition, then convert the Gaussian
        // share of the per-round delta to rho.
        PerRoundBudget b = keySelectionPerRoundBudgetOptimal(epsilonK, deltaK, C);
        double rho = cdpRho(b.epsilon(), gaussianShareDelta(b.delta(), alpha));
        return new KeySelectionRoundBudget(b.epsilon(), b.delta(), rho);
      }
      case ZCDP_LINEAR -> {
        // Skip (eps, delta)-DP composition: convert (eps_k, delta_k) directly to
        // rho_k and split linearly across rounds (sequential composition for
        // rho-zCDP). delta is partitioned linearly too so we have a per-round delta
        // for the beta accounting, and eps_round is recovered from rho_round via
        // Proposition 1.3 of the zCDP paper: rho-CDP => (rho + 2*sqrt(rho*ln(1/delta)),
        // delta)-DP.
        double rhoKTotal = cdpRho(epsilonK, deltaK);
        double rhoRound = rhoKTotal / C;
        double deltaRound = deltaK / C;
        double epsRound = rhoRound + 2.0 * FastMath.sqrt(rhoRound * FastMath.log(1.0 / deltaRound));
        return new KeySelectionRoundBudget(epsRound, deltaRound, rhoRound);
      }
      default -> throw new IllegalStateException("Unknown composition mode: " + composition);
    }
  }

  /**
   * Translates a high-level DP configuration into the concrete $\sigma_k$,
   * $\sigma_h$ and threshold quantile consumed by
   * {@link StreamingDPMechanism},
   * following DP-SQLP Section 4.4.
   * <p>
   * This wraps the full calibration pipeline so callers do not need to remember
   * the exact sequence of steps: derive the per-round key-selection budget for
   * the chosen {@link CompositionMode}, calibrate $\sigma_k$ against the
   * per-round $\rho$, derive $\beta$ and the threshold quantile from the
   * threshold-failure share, and calibrate $\sigma_h$ directly against
   * $(\varepsilon_h, \delta_h)$ with sensitivity $C \cdot L_m$.
   *
   * @param composition             the C-fold composition theorem to apply
   * @param epsilonK                total epsilon budget for key selection
   * @param deltaK                  total delta budget for key selection
   * @param epsilonH                total epsilon budget for histogram release
   * @param deltaH                  total delta budget for histogram release
   * @param C                       maximum number of contributions per user
   * @param T                       maximum number of time steps (triggering times)
   * @param perRecordClamp          per-record clamp value $L_m$
   * @param thresholdFailureFraction fraction $\alpha$ of the per-round
   *                                 key-selection delta reserved for the
   *                                 threshold-failure cost, in (0, 1)
   * @return the derived calibration parameters
   */
  public static DpCalibration calibrate(
      CompositionMode composition,
      double epsilonK,
      double deltaK,
      double epsilonH,
      double deltaH,
      long C,
      int T,
      double perRecordClamp,
      double thresholdFailureFraction) {
    final double alpha = thresholdFailureFraction;
    if (alpha <= 0.0 || alpha >= 1.0) {
      throw new IllegalArgumentException(
          "thresholdFailureFraction (alpha) must lie in (0, 1); got " + alpha);
    }
    if (C <= 0) {
      throw new IllegalArgumentException("maxUserContributions (C) must be positive; got " + C);
    }
    if (T <= 0) {
      throw new IllegalArgumentException("maxTimeSteps (T) must be positive; got " + T);
    }

    // 1. Per-round key-selection budget and calibration rho for the chosen theorem.
    KeySelectionRoundBudget keyRound = keySelectionRoundBudget(composition, epsilonK, deltaK, C, alpha);

    // 2. Gaussian noise for key selection (sensitivity 1).
    double sigmaKey = calculateSigma(keyRound.rho(), T, 1.0);

    // 3. Accuracy parameter beta and threshold quantile from the threshold-failure share.
    double beta = computeBeta(keyRound.epsilon(), keyRound.delta(), alpha);
    double thresholdQuantile = thresholdQuantile(beta);

    // 4. Histogram noise calibrated directly against (eps_h, delta_h) with sensitivity C * L_m.
    double rhoHist = cdpRho(epsilonH, deltaH);
    double sigmaHist = calculateSigma(rhoHist, T, l1Sensitivity(C, perRecordClamp));

    return new DpCalibration(
        keyRound.epsilon(), keyRound.delta(), keyRound.rho(), sigmaKey,
        rhoHist, sigmaHist,
        beta, thresholdQuantile);
  }
}
