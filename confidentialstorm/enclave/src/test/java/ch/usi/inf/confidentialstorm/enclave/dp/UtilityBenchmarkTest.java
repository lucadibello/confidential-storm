package ch.usi.inf.confidentialstorm.enclave.dp;

import ch.usi.inf.confidentialstorm.enclave.util.DPUtil;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.util.FastMath;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Monte Carlo utility benchmark for {@link StreamingDPMechanism}, replicating
 * the
 * synthetic experiment of Section 5.1 of the DP-SQLP paper.
 * <p>
 * Since we are not able to replicate the same exact workload and computational
 * environment as the paper, we focus on measuring the utility metrics (l0,
 * l_inf, l1, l2) of our implementation under the same DP parameters and data
 * distribution assumptions as the paper. To give comparable results, we follow
 * the same data distribution approach as the paper, and use a similar batch
 * stremaing setup (with similar batch size) and repeat the experiment multiple
 * times to get an empirical estimate of the expected utility.
 * <p>
 * IMPORTANT: this test is gated behind {@code -Dbenchmark=true} so it is
 * excluded from the regular {@code mvn test} run. Suggested usage for full
 * scale execution:
 * {@code mvn clean test -Dbenchmark=true -Dbenchmark.fast=false}.
 * Fast mode (default when {@code -Dbenchmark=true}) uses smaller dataset sizes
 * and finishes within a couple of minutes.
 */
@Tag("benchmark")
@EnabledIfSystemProperty(named = "benchmark", matches = "true")
@Execution(ExecutionMode.SAME_THREAD)
class UtilityBenchmarkTest {

  // -------------------------------------------------------------------------
  // Paper DP parameters (Section 5.1)
  // -------------------------------------------------------------------------

  private static final double EPSILON = 6.0;
  private static final double DELTA = 1e-9;
  private static final double EPS_K = EPSILON / 2.0;
  private static final double EPS_H = EPSILON / 2.0;
  private static final double DELTA_K = DELTA * 2.0 / 3.0;
  private static final double DELTA_H = DELTA / 3.0;
  private static final long C = 32L;
  private static final double L = 1.0;
  private static final long MU = 50L;

  /**
   * Fraction of the per-round key-selection delta budget reserved for the
   * threshold-failure cost (e^{eps}+1) * beta of Algorithm 1. This is the
   * pre-allocation approach described in the thesis background chapter
   * (paragraph "Choosing the Accuracy Parameter beta").
   */
  private static final double ALPHA = 0.1;

  // -------------------------------------------------------------------------
  // Data-generation parameters
  // -------------------------------------------------------------------------

  /**
   * Fast-mode toggle. CI default: true. Override with -Dbenchmark.fast=false for
   * paper-scale.
   */
  private static final boolean FAST_MODE = Boolean.parseBoolean(System.getProperty("benchmark.fast", "true"));

  private static final int NUM_USERS = FAST_MODE ? 500_000 : 10_000_000;
  private static final int NUM_KEYS = FAST_MODE ? 100_000 : 1_000_000;
  private static final int NUM_RUNS = FAST_MODE ? 5 : 20;

  private static final int MAX_TIME_STEP_SMALL = 100;
  private static final int MAX_TIME_STEP_LARGE = 1000;

  // Zipf-Mandelbrot parameters from Section 5.1
  private static final int USER_DIST_N = 100_000;
  private static final double USER_DIST_Q = 26.0;
  private static final double USER_DIST_S = 6.738;
  private static final double KEY_DIST_Q = 1000.0;
  private static final double KEY_DIST_S = 1.4;

  private static final long BASE_SEED = 42L;

  // -------------------------------------------------------------------------
  // Tests
  // -------------------------------------------------------------------------

  @Test
  void benchmarkT100() {
    runBenchmark(MAX_TIME_STEP_SMALL);
  }

  @Test
  void benchmarkT1000() {
    runBenchmark(MAX_TIME_STEP_LARGE);
  }

  // -------------------------------------------------------------------------
  // Core benchmark loop
  // -------------------------------------------------------------------------

  private void runBenchmark(int T) {
    System.out.printf(Locale.ROOT,
        "%n=== UtilityBenchmark: T=%d, NUM_USERS=%d, NUM_KEYS=%d, NUM_RUNS=%d (fast=%s) ===%n",
        T, NUM_USERS, NUM_KEYS, NUM_RUNS, FAST_MODE);
    logCalibration(T);

    double[][] perRun = new double[NUM_RUNS][];
    long totalNanos = 0L;

    for (int run = 0; run < NUM_RUNS; run++) {
      long t0 = System.nanoTime();
      perRun[run] = runOnce(T, BASE_SEED + run);
      long elapsed = System.nanoTime() - t0;
      totalNanos += elapsed;

      System.out.printf(Locale.ROOT,
          "  run %2d/%2d  l0=%-10.0f  l_inf=%-10.0f  l_1=%-12.0f  l_2=%-10.2f  (%.1fs)%n",
          run + 1, NUM_RUNS,
          perRun[run][0], perRun[run][1], perRun[run][2], perRun[run][3],
          elapsed / 1e9);
    }

    double[] mean = new double[4];
    double[] std = new double[4];
    for (double[] m : perRun) {
      for (int i = 0; i < 4; i++)
        mean[i] += m[i];
    }
    for (int i = 0; i < 4; i++)
      mean[i] /= NUM_RUNS;
    for (double[] m : perRun) {
      for (int i = 0; i < 4; i++) {
        double d = m[i] - mean[i];
        std[i] += d * d;
      }
    }
    for (int i = 0; i < 4; i++)
      std[i] = FastMath.sqrt(std[i] / NUM_RUNS);

    System.out.printf(Locale.ROOT,
        "%n=== T=%d micro-batches (avg over %d runs) ===%n", T, NUM_RUNS);
    System.out.printf(Locale.ROOT, "%-18s %-20s %-12s%n", "Metric", "DP-SQLP (ours)", "stdev");
    System.out.printf(Locale.ROOT, "%-18s %-20.2f %-12.2f%n", "Keys retained", mean[0], std[0]);
    System.out.printf(Locale.ROOT, "%-18s %-20.2f %-12.2f%n", "l_inf norm", mean[1], std[1]);
    System.out.printf(Locale.ROOT, "%-18s %-20.2f %-12.2f%n", "l_1  norm", mean[2], std[2]);
    System.out.printf(Locale.ROOT, "%-18s %-20.2f %-12.2f%n", "l_2  norm", mean[3], std[3]);
    System.out.printf(Locale.ROOT, "Total wall-clock: %.1fs (%.1fs / run)%n",
        totalNanos / 1e9, totalNanos / 1e9 / NUM_RUNS);
  }

  /**
   * One Monte Carlo trial: builds a fresh mechanism + dataset with the given
   * seed,
   * streams all T batches through the mechanism, and returns the final-step
   * (l0, l_inf, l1, l2) metrics versus the unnoised ground-truth histogram.
   *
   * @param T    number of micro-batches (time steps) to stream through the
   * @param seed random seed for data generation (different runs should use
   *             different seeds)
   * @return array of (l0, l_inf, l1, l2)
   */
  private double[] runOnce(int T, long seed) {
    Random rng = new Random(seed);
    ZipfMandelbrot userDist = new ZipfMandelbrot(USER_DIST_N, USER_DIST_Q, USER_DIST_S, rng);
    ZipfMandelbrot keyDist = new ZipfMandelbrot(NUM_KEYS, KEY_DIST_Q, KEY_DIST_S, rng);

    // We generatate the entire dataset upfront to avoid including data generation
    // time in the benchmark. Each entry is a (userId, keyRank) pair. Also, we free
    // the batch list once consumed below.
    @SuppressWarnings("unchecked")
    List<int[]>[] batches = new List[T];
    for (int t = 0; t < T; t++)
      batches[t] = new ArrayList<>();

    for (int u = 0; u < NUM_USERS; u++) {
      long budget = Math.min((long) userDist.sample(), C);
      for (long c = 0; c < budget; c++) {
        int batchIdx = rng.nextInt(T);
        int keyRank = keyDist.sample();
        batches[batchIdx].add(new int[] { u, keyRank });
      }
    }

    // Build a fresh mechanism for this run, stream all batches through it
    StreamingDPMechanism mechanism = buildMechanism(T);
    Map<String, Long> groundTruth = new HashMap<>();
    Map<String, Long> dpHistogram = null;

    // cycle thorugh time steps 0..T-1, feeding the corresponding batch into the
    // mechanism and snapshotting
    for (int t = 0; t < T; t++) {
      // feed entire batch into the mechanism at time step tr_i
      for (int[] rec : batches[t]) {
        String userId = "u" + rec[0];
        String key = Integer.toString(rec[1]);
        mechanism.addContribution(userId, key, 1.0);
        groundTruth.merge(key, 1L, Long::sum);
      }
      batches[t] = null; // free per-batch backing array as we consume it
      dpHistogram = mechanism.snapshot(); // produce snapshot for timestep tr_i
    }

    // after the final step, compute and return the (l0, l_inf, l1, l2)
    return computeMetrics(dpHistogram, groundTruth);
  }

  /**
   * Computes (l0, l_inf, l1, l2) over the union of the released histogram and the
   * ground-truth histogram keys. Missing keys on either side are treated as 0.
   */
  private double[] computeMetrics(Map<String, Long> dp, Map<String, Long> truth) {
    Set<String> allKeys = new HashSet<>(dp.size() + truth.size());
    allKeys.addAll(dp.keySet());
    allKeys.addAll(truth.keySet());

    long lInf = 0L;
    long l1 = 0L;
    double l2sq = 0.0;
    for (String k : allKeys) {
      long diff = dp.getOrDefault(k, 0L) - truth.getOrDefault(k, 0L);
      long ad = Math.abs(diff);
      if (ad > lInf)
        lInf = ad;
      l1 += ad;
      l2sq += (double) diff * (double) diff;
    }
    return new double[] { dp.size(), lInf, l1, FastMath.sqrt(l2sq) };
  }

  /**
   * Calibrates sigmas and the threshold quantile following the privacy-tight
   * pre-allocation approach from the background chapter (and as implemented in
   * {@link ch.usi.inf.confidentialstorm.enclave.service.bolts.base.dp.AbstractDataPerturbationServiceProvider})
   */
  private StreamingDPMechanism buildMechanism(int T) {
    DPUtil.PerRoundBudget keyRoundBudget = DPUtil.keySelectionPerRoundBudget(EPS_K, DELTA_K, C);

    double deltaGaussianShare = DPUtil.gaussianShareDelta(keyRoundBudget.delta(), ALPHA);
    double rhoK = DPUtil.cdpRho(keyRoundBudget.epsilon(), deltaGaussianShare);
    double sigmaKey = DPUtil.calculateSigma(rhoK, T, 1.0);

    double beta = DPUtil.computeBeta(keyRoundBudget.epsilon(), keyRoundBudget.delta(), ALPHA);
    double thresholdQuantile = new NormalDistribution(0.0, 1.0)
        .inverseCumulativeProbability(1.0 - beta);

    double rhoH = DPUtil.cdpRho(EPS_H, DELTA_H);
    double sigmaHist = DPUtil.calculateSigma(rhoH, T, DPUtil.l1Sensitivity(C, L));

    return new StreamingDPMechanism(sigmaKey, sigmaHist, thresholdQuantile, T, MU, C);
  }

  private void logCalibration(int T) {
    DPUtil.PerRoundBudget keyRoundBudget = DPUtil.keySelectionPerRoundBudget(EPS_K, DELTA_K, C);
    double deltaGaussianShare = DPUtil.gaussianShareDelta(keyRoundBudget.delta(), ALPHA);
    double rhoK = DPUtil.cdpRho(keyRoundBudget.epsilon(), deltaGaussianShare);
    double sigmaKey = DPUtil.calculateSigma(rhoK, T, 1.0);

    double beta = DPUtil.computeBeta(keyRoundBudget.epsilon(), keyRoundBudget.delta(), ALPHA);
    double thresholdQuantile = new NormalDistribution(0.0, 1.0)
        .inverseCumulativeProbability(1.0 - beta);

    double rhoH = DPUtil.cdpRho(EPS_H, DELTA_H);
    double sigmaHist = DPUtil.calculateSigma(rhoH, T, DPUtil.l1Sensitivity(C, L));

    double kappa = FastMath.ceil(FastMath.log(T) / FastMath.log(2));
    double honakerNodeVariance = sigmaKey * sigmaKey / (2.0 * (1.0 - FastMath.pow(2.0, -kappa)));
    double tauAtLastStep = FastMath.sqrt(kappa * honakerNodeVariance) * thresholdQuantile;

    System.out.printf(Locale.ROOT,
        "calibration: eps_k_round=%.4f delta_k_round=%.3e (alpha=%.2f)%n"
            + "             rho_k=%.4e sigma_k=%.3f%n"
            + "             rho_h=%.4e sigma_h=%.3f  (L1=%.1f)%n"
            + "             beta=%.3e Phi^-1(1-beta)=%.3f%n"
            + "             kappa=ceil(log2 T)=%.0f%n"
            + "             tau_T-1 ~= %.2f   (release threshold mu+tau = %.2f, mu=%d)%n",
        keyRoundBudget.epsilon(), keyRoundBudget.delta(), ALPHA,
        rhoK, sigmaKey,
        rhoH, sigmaHist, DPUtil.l1Sensitivity(C, L),
        beta, thresholdQuantile,
        kappa,
        tauAtLastStep, MU + tauAtLastStep, MU);
  }

  // -------------------------------------------------------------------------
  // Inline Zipf-Mandelbrot sampler to avoid cross-module dependencies!
  // -------------------------------------------------------------------------

  private static final class ZipfMandelbrot {
    private final int N;
    private final Random rng;
    private final double[] cdf;

    ZipfMandelbrot(int N, double q, double s, Random rng) {
      if (N <= 0)
        throw new IllegalArgumentException("N must be positive");
      if (q < 0)
        throw new IllegalArgumentException("q must be >= 0");
      if (s <= 0)
        throw new IllegalArgumentException("s must be > 0");
      this.N = N;
      this.rng = rng;

      double[] cumulative = new double[N];
      double sum = 0.0;
      for (int k = 1; k <= N; k++) {
        sum += 1.0 / FastMath.pow(k + q, s);
        cumulative[k - 1] = sum;
      }
      // Normalize so cumulative[N-1] == 1.0 exactly.
      double norm = cumulative[N - 1];
      for (int i = 0; i < N; i++)
        cumulative[i] /= norm;
      cumulative[N - 1] = 1.0;
      this.cdf = cumulative;
    }

    /** Inverse-transform sampling via binary search. Returns rank in [1, N]. */
    int sample() {
      double u = rng.nextDouble();
      int left = 0;
      int right = N - 1;
      while (left < right) {
        int mid = (left + right) >>> 1;
        if (cdf[mid] < u) {
          left = mid + 1;
        } else {
          right = mid;
        }
      }
      return left + 1;
    }
  }
}
