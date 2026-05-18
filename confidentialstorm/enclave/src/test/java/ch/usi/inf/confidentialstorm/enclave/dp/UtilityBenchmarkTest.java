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
 * the synthetic experiment of Section 5.1 of the DP-SQLP paper.
 * <p>
 * Since we cannot replicate the same exact workload and computational
 * environment as the paper, we focus on measuring the utility metrics (l0,
 * l_inf, l1, l2) of our implementation under the same DP parameters and data
 * distribution assumptions, and on quantifying how each calibration choice
 * moves utility. The benchmark therefore runs several {@link BenchmarkConfig}
 * variants on every Monte Carlo seed and emits a single comparison table.
 * <p>
 * The configurations span three knobs (see {@link #defaultConfigs()} for the
 * exact set):
 * <ul>
 * <li>$\mu$ ($0$ matches Section 5.1; $50$ matches the Google Trends production
 * setting described in Section 6.2).</li>
 * <li>$\beta$ either privacy-tight from the pre-allocation approach (thesis
 * background, "Choosing the Accuracy Parameter $\beta$") or relaxed to a fixed
 * $10^{-5}$, which loosens the per-round guarantee but isolates the threshold
 * inflation caused by tiny $\beta$.</li>
 * <li>$C$-fold composition either the analytical
 * {@link DPUtil#keySelectionPerRoundBudget Dwork} bound (current production
 * default) or the {@link DPUtil#keySelectionPerRoundBudgetOptimal Kairouz-Oh-Viswanath}
 * optimal composition referenced in Section 4.4 of the paper.</li>
 * </ul>
 * <p>
 * IMPORTANT: this test is gated behind {@code -Dbenchmark=true} so it is
 * excluded from the regular {@code mvn test} run. Suggested usage for
 * paper-scale execution:
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

  /**
   * Fraction of the per-round key-selection delta budget reserved for the
   * threshold-failure cost (e^{eps}+1) * beta of Algorithm 1. This is the
   * pre-allocation approach described in the thesis background chapter
   * (paragraph "Choosing the Accuracy Parameter beta"). Only used when
   * {@link BetaMode#PRIVACY_TIGHT} is selected.
   */
  private static final double ALPHA_PRIVACY_TIGHT = 0.1;

  /**
   * Loose, fixed {@code beta} used in the {@link BetaMode#FIXED} configurations
   * to approximate the (unstated) accuracy parameter implicitly used in the
   * paper Section 5.1 measurements. The corresponding per-round guarantee is
   * $(\varepsilon_k^{(r)}, \delta_k^{(r)} + (e^{\varepsilon_k^{(r)}}+1)\beta)$-DP --
   * looser than the labeled $(\varepsilon_k, \delta_k)$ but the only way to
   * shrink $\Phi^{-1}(1-\beta)$ enough to come close to the paper utility.
   */
  private static final double LOOSE_BETA = 1e-5;

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
  // Configuration model
  // -------------------------------------------------------------------------

  /**
   * Which $C$-fold composition theorem to use when deriving the per-round
   * key-selection budget. {@code DWORK_ANALYTICAL} matches the production
   * default; {@code OPTIMAL_KOV} matches the "empirically tighter variant of
   * advanced composition" referenced in Section 4.4 of the paper.
   */
  enum CompositionMode {
    DWORK_ANALYTICAL,
    OPTIMAL_KOV
  }

  /**
   * How $\beta$ (and thus $\Phi^{-1}(1-\beta)$) is chosen.
   * {@code PRIVACY_TIGHT} uses the pre-allocation approach from the background
   * chapter ($\beta = \alpha\,\delta_k^{(r)} / (e^{\varepsilon_k^{(r)}}+1)$) so
   * the per-round cost is exactly $(\varepsilon_k^{(r)}, \delta_k^{(r)})$-DP.
   * {@code FIXED} bypasses the split, calibrates $\sigma_k$ against the full
   * $\delta_k^{(r)}$, and uses {@link #LOOSE_BETA}; the resulting per-round
   * guarantee is strictly looser.
   */
  enum BetaMode {
    PRIVACY_TIGHT,
    FIXED
  }

  /**
   * One row of the comparison table: a named combination of $\mu$, $\beta$
   * mode, and $C$-fold composition. The configurations are intentionally
   * benchmark-only and do not affect production calibration in
   * {@code AbstractDataPerturbationServiceProvider}.
   */
  record BenchmarkConfig(
      String name,
      long mu,
      BetaMode betaMode,
      double betaFixed,
      CompositionMode composition) {
  }

  /**
   * Derived numbers from one calibration of a {@link BenchmarkConfig}. Captures
   * everything {@link #buildMechanism} needs plus the diagnostic fields the
   * benchmark logs.
   */
  record Calibration(
      double epsRound,
      double deltaRound,
      double rhoK,
      double sigmaKey,
      double rhoH,
      double sigmaHist,
      double beta,
      double thresholdQuantile,
      double tauAtLastStep,
      double kappa) {
  }

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

  /**
   * The configurations exercised by Solution D of the thesis benchmark
   * narrative. The order is meaningful: each row changes exactly one knob
   * compared to a previous row so the table reads as an ablation -- mu,
   * then beta, then composition.
   */
  private static List<BenchmarkConfig> defaultConfigs() {
    List<BenchmarkConfig> configs = new ArrayList<>();
    configs.add(new BenchmarkConfig(
        "tight + mu=50 + Dwork",
        50L, BetaMode.PRIVACY_TIGHT, Double.NaN, CompositionMode.DWORK_ANALYTICAL));
    configs.add(new BenchmarkConfig(
        "tight + mu=0  + Dwork",
        0L, BetaMode.PRIVACY_TIGHT, Double.NaN, CompositionMode.DWORK_ANALYTICAL));
    configs.add(new BenchmarkConfig(
        "loose + mu=0  + Dwork",
        0L, BetaMode.FIXED, LOOSE_BETA, CompositionMode.DWORK_ANALYTICAL));
    configs.add(new BenchmarkConfig(
        "tight + mu=0  + KOV",
        0L, BetaMode.PRIVACY_TIGHT, Double.NaN, CompositionMode.OPTIMAL_KOV));
    configs.add(new BenchmarkConfig(
        "loose + mu=0  + KOV",
        0L, BetaMode.FIXED, LOOSE_BETA, CompositionMode.OPTIMAL_KOV));
    return configs;
  }

  private void runBenchmark(int T) {
    List<BenchmarkConfig> configs = defaultConfigs();

    System.out.printf(Locale.ROOT,
        "%n=== UtilityBenchmark: T=%d, NUM_USERS=%d, NUM_KEYS=%d, NUM_RUNS=%d, configs=%d (fast=%s) ===%n",
        T, NUM_USERS, NUM_KEYS, NUM_RUNS, configs.size(), FAST_MODE);

    // Calibration depends only on (config, T), not on the seed; compute once
    // and reuse across all Monte Carlo runs.
    Calibration[] cals = new Calibration[configs.size()];
    for (int c = 0; c < configs.size(); c++) {
      cals[c] = calibrate(configs.get(c), T);
      logCalibration(configs.get(c), cals[c]);
    }

    // Accumulate metrics per config across all seeds. The dataset is generated
    // once per seed and shared across configurations, so configurations only
    // differ in calibration/mechanism state.
    double[][][] perRunPerConfig = new double[configs.size()][NUM_RUNS][];
    long[] totalNanosPerConfig = new long[configs.size()];

    for (int run = 0; run < NUM_RUNS; run++) {
      long seed = BASE_SEED + run;
      List<int[]>[] batches = generateBatches(T, seed);

      System.out.printf(Locale.ROOT, "%n[run %2d/%2d, seed=%d]%n", run + 1, NUM_RUNS, seed);
      for (int c = 0; c < configs.size(); c++) {
        BenchmarkConfig cfg = configs.get(c);
        Calibration cal = cals[c];

        long t0 = System.nanoTime();
        double[] metrics = runOnce(batches, T, cal, cfg.mu());
        long elapsed = System.nanoTime() - t0;
        totalNanosPerConfig[c] += elapsed;

        perRunPerConfig[c][run] = metrics;
        System.out.printf(Locale.ROOT,
            "  %-25s  l0=%-8.0f  l_inf=%-8.0f  l_1=%-12.0f  l_2=%-10.2f  (%.1fs)%n",
            cfg.name(), metrics[0], metrics[1], metrics[2], metrics[3], elapsed / 1e9);
      }
    }

    // Aggregate into mean/stdev per config and print the final comparison table.
    System.out.printf(Locale.ROOT,
        "%n=== T=%d micro-batches (avg over %d runs) ===%n", T, NUM_RUNS);
    System.out.printf(Locale.ROOT, "%-28s | %-14s | %-14s | %-18s | %-14s | %-10s%n",
        "Configuration", "Keys", "l_inf", "l_1", "l_2", "sec/run");
    System.out.printf(Locale.ROOT, "%s%n", "-".repeat(28 + 14 * 3 + 18 + 10 + 5 * 3));
    for (int c = 0; c < configs.size(); c++) {
      double[] mean = new double[4];
      double[] std = new double[4];
      for (int r = 0; r < NUM_RUNS; r++) {
        for (int i = 0; i < 4; i++)
          mean[i] += perRunPerConfig[c][r][i];
      }
      for (int i = 0; i < 4; i++)
        mean[i] /= NUM_RUNS;
      for (int r = 0; r < NUM_RUNS; r++) {
        for (int i = 0; i < 4; i++) {
          double d = perRunPerConfig[c][r][i] - mean[i];
          std[i] += d * d;
        }
      }
      for (int i = 0; i < 4; i++)
        std[i] = FastMath.sqrt(std[i] / NUM_RUNS);

      double avgSec = totalNanosPerConfig[c] / 1e9 / NUM_RUNS;
      System.out.printf(Locale.ROOT,
          "%-28s | %6.0f +- %-3.0f | %6.0f +- %-3.0f | %8.0f +- %-7.0f | %6.0f +- %-3.0f | %10.1f%n",
          configs.get(c).name(),
          mean[0], std[0],
          mean[1], std[1],
          mean[2], std[2],
          mean[3], std[3],
          avgSec);
    }
  }

  /**
   * Generates the full batched dataset for a single Monte Carlo seed. The
   * dataset is reused across all benchmark configurations on the same seed so
   * the comparison isolates calibration differences rather than sampling noise.
   */
  @SuppressWarnings("unchecked")
  private List<int[]>[] generateBatches(int T, long seed) {
    Random rng = new Random(seed);
    ZipfMandelbrot userDist = new ZipfMandelbrot(USER_DIST_N, USER_DIST_Q, USER_DIST_S, rng);
    ZipfMandelbrot keyDist = new ZipfMandelbrot(NUM_KEYS, KEY_DIST_Q, KEY_DIST_S, rng);

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
    return batches;
  }

  /**
   * One Monte Carlo trial under a given calibration: streams all T batches
   * through a fresh mechanism and returns the final-step (l0, l_inf, l1, l2)
   * metrics versus the unnoised ground-truth histogram.
   */
  private double[] runOnce(List<int[]>[] batches, int T, Calibration cal, long mu) {
    StreamingDPMechanism mechanism = new StreamingDPMechanism(
        cal.sigmaKey(), cal.sigmaHist(), cal.thresholdQuantile(), T, mu, C);

    Map<String, Long> groundTruth = new HashMap<>();
    Map<String, Long> dpHistogram = null;

    for (int t = 0; t < T; t++) {
      for (int[] rec : batches[t]) {
        String userId = "u" + rec[0];
        String key = Integer.toString(rec[1]);
        mechanism.addContribution(userId, key, 1.0);
        groundTruth.merge(key, 1L, Long::sum);
      }
      dpHistogram = mechanism.snapshot();
    }
    return computeMetrics(dpHistogram, groundTruth);
  }

  /**
   * Computes (l0, l_inf, l1, l2) over the union of the released histogram and
   * the ground-truth histogram keys. Missing keys on either side are treated as
   * 0.
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
   * Translates a {@link BenchmarkConfig} into the concrete $\sigma_k$,
   * $\sigma_h$, and threshold quantile used by {@link StreamingDPMechanism}.
   * {@link BetaMode#PRIVACY_TIGHT} mirrors the production calibration in
   * {@code AbstractDataPerturbationServiceProvider}; {@link BetaMode#FIXED}
   * skips the $(1-\alpha)/\alpha$ split and uses {@link #LOOSE_BETA} directly.
   */
  private Calibration calibrate(BenchmarkConfig cfg, int T) {
    DPUtil.PerRoundBudget keyRoundBudget = switch (cfg.composition()) {
      case DWORK_ANALYTICAL -> DPUtil.keySelectionPerRoundBudget(EPS_K, DELTA_K, C);
      case OPTIMAL_KOV -> DPUtil.keySelectionPerRoundBudgetOptimal(EPS_K, DELTA_K, C);
    };

    double deltaForGaussian;
    double beta;
    switch (cfg.betaMode()) {
      case PRIVACY_TIGHT -> {
        deltaForGaussian = DPUtil.gaussianShareDelta(keyRoundBudget.delta(), ALPHA_PRIVACY_TIGHT);
        beta = DPUtil.computeBeta(keyRoundBudget.epsilon(), keyRoundBudget.delta(), ALPHA_PRIVACY_TIGHT);
      }
      case FIXED -> {
        deltaForGaussian = keyRoundBudget.delta();
        beta = cfg.betaFixed();
      }
      default -> throw new IllegalStateException("Unhandled beta mode: " + cfg.betaMode());
    }

    double rhoK = DPUtil.cdpRho(keyRoundBudget.epsilon(), deltaForGaussian);
    double sigmaKey = DPUtil.calculateSigma(rhoK, T, 1.0);

    double thresholdQuantile = new NormalDistribution(0.0, 1.0)
        .inverseCumulativeProbability(1.0 - beta);

    double rhoH = DPUtil.cdpRho(EPS_H, DELTA_H);
    double sigmaHist = DPUtil.calculateSigma(rhoH, T, DPUtil.l1Sensitivity(C, L));

    double kappa = FastMath.ceil(FastMath.log(T) / FastMath.log(2));
    double honakerNodeVariance = sigmaKey * sigmaKey / (2.0 * (1.0 - FastMath.pow(2.0, -kappa)));
    double tauAtLastStep = FastMath.sqrt(kappa * honakerNodeVariance) * thresholdQuantile;

    return new Calibration(
        keyRoundBudget.epsilon(), keyRoundBudget.delta(),
        rhoK, sigmaKey,
        rhoH, sigmaHist,
        beta, thresholdQuantile,
        tauAtLastStep, kappa);
  }

  private void logCalibration(BenchmarkConfig cfg, Calibration cal) {
    System.out.printf(Locale.ROOT,
        "calibration [%s]:%n"
            + "  eps_k_round=%.4f  delta_k_round=%.3e   composition=%s  beta_mode=%s%n"
            + "  rho_k=%.4e   sigma_k=%.3f%n"
            + "  rho_h=%.4e   sigma_h=%.3f  (L1=%.1f)%n"
            + "  beta=%.3e   Phi^-1(1-beta)=%.3f%n"
            + "  kappa=ceil(log2 T)=%.0f%n"
            + "  tau_T-1 ~= %.2f   release threshold mu+tau = %.2f   (mu=%d)%n",
        cfg.name(),
        cal.epsRound(), cal.deltaRound(), cfg.composition(), cfg.betaMode(),
        cal.rhoK(), cal.sigmaKey(),
        cal.rhoH(), cal.sigmaHist(), DPUtil.l1Sensitivity(C, L),
        cal.beta(), cal.thresholdQuantile(),
        cal.kappa(),
        cal.tauAtLastStep(), cfg.mu() + cal.tauAtLastStep(), cfg.mu());
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
