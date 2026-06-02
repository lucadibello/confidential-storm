package ch.usi.inf.confidentialstorm.enclave.dp;

import ch.usi.inf.confidentialstorm.common.dp.CompositionMode;
import ch.usi.inf.confidentialstorm.enclave.util.DPUtil;
import org.apache.commons.math3.util.FastMath;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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
 * distribution assumptions, and on quantifying how the privacy-tight
 * pre-allocation parameter $\alpha$ moves utility. The benchmark therefore
 * runs several {@link BenchmarkConfig} variants on every Monte Carlo seed and
 * emits a CSV comparison table.
 * <p>
 * The configurations span two knobs:
 * <ul>
 * <li>$\alpha \in (0, 1)$, the share of the per-round delta budget reserved
 * for the threshold-failure cost $(e^{\varepsilon_k^{(r)}}+1)\beta$ from
 * Algorithm 1 (see thesis background, "Choosing the Accuracy Parameter
 * $\beta$"). Default: $\alpha = 0.5$. Sweep mode runs $\alpha \in \{0.1, 0.2,
 * \ldots, 0.9\}$; the exact bounds are excluded because $\alpha = 0$ gives
 * $\beta = 0$ (infinite threshold quantile, empty histogram) and $\alpha = 1$
 * gives a zero Gaussian-noise budget (undefined $\sigma_k$).</li>
 * <li>$C$-fold composition: one of the analytical
 * {@link DPUtil#keySelectionPerRoundBudget Dwork} bound, the
 * {@link DPUtil#keySelectionPerRoundBudgetOptimal Kairouz-Oh-Viswanath}
 * optimal composition referenced in Section 4.4 of the paper, or
 * {@code ZCDP_LINEAR} which skips $(\varepsilon, \delta)$-DP composition
 * entirely and splits the budget linearly in $\rho$-zCDP
 * ($\rho_k^{(r)} = \rho_k / C$). The latter is strictly tighter because the
 * $(\varepsilon, \delta) \to \rho$ conversion at the Gaussian-calibration
 * step is lossy.</li>
 * </ul>
 * <p>
 * $\mu$ is fixed at 0 to match the §5.1 synthetic experiment (the paper does
 * not state $\mu$ for §5.1; 0 is the natural "pure-threshold" choice and is
 * the value that reproduces Table 1 within ~5%).
 * <p>
 * IMPORTANT: this test is gated behind {@code -Dbenchmark=true} so it is
 * excluded from the regular {@code mvn test} run. Suggested usage for
 * paper-scale execution:
 * {@code mvn clean test -Dbenchmark=true -Dbenchmark.fast=false}.
 * To produce an alpha sweep, add {@code -Dbenchmark.alpha=true}.
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
   * Threshold offset $\mu$.
   */
  private static final long MU = 0L;

  // -------------------------------------------------------------------------
  // Data-generation parameters
  // -------------------------------------------------------------------------

  /**
   * Fast-mode toggle. CI default: true. Override with -Dbenchmark.fast=false for
   * paper-scale.
   */
  private static final boolean FAST_MODE = Boolean.parseBoolean(System.getProperty("benchmark.fast", "true"));

  /**
   * Alpha-sweep toggle. When true, sweeps $\alpha \in \{0.1, 0.2, \ldots,
   * 0.9\}$ to produce a utility-vs-alpha comparison plot. When false
   * (default), runs only the paper-correct baseline at $\alpha = 0.5$.
   */
  private static final boolean ALPHA_SWEEP = Boolean.parseBoolean(System.getProperty("benchmark.alpha", "false"));

  private static final int NUM_USERS = FAST_MODE ? 500_000 : 10_000_000;
  private static final int NUM_KEYS = FAST_MODE ? 100_000 : 1_000_000;
  private static final int NUM_RUNS = FAST_MODE ? 1 : 10;

  private static final int MAX_TIME_STEP_SMALL = 100;
  private static final int MAX_TIME_STEP_LARGE = 1000;

  // Zipf-Mandelbrot parameters from Section 5.1
  private static final int USER_DIST_N = 100_000;
  private static final double USER_DIST_Q = 26.0;
  private static final double USER_DIST_S = 6.738;
  private static final double KEY_DIST_Q = 1000.0;
  private static final double KEY_DIST_S = 1.4;

  private static final long BASE_SEED = 42L;

  private static final String CSV_HEADER = "T,alpha,composition,mu,run,"
      + "l0_mean,l_inf_mean,l1_mean,l2_mean,sec_per_run,"
      + "eps_round,delta_round,sigma_key,sigma_hist,beta,threshold_quantile,tau_at_last_step";

  // -------------------------------------------------------------------------
  // Configuration model
  // -------------------------------------------------------------------------

  /**
   * One row of the comparison table: a named combination of $\alpha$ and the
   * $C$-fold composition theorem ({@link CompositionMode}).
   */
  record BenchmarkConfig(
      String name,
      double alpha,
      CompositionMode composition) {
  }

  /**
   * Derived numbers from one calibration of a {@link BenchmarkConfig}. Captures
   * everything {@link #runOnce} needs plus the diagnostic fields the
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
   * Builds the list of benchmark configurations to run.
   *
   * @return list of configurations, in the order they will be printed in the
   *         final comparison table
   */
  private static List<BenchmarkConfig> defaultConfigs() {
    // Sweep stays strictly inside (0, 1): alpha=0 collapses beta to zero
    // (infinite threshold quantile), alpha=1 collapses the Gaussian-noise
    // budget to zero. Both endpoints produce degenerate metrics that would
    // distort any utility-vs-alpha plot.
    double[] alphas = ALPHA_SWEEP
        ? new double[] { 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9 }
        : new double[] { 0.5 };

    List<BenchmarkConfig> configs = new ArrayList<>(alphas.length * CompositionMode.values().length);
    for (double alpha : alphas) {
      for (CompositionMode comp : CompositionMode.values()) {
        String name = String.format(Locale.ROOT, "alpha=%.2f + %s", alpha, comp);
        configs.add(new BenchmarkConfig(name, alpha, comp));
      }
    }
    return configs;
  }

  /**
   * Runs the full benchmark for a given number of micro-batches $T$: generates
   * the datasets, runs all configurations on all Monte Carlo seeds, prints
   * the final comparison table, and writes per-run and summary metrics to a
   * timestamped CSV file.
   *
   * @param T number of micro-batches (time steps) to stream through the mechanism
   */
  private void runBenchmark(int T) {
    List<BenchmarkConfig> configs = defaultConfigs();

    System.out.printf(Locale.ROOT,
        "%n=== UtilityBenchmark: T=%d, NUM_USERS=%d, NUM_KEYS=%d, NUM_RUNS=%d, configs=%d "
            + "(fast=%s, alphaSweep=%s) ===%n",
        T, NUM_USERS, NUM_KEYS, NUM_RUNS, configs.size(), FAST_MODE, ALPHA_SWEEP);

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
    double[][] secPerRunPerConfig = new double[configs.size()][NUM_RUNS];

    for (int run = 0; run < NUM_RUNS; run++) {
      long seed = BASE_SEED + run;
      List<int[]>[] batches = generateBatches(T, seed);

      System.out.printf(Locale.ROOT, "%n[run %2d/%2d, seed=%d]%n", run + 1, NUM_RUNS, seed);
      for (int c = 0; c < configs.size(); c++) {
        BenchmarkConfig cfg = configs.get(c);

        long t0 = System.nanoTime();
        double[] metrics = runOnce(batches, T, cfg, MU);
        double elapsedSec = (System.nanoTime() - t0) / 1e9;

        perRunPerConfig[c][run] = metrics;
        secPerRunPerConfig[c][run] = elapsedSec;

        System.out.printf(Locale.ROOT,
            "  %-30s  l0=%-8.0f  l_inf=%-8.0f  l_1=%-12.0f  l_2=%-10.2f  (%.1fs)%n",
            cfg.name(), metrics[0], metrics[1], metrics[2], metrics[3], elapsedSec);
      }
    }

    // Aggregate into mean/stdev per config, print the final comparison table,
    // and gather CSV rows (one per run, plus a summary row per config).
    System.out.printf(Locale.ROOT,
        "%n=== T=%d micro-batches (avg over %d runs) ===%n", T, NUM_RUNS);
    System.out.printf(Locale.ROOT, "%-30s | %-14s | %-14s | %-18s | %-14s | %-10s%n",
        "Configuration", "Keys", "l_inf", "l_1", "l_2", "sec/run");
    System.out.printf(Locale.ROOT, "%s%n", "-".repeat(30 + 14 * 3 + 18 + 10 + 5 * 3));

    List<String> csvRows = new ArrayList<>(configs.size() * (NUM_RUNS + 1));
    for (int c = 0; c < configs.size(); c++) {
      BenchmarkConfig cfg = configs.get(c);
      Calibration cal = cals[c];

      double[] mean = new double[4];
      double[] std = new double[4];
      double meanSec = 0.0;
      for (int r = 0; r < NUM_RUNS; r++) {
        for (int i = 0; i < 4; i++)
          mean[i] += perRunPerConfig[c][r][i];
        meanSec += secPerRunPerConfig[c][r];
      }
      for (int i = 0; i < 4; i++)
        mean[i] /= NUM_RUNS;
      meanSec /= NUM_RUNS;
      for (int r = 0; r < NUM_RUNS; r++) {
        for (int i = 0; i < 4; i++) {
          double d = perRunPerConfig[c][r][i] - mean[i];
          std[i] += d * d;
        }
      }
      for (int i = 0; i < 4; i++)
        std[i] = FastMath.sqrt(std[i] / NUM_RUNS);

      System.out.printf(Locale.ROOT,
          "%-30s | %6.0f +- %-3.0f | %6.0f +- %-3.0f | %8.0f +- %-7.0f | %6.0f +- %-3.0f | %10.1f%n",
          cfg.name(),
          mean[0], std[0],
          mean[1], std[1],
          mean[2], std[2],
          mean[3], std[3],
          meanSec);

      for (int r = 0; r < NUM_RUNS; r++) {
        csvRows.add(formatCsvRow(T, cfg, cal, r, perRunPerConfig[c][r], secPerRunPerConfig[c][r]));
      }
      csvRows.add(formatCsvRow(T, cfg, cal, -1, mean, meanSec));
    }

    writeCsv(T, csvRows);
  }

  private static String formatCsvRow(int T, BenchmarkConfig cfg, Calibration cal, int run,
      double[] metrics, double secPerRun) {
    return String.format(Locale.ROOT,
        "%d,%.4f,%s,%d,%d,%.6f,%.6f,%.6f,%.6f,%.6f,%.6e,%.6e,%.6f,%.6f,%.6e,%.6f,%.6f",
        T, cfg.alpha(), cfg.composition(), MU, run,
        metrics[0], metrics[1], metrics[2], metrics[3], secPerRun,
        cal.epsRound(), cal.deltaRound(), cal.sigmaKey(), cal.sigmaHist(),
        cal.beta(), cal.thresholdQuantile(), cal.tauAtLastStep());
  }

  private static void writeCsv(int T, List<String> rows) {
    String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss", Locale.ROOT).format(new Date());
    Path dir = Paths.get("target");
    try {
      Files.createDirectories(dir);
    } catch (IOException e) {
      dir = Paths.get(".");
    }
    Path csvPath = dir.resolve(String.format(Locale.ROOT, "benchmark_T%d_%s.csv", T, timestamp));
    boolean newFile = !Files.exists(csvPath);
    try (BufferedWriter w = Files.newBufferedWriter(csvPath, StandardCharsets.UTF_8,
        StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
      if (newFile) {
        w.write(CSV_HEADER);
        w.newLine();
      }
      for (String row : rows) {
        w.write(row);
        w.newLine();
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write CSV: " + csvPath, e);
    }
    System.out.printf(Locale.ROOT, "%nWrote %d rows to %s%n", rows.size(), csvPath.toAbsolutePath());
  }

  /**
   * Generates the full batched dataset for a single Monte Carlo seed.
   * <p>
   * The dataset is reused across all benchmark configurations on the same seed
   * in order to compare the utility impct of different calibrations on the same
   * underlying data.
   *
   * @param T    number of micro-batches (time steps) to stream through the
   *             mechanism
   * @param seed random seed for reproducibility
   * @return array of length T, where each element is a list of (userId, key)
   *         pairs representing the contributions assigned to that batch.
   */
  @SuppressWarnings("unchecked")
  private List<int[]>[] generateBatches(int T, long seed) {
    Random rng = new Random(seed);
    ZipfMandelbrot userDist = new ZipfMandelbrot(USER_DIST_N, USER_DIST_Q, USER_DIST_S, rng);
    ZipfMandelbrot keyDist = new ZipfMandelbrot(NUM_KEYS, KEY_DIST_Q, KEY_DIST_S, rng);

    List<int[]>[] batches = new List[T];
    for (int t = 0; t < T; t++)
      batches[t] = new ArrayList<>();

    // for each user, sample a budget and emit that many contributions with random
    // keys into random batches.
    for (int u = 0; u < NUM_USERS; u++) {
      long budget = Math.min((long) userDist.sample(), C);
      for (long c = 0; c < budget; c++) {
        int batchIdx = rng.nextInt(T); // randomly assign contribution to a batch
        int keyRank = keyDist.sample(); // sample key
        // store (userId, key)
        batches[batchIdx].add(new int[] { u, keyRank });
      }
    }
    return batches;
  }

  /**
   * One Monte Carlo trial under a given calibration: streams all T batches
   * through a fresh mechanism and returns the final-step (l0, l_inf, l1, l2)
   * metrics versus the ground-truth histogram (no DP).
   *
   * @param batches the full dataset, pre-batched by time step, as generated by
   *                {@link #generateBatches}
   * @param T       number of micro-batches (time steps) to stream through the
   *                mechanism
   * @param cfg     the configuration (composition theorem + alpha) used to
   *                construct the mechanism
   * @param mu      the threshold offset to use in the mechanism
   * @return array of (l0, l_inf, l1, l2) metrics comparing the final released
   *         histogram
   */
  private double[] runOnce(List<int[]>[] batches, int T, BenchmarkConfig cfg, long mu) {
    // Construct the mechanism directly from the high-level DP budget using the
    // chosen composition theorem; the calibration pipeline runs internally.
    StreamingDPMechanism mechanism = new StreamingDPMechanism(
        cfg.composition(), EPS_K, DELTA_K, EPS_H, DELTA_H, T, mu, C, L, cfg.alpha());

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
   * the ground-truth histogram keys.
   * <p>
   * Missing keys on either side are treated as 0.
   *
   * @param dp    the released histogram from the mechanism, as a map from key to
   *              noisy coun
   * @param truth the ground-truth histogram, as a map from
   *              key to true count *
   * @return array of (l0, l_inf, l1, l2) metrics comparing the DP histogram to
   *         the ground truth
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
   * $\sigma_h$, and threshold $\tau$ used by {@link StreamingDPMechanism}.
   * <p>
   * In all three composition modes, $\delta_k^{(r)}$ is split via the
   * privacy-tight pre-allocation: $(1-\alpha)\delta_k^{(r)}$ for the
   * Gaussian-noise share and $\alpha\delta_k^{(r)}$ for the threshold-failure
   * cost $(e^{\varepsilon_k^{(r)}}+1)\beta$ from Algorithm 1. The modes differ
   * only in how the per-round $\rho$ used to calibrate $\sigma_k$ is obtained:
   * DWORK/KOV go via per-round $(\varepsilon, \delta)$-DP composition and
   * convert to $\rho$ at the last step, while ZCDP_LINEAR converts to
   * $\rho_k$-zCDP once and splits linearly across rounds.
   */
  private Calibration calibrate(BenchmarkConfig cfg, int T) {
    // Delegate the full Section 4.4 calibration pipeline to the shared utility:
    // the per-round key-selection budget for the chosen composition theorem,
    // sigma_k, beta, the threshold quantile, and sigma_h are all derived there.
    DPUtil.DpCalibration dp = DPUtil.calibrate(
        cfg.composition(), EPS_K, DELTA_K, EPS_H, DELTA_H, C, T, L, cfg.alpha());

    // Diagnostic-only fields used by the CSV/log output, recovered from the
    // calibrated sigma_k and threshold quantile.
    double kappa = FastMath.ceil(FastMath.log(T) / FastMath.log(2));
    double honakerNodeVariance = dp.sigmaKey() * dp.sigmaKey() / (2.0 * (1.0 - FastMath.pow(2.0, -kappa)));
    double tauAtLastStep = FastMath.sqrt(kappa * honakerNodeVariance) * dp.thresholdQuantile();

    return new Calibration(
        dp.epsilonKeyRound(), dp.deltaKeyRound(),
        dp.rhoKey(), dp.sigmaKey(),
        dp.rhoHist(), dp.sigmaHist(),
        dp.beta(), dp.thresholdQuantile(),
        tauAtLastStep, kappa);
  }

  private void logCalibration(BenchmarkConfig cfg, Calibration cal) {
    System.out.printf(Locale.ROOT,
        "calibration [%s]:%n"
            + "  eps_k_round=%.4f  delta_k_round=%.3e   composition=%s  alpha=%.2f%n"
            + "  rho_k=%.4e   sigma_k=%.3f%n"
            + "  rho_h=%.4e   sigma_h=%.3f  (L1=%.1f)%n"
            + "  beta=%.3e   Phi^-1(1-beta)=%.3f%n"
            + "  kappa=ceil(log2 T)=%.0f%n"
            + "  tau_T-1 ~= %.2f   release threshold mu+tau = %.2f   (mu=%d)%n",
        cfg.name(),
        cal.epsRound(), cal.deltaRound(), cfg.composition(), cfg.alpha(),
        cal.rhoK(), cal.sigmaKey(),
        cal.rhoH(), cal.sigmaHist(), DPUtil.l1Sensitivity(C, L),
        cal.beta(), cal.thresholdQuantile(),
        cal.kappa(),
        cal.tauAtLastStep(), MU + cal.tauAtLastStep(), MU);
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
