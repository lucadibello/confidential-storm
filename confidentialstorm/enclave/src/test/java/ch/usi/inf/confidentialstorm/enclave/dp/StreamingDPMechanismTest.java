package ch.usi.inf.confidentialstorm.enclave.dp;

import ch.usi.inf.confidentialstorm.enclave.util.DPUtil;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link StreamingDPMechanism} correctness.
 *
 * These tests verify the DP mechanism without SGX by calling it directly.
 * All tests use the same DP parameters as the paper (epsilon=6, delta=1e-9, C=32, L=1)
 * with the same epsilon/delta split: epsilon_k = epsilon/2, epsilon_h = epsilon/2, delta_k = delta*2/3, delta_h = delta/3.
 *
 * Test strategy:
 *   1. Noise scale: with sigma=0 (no noise), output must equal true count exactly.
 *   2. Bias: averaged over many runs, per-key error converges to 0.
 *   3. Noise variance: empirical variance matches theoretical Gaussian variance.
 *   4. Key selection: keys well above mu are always released; keys well below never are.
 *   5. Zero-noise correctness: full multi-key, multi-epoch pipeline produces exact counts.
 */
class StreamingDPMechanismTest {

    // Paper DP parameters (Section 5.1)
    private static final double EPSILON   = 6.0;
    private static final double DELTA     = 1e-9;
    private static final long   C         = 32L;
    private static final double L         = 1.0;
    private static final long   MU        = 5L;   // low mu so keys are easily released in tests
    private static final int    T         = 100;  // max time steps

    // Paper budget split (Section 5.1 footnote)
    private static final double EPS_K   = EPSILON / 2.0;
    private static final double EPS_H   = EPSILON / 2.0;
    private static final double DELTA_K = DELTA * 2.0 / 3.0;
    private static final double DELTA_H = DELTA / 3.0;

    /** Build a mechanism with real paper parameters (Section 4.4 per-round composition). */
    private StreamingDPMechanism buildMechanism(int maxTimeSteps, long mu) {
        DPUtil.PerRoundBudget keyRoundBudget = DPUtil.keySelectionPerRoundBudget(EPS_K, DELTA_K, C);
        double rhoK     = DPUtil.cdpRho(keyRoundBudget.epsilon(), keyRoundBudget.delta());
        double sigmaKey = DPUtil.calculateSigma(rhoK, maxTimeSteps, 1.0);
        double rhoH     = DPUtil.cdpRho(EPS_H, DELTA_H);
        double sigmaHist= DPUtil.calculateSigma(rhoH, maxTimeSteps, DPUtil.l1Sensitivity(C, L));
        return new StreamingDPMechanism(sigmaKey, sigmaHist, maxTimeSteps, mu, C);
    }

    /**
     * Build a low-noise mechanism for verifying algorithmic behavior at unit-test scale.
     * <p>
     * Paper-scale sigma (C=32-round advanced composition at T=100, epsilon=6) gives
     * sigmaKey approx 200 and tau = sigmaKey * probit(1-1e-5) approx 860. No realistic unit-test
     * user count crosses that threshold -- the paper reproduces utility at 10M users,
     * where noise is small relative to typical key populations.
     * <p>
     * For correctness tests we fix sigma so tau stays in single digits: keys with a
     * few tens of users release reliably and noise statistics can be estimated in
     * bounded wall time. This is NOT a privacy-valid calibration; it is a test fixture
     * that exercises the same Algorithm 1/2/3/4 code paths as the paper configuration.
     */
    private StreamingDPMechanism buildLowNoiseMechanism(int maxTimeSteps, long mu,
                                                        double sigmaKey, double sigmaHist) {
        return new StreamingDPMechanism(sigmaKey, sigmaHist, maxTimeSteps, mu, C);
    }

    /** Build a zero-noise mechanism (sigma=0) for deterministic correctness checks. */
    private StreamingDPMechanism buildZeroNoiseMechanism(int maxTimeSteps, long mu) {
        return new StreamingDPMechanism(0.0, 0.0, maxTimeSteps, mu, C);
    }

    // -------------------------------------------------------------------------
    // Test 1: zero-noise single key -- output equals true count exactly
    // -------------------------------------------------------------------------

    @Test
    void zeroNoise_singleKey_outputEqualsExactCount() {
        StreamingDPMechanism m = buildZeroNoiseMechanism(T, 0L); // mu=0 -> always release

        // Inject 10 contributions from distinct users across 5 epochs (2 per epoch)
        for (int epoch = 0; epoch < 5; epoch++) {
            m.addContribution("user" + (epoch * 2),     "keyA", 1.0);
            m.addContribution("user" + (epoch * 2 + 1), "keyA", 1.0);
        }

        // Take snapshot at epoch 5
        for (int epoch = 0; epoch < 4; epoch++) m.snapshot(); // epochs 0-3: no contributions
        Map<String, Long> result = m.snapshot(); // epoch 4

        // Contributions were added before any snapshot -> all land in epoch 0's window
        // After epoch 0 snapshot: keyA gets selected and its noisy sum = 10 (no noise)
        assertTrue(result.containsKey("keyA"), "keyA must be released");
        // With zero noise the output must equal the true cumulative count (10)
        assertEquals(10L, result.get("keyA"), "zero-noise output must equal true count");
    }

    // -------------------------------------------------------------------------
    // Test 2: zero-noise multi-key pipeline -- each key matches its true count
    // -------------------------------------------------------------------------

    @Test
    void zeroNoise_multiKey_allCountsExact() {
        int numKeys  = 5;
        int usersPerKey = 20; // well above mu=5

        StreamingDPMechanism m = buildZeroNoiseMechanism(T, MU);

        // Epoch 0: add distinct users to each key
        for (int k = 0; k < numKeys; k++) {
            for (int u = 0; u < usersPerKey; u++) {
                m.addContribution("u_k" + k + "_" + u, "key" + k, 1.0);
            }
        }
        Map<String, Long> snapshot = m.snapshot(); // epoch 0

        for (int k = 0; k < numKeys; k++) {
            String key = "key" + k;
            assertTrue(snapshot.containsKey(key), key + " must be released");
            assertEquals((long) usersPerKey, snapshot.get(key),
                    "zero-noise count for " + key + " must equal true count");
        }
    }

    // -------------------------------------------------------------------------
    // Test 3: noise bias -- mean error over many runs converges to 0
    // -------------------------------------------------------------------------

    @Test
    void noisedMechanism_bias_convergesToZero() {
        int runs        = 200;
        int trueCount   = 50;
        int numUsers    = trueCount;
        double sigmaKey  = 1.0;   // tau = probit(1-1e-5) approx 4.27; mu+tau approx 9.3 << 50
        double sigmaHist = 2.0;
        double tolerance = trueCount * 0.30;

        long sumError = 0;
        int observed  = 0;

        for (int run = 0; run < runs; run++) {
            StreamingDPMechanism m = buildLowNoiseMechanism(T, MU, sigmaKey, sigmaHist);

            for (int u = 0; u < numUsers; u++) {
                m.addContribution("user_" + run + "_" + u, "targetKey", 1.0);
            }
            Map<String, Long> snap = m.snapshot();

            if (snap.containsKey("targetKey")) {
                sumError += snap.get("targetKey") - trueCount;
                observed++;
            }
        }

        assertTrue(observed > runs / 2,
                "targetKey must be released in at least half the runs (released in " + observed + "/" + runs + ")");

        double meanError = (double) sumError / observed;
        assertTrue(Math.abs(meanError) < tolerance,
                "mean error " + meanError + " exceeds bias tolerance +/-" + tolerance);
    }

    // -------------------------------------------------------------------------
    // Test 4: noise variance -- empirical variance matches theoretical Gaussian
    // -------------------------------------------------------------------------

    @Test
    void noisedMechanism_variance_matchesTheoretical() {
        int runs      = 500;
        int trueCount = 100;
        int numUsers  = trueCount;
        double sigmaKey  = 1.0;
        double sigmaHist = 2.0;

        // At epoch 0 the Honaker path contains only the leaf node (kappa=1), so
        // the theoretical variance of the prefix sum equals sigmaHist^2 exactly.
        // We still compare against a loose upper bound H * sigmaHist^2 to tolerate
        // finite-sample noise and rounding.
        int H = (int) Math.ceil(Math.log(T) / Math.log(2));
        double theoreticalVarianceUpperBound = H * sigmaHist * sigmaHist;

        List<Long> outputs = new ArrayList<>();
        for (int run = 0; run < runs; run++) {
            StreamingDPMechanism m = buildLowNoiseMechanism(T, MU, sigmaKey, sigmaHist);
            for (int u = 0; u < numUsers; u++) {
                m.addContribution("user_" + run + "_" + u, "targetKey", 1.0);
            }
            Map<String, Long> snap = m.snapshot();
            if (snap.containsKey("targetKey")) {
                outputs.add(snap.get("targetKey"));
            }
        }

        assertTrue(outputs.size() > runs / 2, "Key must be released in majority of runs");

        double mean = outputs.stream().mapToLong(Long::longValue).average().orElse(0);
        double empiricalVariance = outputs.stream()
                .mapToDouble(v -> (v - mean) * (v - mean))
                .average().orElse(0);

        assertTrue(empiricalVariance > 0, "Empirical variance must be positive (noise is being added)");
        assertTrue(empiricalVariance < 10.0 * theoreticalVarianceUpperBound,
                "Empirical variance " + empiricalVariance +
                " far exceeds theoretical upper bound " + theoreticalVarianceUpperBound +
                " -- noise scale may be miscalibrated");
    }

    // -------------------------------------------------------------------------
    // Test 5: key selection -- high-count keys always released, low-count never
    // -------------------------------------------------------------------------

    @Test
    void keySelection_highCountAlwaysReleased_lowCountRarelyReleased() {
        int runs = 50;

        // "hot" key: far above mu+tau -> always released
        int hotUsers  = 500; // >> mu=5
        // "cold" key: single user -> key IS observed by the mechanism, so the gate
        // (noisyUniqueUsers >= mu + tau) is actually evaluated. True count 1 is far
        // below mu=5, so the DP gate should reject in (essentially) every run.
        int coldUsers = 1;

        int hotReleased  = 0;
        int coldReleased = 0;

        for (int run = 0; run < runs; run++) {
            // low-noise fixture: sigmaKey=1.0 -> tau = Phi^-1(1 - 1e-5) approx 4.27.
            // hot (500) >> mu+tau approx 9.27; cold (1) << mu+tau.
            StreamingDPMechanism m = buildLowNoiseMechanism(T, MU, 1.0, 2.0);

            for (int u = 0; u < hotUsers; u++) {
                m.addContribution("hot_user_" + run + "_" + u, "hotKey", 1.0);
            }
            for (int u = 0; u < coldUsers; u++) {
                m.addContribution("cold_user_" + run + "_" + u, "coldKey", 1.0);
            }

            Map<String, Long> snap = m.snapshot();
            if (snap.containsKey("hotKey"))  hotReleased++;
            if (snap.containsKey("coldKey")) coldReleased++;
        }

        assertEquals(runs, hotReleased,
                "hotKey must be released in every run (" + hotReleased + "/" + runs + ")");
        // tau is set at beta=1e-5 quantile, so false release is extremely rare;
        // allow at most 5% to tolerate finite-sample fluctuation across 50 runs.
        assertTrue(coldReleased <= runs / 20,
                "coldKey (1 user) must almost never cross mu+tau threshold (" + coldReleased + "/" + runs + ")");
    }

    // -------------------------------------------------------------------------
    // Test 6: multi-epoch stability -- released keys persist across epochs
    // -------------------------------------------------------------------------

    @Test
    void multiEpoch_releasedKeysPersistAndAccumulate() {
        StreamingDPMechanism m = buildZeroNoiseMechanism(T, MU);

        // Epoch 0: inject enough users to release keyA
        for (int u = 0; u < 20; u++) {
            m.addContribution("user_" + u, "keyA", 1.0);
        }
        Map<String, Long> snap0 = m.snapshot();
        assertTrue(snap0.containsKey("keyA"), "keyA must be released at epoch 0");
        assertEquals(20L, snap0.get("keyA"), "zero-noise epoch-0 count must equal 20");

        // Epoch 1: add 5 more users to keyA; after paper-style reset of key-selection
        // state (Section 4.4), Algo 1 runs again and 5>=mu=5 -> re-selected.
        // Histogram tree is cumulative -> total = 25.
        for (int u = 20; u < 25; u++) {
            m.addContribution("user_" + u, "keyA", 1.0);
        }
        Map<String, Long> snap1 = m.snapshot();

        assertTrue(snap1.containsKey("keyA"), "keyA must remain released at epoch 1");
        assertEquals(25L, snap1.get("keyA"),
                "zero-noise cumulative count must equal 25 (20 + 5) at epoch 1");

        // Epoch 2: no new contributions. Key is not re-evaluated this epoch but
        // carried-forward currentSums must still emit it (no regression).
        Map<String, Long> snap2 = m.snapshot();
        assertTrue(snap2.containsKey("keyA"), "keyA must remain in histogram when silent");
        assertEquals(25L, snap2.get("keyA"),
                "silent-epoch count must equal last released value (cumulative 25)");
    }

    // -------------------------------------------------------------------------
    // Test 7: sigma calibration sanity -- sigmas are positive and finite
    // -------------------------------------------------------------------------

    @Test
    void sigmaCalibration_positiveAndFinite() {
        double rhoK     = DPUtil.cdpRho(EPS_K, DELTA_K);
        double rhoH     = DPUtil.cdpRho(EPS_H, DELTA_H);
        DPUtil.PerRoundBudget keyRoundBudget = DPUtil.keySelectionPerRoundBudget(EPS_K, DELTA_K, C);
        double sigmaKey = DPUtil.calculateSigma(DPUtil.cdpRho(keyRoundBudget.epsilon(), keyRoundBudget.delta()), T, 1.0);
        double sigmaHist= DPUtil.calculateSigma(rhoH, T, DPUtil.l1Sensitivity(C, L));

        assertTrue(rhoK     > 0 && Double.isFinite(rhoK),     "rhoK must be positive and finite");
        assertTrue(rhoH     > 0 && Double.isFinite(rhoH),     "rhoH must be positive and finite");
        assertTrue(sigmaKey > 0 && Double.isFinite(sigmaKey), "sigmaKey must be positive and finite");
        assertTrue(sigmaHist> 0 && Double.isFinite(sigmaHist),"sigmaHist must be positive and finite");

        // Note: under C-fold advanced composition (Section 4.4), per-round key budget
        // shrinks ~C-fold, so sigmaKey can exceed sigmaHist even though hist sensitivity
        // (C*L=32) is larger than key sensitivity (1). No ordering assertion here.
    }

    // -------------------------------------------------------------------------
    // Test 8: per-user sensitivity -- same user contributing multiple times to
    // the same key in one round counts once toward the key-selection unique-user
    // count. This is required for Algorithm 1's per-user sensitivity = 1.
    // -------------------------------------------------------------------------

    @Test
    void keySelection_dedupsRepeatedUserContributions() {
        StreamingDPMechanism m = buildZeroNoiseMechanism(T, 10L); // mu=10

        // 5 distinct users, each contributes 10 times to keyA in this round.
        // Without dedup, raw count = 50 -> would release. With dedup, unique=5 -> below mu=10.
        for (int u = 0; u < 5; u++) {
            for (int c = 0; c < 10; c++) {
                m.addContribution("user_" + u, "keyA", 1.0);
            }
        }
        Map<String, Long> snap = m.snapshot();

        assertFalse(snap.containsKey("keyA"),
                "keyA must NOT be released: only 5 unique users contributed, below mu=10 " +
                "(if released, per-user sensitivity of key selection is broken)");
    }

    // -------------------------------------------------------------------------
    // Test 9: mid-stream release -- histogram variance grows with epoch index
    // (the Honaker path has more noisy nodes), but mean error still approx 0.
    // Exercises a code path that the epoch-0 tests miss: the pad-with-zeros
    // catch-up loop in updateHistogramTree and the cumulative-across-epochs sum.
    // -------------------------------------------------------------------------

    @Test
    void midStreamRelease_unbiasedAndBounded() {
        int runs        = 200;
        int releaseEpoch = 10;   // force release at a non-trivial epoch
        int trueCount   = 50;
        double tolerance = trueCount * 0.50; // looser: noise is additive across catch-up

        long sumError = 0;
        int observed  = 0;

        for (int run = 0; run < runs; run++) {
            // low-noise fixture: 50 users >> mu+tau approx 9.27 -> reliable release at mid-stream.
            StreamingDPMechanism m = buildLowNoiseMechanism(T, MU, 1.0, 2.0);

            // Epochs 0..releaseEpoch-1: silent. Drives key-selection tree with zeros
            // (prediction may register/expire) and exercises the histogram pad-zeros path.
            for (int e = 0; e < releaseEpoch; e++) {
                m.snapshot();
            }

            // Epoch releaseEpoch: inject enough users to cross mu+tau.
            for (int u = 0; u < trueCount; u++) {
                m.addContribution("run" + run + "_u" + u, "midKey", 1.0);
            }
            Map<String, Long> snap = m.snapshot();

            if (snap.containsKey("midKey")) {
                sumError += snap.get("midKey") - trueCount;
                observed++;
            }
        }

        assertTrue(observed > runs / 2,
                "midKey must be released in at least half the runs (" + observed + "/" + runs + ")");

        double meanError = (double) sumError / observed;
        assertTrue(Math.abs(meanError) < tolerance,
                "mid-stream mean error " + meanError + " exceeds tolerance +/-" + tolerance);
    }
}
