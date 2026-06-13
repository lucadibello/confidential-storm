package ch.usi.inf.examples.synthetic_baseline.dp;

import java.util.*;

import org.apache.commons.math3.util.FastMath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements Differentially Private Stream Aggregation mechanism (DP-SQLP) from the paper:
 * "Differentially Private Stream Processing at Scale".
 * <p>
 * This DP mechanism includes the following components as described in the paper:
 * 1. Streaming Key Selection (Algorithm 1): Identifying keys with sufficient unique user contributions.
 * 2. Hierarchical Perturbation (Algorithm 2): Releasing noisy aggregate counts for selected keys.
 * 3. Empty Key Release Prediction (Algorithm 3): Predicting when keys might be released due to noise.
 * 4. Use of Binary Aggregation Trees (Algorithm 4) for efficient DP aggregation over time.
 */
public class StreamingDPMechanism {
    private static final Logger log = LoggerFactory.getLogger(StreamingDPMechanism.class);

    /**
     * Forest of binary aggregation trees for key selection.
     */
    private final Map<String, BinaryAggregationTree> keySelectionForest = new HashMap<>();

    /**
     * Forest of binary aggregation trees for histogram release.
     * <p>
     * For each selected key, we maintain a separate binary aggregation tree to track
     * the sum of contributions over time with added noise.
     */
    private final Map<String, BinaryAggregationTree> histogramForest = new HashMap<>();
    
    /**
     * Map that stores the latest released noisy sum for each key.
     * <p>
     * Values are updated when a key is released and are carried forward across triggering times,
     * ensuring that every snapshot emits the full histogram state (including unchanged keys).
     */
    private final Map<String, Double> currentSums = new HashMap<>();

    /**
     * Map that links each key to its predicted release time step.
     * <p>
     * This is used in the Empty Key Release Prediction (Algorithm 3) to determine when a key is expected to be
     * released due to noise alone.
     */
    private final Map<String, Integer> predictedReleaseTimes = new HashMap<>();

    /**
     * Map that links each key to the set of unique user IDs already counted in the current
     * key-selection round (since the most recent release of that key).
     * <p>
     * This keeps per-round key-selection sensitivity at 1 per user-key pair.
     */
    private final Map<String, Set<String>> observedUsersForKeySelection = new HashMap<>();

    /**
     * Buffer for hierarchical perturbation (Algorithm 2).
     * Stores the aggregated value (Delta V) accumulated since the last release (or start) for each key.
     */
    private final Map<String, Double> unreleasedHistogramBuffer = new HashMap<>();

    /**
     * Buffers for current time window contributions (needed for key selection).
     * Stores the raw count of contributions for each key in the current time step.
     * <p>
     * <b>Double-buffer design:</b> {@code addContribution()} writes into the staging
     * maps. {@code snapshot()} atomically swaps them out (under {@link #bufferLock})
     * and drains them without holding any lock that blocks the executor thread.
     */
    private Map<String, Double> stagingWindowCounts = new HashMap<>();

    /**
     * Links each key to the set of unique user IDs that have contributed to it in the current time step {@link #timeStep}.
     * Written by {@code addContribution()}, swapped and drained by {@code snapshot()}.
     */
    private Map<String, Set<String>> stagingWindowUniqueUsers = new HashMap<>();

    /**
     * Lightweight lock that protects the reference swap between the staging
     * buffers and the snapshot drain buffers.
     */
    private final Object bufferLock = new Object();

    /**
     * The noise scale parameters for the Gaussian noise added during key selection (Algorithm 1).
     */
    private final double sigmaKey;

    /**
     * The noise scale parameters for the Gaussian noise added during histogram release (Algorithm 2).
     * Calibrated for L1 sensitivity = C * L_m.
     */
    private final double sigmaHist;

    /**
     * The maximum number of time steps (triggering times) to process in the stream.
     */
    private final int maxTimeSteps;

    /**
     * The minimum number of unique users (base threshold) required for a key to be considered for release.
     * <p>
     * A key is selected for release if the count of unique users (with DP noise) satisfies:
     * noisy_unique_users >= mu + tau_tr_i
     * where tau_tr_i is a time-dependent threshold.
     */
    private final long mu;

    /**
     * Quantile Phi^{-1}(1 - beta) of the standard normal distribution used to compute the
     * time-dependent key-selection threshold.
     */
    private final double thresholdQuantile;

    /**
     * The current time step in the stream processing (tr_j).
     */
    private int timeStep = 0;

    /**
     * Initializes the streaming DP mechanism with the provided noise scales and parameters.
     *
     * @param sigmaKey                Noise scale for key selection (Algorithm 1), based on sensitivity 1.
     *                                Must be calibrated for ((eps_k_round, (1-alpha) * delta_k_round))-DP
     *                                so that the threshold-failure term (e^eps + 1) * beta consumes the
     *                                remaining alpha * delta_k_round share without inflating the budget.
     * @param sigmaHist               Noise scale for histogram release (Algorithm 2), calibrated for L1 sensitivity C*L_m.
     * @param thresholdQuantile       Phi^{-1}(1 - beta) of the standard normal, with beta derived from the
     *                                per-round budget as beta = alpha * delta_k_round / (e^eps + 1).
     * @param maxTimeSteps            Maximum number of triggering times (T) to process in the stream.
     * @param mu                      Base threshold for key selection (minimum unique users).
     * @param maxContributionsPerUser Maximum contributions per user (C) for bounding sensitivity.
     */
    public StreamingDPMechanism(double sigmaKey,
                                double sigmaHist,
                                double thresholdQuantile,
                                int maxTimeSteps,
                                long mu,
                                long maxContributionsPerUser) {
        // ensure that mu is non-negative -> mu >= 0
        if (mu < 0) {
            throw new IllegalArgumentException("mu must be non-negative");
        }
        if (maxContributionsPerUser <= 0) {
            throw new IllegalArgumentException("maxContributionsPerUser must be positive");
        }
        if (!Double.isFinite(thresholdQuantile) || thresholdQuantile <= 0) {
            throw new IllegalArgumentException("thresholdQuantile must be a positive finite value");
        }
        this.sigmaKey = sigmaKey;
        this.sigmaHist = sigmaHist;
        this.thresholdQuantile = thresholdQuantile;
        this.maxTimeSteps = maxTimeSteps;
        this.mu = mu;
    }

    /**
     * Initializes the streaming DP mechanism directly from a high-level DP
     * configuration, deriving the noise scales and threshold quantile internally
     * via the chosen {@link CompositionMode} composition theorem.
     * <p>
     * This is the recommended entry point: callers supply the privacy budget and
     * sensitivity parameters and pick a composition theorem, and the mechanism
     * runs the full DP-SQLP Section 4.4 calibration pipeline (see
     * {@link DPUtil#calibrate}) without the caller having to remember the exact
     * steps.
     *
     * @param composition              the C-fold composition theorem used to
     *                                 derive the per-round key-selection budget
     * @param epsilonK                 total epsilon budget for key selection
     * @param deltaK                   total delta budget for key selection
     * @param epsilonH                 total epsilon budget for histogram release
     * @param deltaH                   total delta budget for histogram release
     * @param maxTimeSteps             maximum number of triggering times (T)
     * @param mu                       base threshold for key selection
     * @param maxContributionsPerUser  maximum contributions per user (C)
     * @param perRecordClamp           per-record clamp value (L_m)
     * @param thresholdFailureFraction fraction alpha of the per-round
     *                                 key-selection delta reserved for the
     *                                 threshold-failure cost, in (0, 1)
     */
    public StreamingDPMechanism(CompositionMode composition,
                                double epsilonK,
                                double deltaK,
                                double epsilonH,
                                double deltaH,
                                int maxTimeSteps,
                                long mu,
                                long maxContributionsPerUser,
                                double perRecordClamp,
                                double thresholdFailureFraction) {
        this(DPUtil.calibrate(composition, epsilonK, deltaK, epsilonH, deltaH,
                maxContributionsPerUser, maxTimeSteps, perRecordClamp, thresholdFailureFraction),
                maxTimeSteps, mu, maxContributionsPerUser);
    }

    /**
     * Initializes the streaming DP mechanism from a high-level DP configuration
     * using the default {@link CompositionMode#ZCDP_LINEAR} composition, which is
     * the tightest of the three theorems and therefore the recommended default.
     * <p>
     * Equivalent to the {@link #StreamingDPMechanism(CompositionMode, double,
     * double, double, double, int, long, long, double, double) composition-aware
     * constructor} with {@link CompositionMode#ZCDP_LINEAR}.
     *
     * @param epsilonK                 total epsilon budget for key selection
     * @param deltaK                   total delta budget for key selection
     * @param epsilonH                 total epsilon budget for histogram release
     * @param deltaH                   total delta budget for histogram release
     * @param maxTimeSteps             maximum number of triggering times (T)
     * @param mu                       base threshold for key selection
     * @param maxContributionsPerUser  maximum contributions per user (C)
     * @param perRecordClamp           per-record clamp value (L_m)
     * @param thresholdFailureFraction fraction alpha of the per-round
     *                                 key-selection delta reserved for the
     *                                 threshold-failure cost, in (0, 1)
     */
    public StreamingDPMechanism(double epsilonK,
                                double deltaK,
                                double epsilonH,
                                double deltaH,
                                int maxTimeSteps,
                                long mu,
                                long maxContributionsPerUser,
                                double perRecordClamp,
                                double thresholdFailureFraction) {
        this(CompositionMode.ZCDP_LINEAR, epsilonK, deltaK, epsilonH, deltaH,
                maxTimeSteps, mu, maxContributionsPerUser, perRecordClamp, thresholdFailureFraction);
    }

    /**
     * Delegating constructor from a pre-computed {@link DPUtil.DpCalibration},
     * unpacking the derived noise scales and threshold quantile.
     */
    private StreamingDPMechanism(DPUtil.DpCalibration calibration,
                                 int maxTimeSteps,
                                 long mu,
                                 long maxContributionsPerUser) {
        this(calibration.sigmaKey(), calibration.sigmaHist(), calibration.thresholdQuantile(),
                maxTimeSteps, mu, maxContributionsPerUser);
    }

    /**
     * Records a contribution from a user for a specific key in the current time window.
     * <p>
     * Assumes contributions have already been bounded by the upstream user-contribution-bounding
     * stage so that each {@code userId} appears in at most {@code C} records across the stream
     * (per Section 3.2 of the DP-SQLP paper). This method does NOT enforce the C bound internally.
     * <p>
     * This method is thread-safe and can be called concurrently by multiple executor threads.
     *
     * @param key    The aggregation key
     * @param clamped_count  The clamped contribution value
     * @param userId The user ID contributing this value
     */
    public void addContribution(String userId, String key, double clamped_count) {
        synchronized (bufferLock) {
            // accumulate contribution for this key in the current window
            stagingWindowCounts.merge(key, clamped_count, Double::sum);
            // record contribution from this user to this key in the current window
            stagingWindowUniqueUsers.computeIfAbsent(key, k -> new HashSet<>()).add(userId);
        }
    }

    /**
     * Proceeds by one time step (triggering time), processing all contributions in the current window.
     * <p>
     * This method performs the following steps:
     * 1. Identifies all keys that need processing (current micro-batch + predicted releases).
     * 2. Executes Algorithm 1 (Streaming Private Key Selection) for new or pending keys.
     * 3. Executes Algorithm 3 (Empty Key Release Prediction) for keys that were not selected.
     * 4. Executes Algorithm 2 (Hierarchical Perturbation) for all selected keys to update their noisy sums.
     * 5. Increments the time step and returns the latest differentially private histogram.
     *
     * @return A map representing the noisy histogram, sorted by counts in descending order.
     */
    public Map<String, Long> snapshot() {
        log.debug("[DP-MECHANISM] snapshot() START - timeStep={}, maxTimeSteps={}", timeStep, maxTimeSteps);

        /* 
         * Atomically swap the staging buffers with fresh empty maps to ensure
         * thread-safety for addContribution() without blocking executor threads.
         */
        final Map<String, Double> currentWindowCounts;
        final Map<String, Set<String>> currentWindowUniqueUsers;
        synchronized (bufferLock) {
            currentWindowCounts = this.stagingWindowCounts;
            currentWindowUniqueUsers = this.stagingWindowUniqueUsers;
            this.stagingWindowCounts = new HashMap<>();
            this.stagingWindowUniqueUsers = new HashMap<>();
        }

        // If max time steps are exceeded, return the final histogram
        if (timeStep >= maxTimeSteps) {
            log.debug("[DP-MECHANISM] timeStep >= maxTimeSteps, returning final histogram");
            // Free all accumulated per-key state that will never be used again
            trimExpiredState();
            return produceHistogram();
        }

        // 1. Identify keys to process (current window active keys + predicted releases)
        Set<String> keysToProcess = new HashSet<>(currentWindowCounts.keySet());
        log.debug("[DP-MECHANISM] Keys with contributions this window: {}", keysToProcess.size());

        // Load keys with predicted release time equal to current time step (Algo 3 Case 2)
        Iterator<Map.Entry<String, Integer>> predictionIt = predictedReleaseTimes.entrySet().iterator();
        while (predictionIt.hasNext()) {
            Map.Entry<String, Integer> entry = predictionIt.next();
            if (entry.getValue() == timeStep) {
                keysToProcess.add(entry.getKey());
                predictionIt.remove(); // remove prediction as we are processing it now
            }
        }

        log.debug("[DP-MECHANISM] Keys with contributions this window: {}, predicted releases: {}",
                    currentWindowCounts.size(), predictedReleaseTimes.size());

        // 2. Process each key
        for (String key : keysToProcess) {
            boolean keyAppearedThisStep = currentWindowCounts.containsKey(key);

            // Get inputs for this window
            double countInput = currentWindowCounts.getOrDefault(key, 0.0);

            // Accumulate contributions since last release
            unreleasedHistogramBuffer.merge(key, countInput, Double::sum);

            // --- Key Selection Logic (Algo 1 & 3) ---

            // Discard prior prediction if key appears before predicted time (Algo 3 Case 1)
            if (keyAppearedThisStep && predictedReleaseTimes.containsKey(key)) {
                int predictedTime = predictedReleaseTimes.get(key);
                if (predictedTime > timeStep) {
                    // Key appeared before prediction - discard stale prediction
                    predictedReleaseTimes.remove(key);
                }
            }

            // Initialize or reuse key-selection tree (Algo 1 Step 5)
            BinaryAggregationTree t_key = keySelectionForest.get(key);
            if (t_key == null) {
                t_key = new BinaryAggregationTree(maxTimeSteps, sigmaKey);
                keySelectionForest.put(key, t_key);
                // Reset user tracking for the new round
                observedUsersForKeySelection.remove(key);
                log.debug("[DP-MECHANISM] Key {} initialized with new key-selection tree", key);
            }

            // Add user count delta to the tree (Algo 1 Step 7)
            Set<String> observedUsers = observedUsersForKeySelection.computeIfAbsent(key, k -> new HashSet<>());
            Set<String> windowUsers = currentWindowUniqueUsers.getOrDefault(key, Collections.emptySet());

            // Count new unique users in current round for this key
            int newUniqueUsers = 0;
            for (String userId : windowUsers) {
                if (observedUsers.add(userId)) {
                    newUniqueUsers++;
                }
            }
            t_key.addToTree(timeStep, newUniqueUsers);

            // Compute noisy unique users (Algo 1 Step 8)
            double noisyUniqueUsers = t_key.getTotalSum(timeStep);

            // Check key selection condition (Algo 1 Step 9)
            double currentVariance = t_key.getHonakerVariance(timeStep);
            double tau_tr_i = computeTau(currentVariance);
            if (noisyUniqueUsers >= (double) mu + tau_tr_i) {
                // KEY SELECTED FOR RELEASE!

                // Release key and update histogram tree (Algo 2)
                updateHistogramTree(key);

                // Reset key selection state for the next round
                resetKeySelectionState(key);
                log.debug("[DP-MECHANISM] Key {} SELECTED (noisy users={} >= mu+tau={})",
                        key, noisyUniqueUsers, mu + tau_tr_i);
            } else {
                // NOT SELECTED

                // Predict future release time for unselected keys (Algo 3 Steps 2-3)
                if (keyAppearedThisStep) {
                    runEmptyKeyPrediction(key, t_key);
                }
            }
        }

        // Proceed to next time step
        timeStep++;

        // Produce and return the noisy histogram
        Map<String, Long> result = produceHistogram();
        log.debug("[DP-MECHANISM] snapshot() COMPLETE - returning {} keys", result.size());
        return result;
    }

    /**
     * Updates the histogram aggregation tree for a given key by adding the accumulated Delta V since the last release.
     * Delta V represents the sum of contributions accumulated for this key since it was last released.
     *
     * @param key The key for which to update the histogram tree
     */
    private void updateHistogramTree(String key) {
        // Get or create histogram tree, catching up if new
        BinaryAggregationTree histTree = histogramForest.get(key);
        if (histTree == null) {
            // BinaryAggregationTree pre-seeds every node with noise at construction
            histTree = new BinaryAggregationTree(maxTimeSteps, sigmaHist);
            histogramForest.put(key, histTree);
            log.debug("[DP-MECHANISM] Created new histogram tree for key {} at timeStep {}", key, timeStep);
        }

        // Get aggregated value since last release (Algo 2 Step 7)
        double deltaV = unreleasedHistogramBuffer.getOrDefault(key, 0.0);

        // Add delta to the histogram tree (Algo 2 Step 8)
        histTree.addToTree(timeStep, deltaV);

        // Compute noisy sum (Algo 2 Step 9)
        double noisySum = histTree.getTotalSum(timeStep);
        currentSums.put(key, noisySum); // Store result for release

        // Clear unreleased buffer
        unreleasedHistogramBuffer.remove(key);
    }

    /**
     * Algorithm 3: Empty Key Release Prediction
     * Predicts if a key (not selected at current time) will be selected in future due to noise alone.
     *
     * @param key The key to run prediction for (must be in S_tr_j but NOT selected)
     * @param keyTree The binary aggregation tree for this key's unique user counts
     */
    private void runEmptyKeyPrediction(String key, BinaryAggregationTree keyTree) {
        // Skip if prediction already exists for this key
        if (predictedReleaseTimes.containsKey(key)) {
            log.debug("[PREDICTION] Key {} already has prediction, skipping", key);
            return;
        }

        int futureSteps = maxTimeSteps - timeStep - 1;
        log.debug("[PREDICTION] Starting prediction for key {} ({} future steps to check)", key, futureSteps);

        // Iterate through future steps to predict release (Algo 3 Step 4)
        int iterationCount = 0;
        for (int tr_p = timeStep + 1; tr_p < maxTimeSteps; tr_p++) {
            iterationCount++;
            // Assume no new contributions in future steps (Algo 3 Step 5)

            // Perform streaming private key selection at future step (Algo 3 Step 6)
            double predictedNoisyCount = keyTree.getTotalSum(tr_p);
            double futureVariance = keyTree.getHonakerVariance(tr_p);
            double futureTau = computeTau(futureVariance);

            // Record predicted release step if selected (Algo 3 Step 7)
            if (predictedNoisyCount >= (double) mu + futureTau) {
                // Predicted selection due to noise
                predictedReleaseTimes.put(key, tr_p);

                log.debug("[PREDICTION] Key {} will be released at future step tr_p={} (checked {} iterations)",
                        key, tr_p, iterationCount);
                break; // Found the earliest release time
            }
        }
    }

    /**
     * Computes the tau value based on the Gaussian distribution.
     * <p>
     * Uses the standard Gaussian cumulative distribution.
     *
     * @param lambda_square The total Honaker variance at time step i.
     * @return The computed tau value.
     */
    private double computeTau(double lambda_square) {
        return FastMath.sqrt(lambda_square) * thresholdQuantile;
    }

    /**
     * Produces a differentially private histogram sorted by counts in descending order.
     * <p>
     * Rounds negative counts to zero to avoid showing negative values.
     *
     * @return A map representing the noisy histogram with keys and their corresponding counts.
     */
    private Map<String, Long> produceHistogram() {
        Map<String, Long> sortedHistogram = new LinkedHashMap<>();

        // Sort by value descending
        currentSums.entrySet().stream()
                .sorted((a, b) -> Double.compare(b.getValue(), a.getValue()))
                .forEach(entry -> {
                    long rounded = FastMath.round(entry.getValue());
                    // Clamp to zero
                    sortedHistogram.put(entry.getKey(), FastMath.max(0L, rounded));
                });

        // Return sorted histogram
        return sortedHistogram;
    }

    /**
     * Frees all accumulated per-key state once the mechanism has exhausted its time budget
     * (timeStep >= maxTimeSteps). At that point no further DP-valid releases can occur, so
     * all forests, selection state, prediction maps, and the unreleased buffer are dead weight.
     */
    private void trimExpiredState() {
        keySelectionForest.clear();
        histogramForest.clear();
        observedUsersForKeySelection.clear();
        predictedReleaseTimes.clear();
        unreleasedHistogramBuffer.clear();
        // Acquire lock to safely clear staging buffers
        synchronized (bufferLock) {
            stagingWindowCounts.clear();
            stagingWindowUniqueUsers.clear();
        }
        log.debug("[DP-MECHANISM] trimExpiredState: released all per-key state after maxTimeSteps={}", maxTimeSteps);
    }

    /**
     * Resets the key selection state for a given key after it has been selected.
     *
     * @param key The key to reset.
     */
    private void resetKeySelectionState(String key) {
        keySelectionForest.remove(key);
        observedUsersForKeySelection.remove(key);
        predictedReleaseTimes.remove(key);
    }
}
