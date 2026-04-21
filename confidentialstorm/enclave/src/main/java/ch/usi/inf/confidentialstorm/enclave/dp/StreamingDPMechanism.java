package ch.usi.inf.confidentialstorm.enclave.dp;

import java.util.*;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.util.FastMath;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLoggerFactory;

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
    private static final EnclaveLogger log = EnclaveLoggerFactory.getLogger(StreamingDPMechanism.class);

    // Aggregation trees for key selection and histogram (algorithm 4)

    /**
     * Forest of binary aggregation trees for key selection.
     * <p>
     * For each key detected during the key selection phase, we maintain a separate binary aggregation tree to track
     * the number of unique users contributing to that key over time.
     * <p>
     * This guarantees p-zCDP for each of the trees for each key.
     */
    private final Map<String, BinaryAggregationTree> keySelectionForest = new HashMap<>();

    /**
     * Forest of binary aggregation trees for histogram release.
     * <p>
     * For each selected key, we maintain a separate binary aggregation tree to track the sum of contributions
     * over time with added noise.
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
     * Lightweight lock that protects only the reference swap between the staging
     * buffers and the snapshot drain buffers. Held for O(1).
     * 
     * NOTE: held for just two reference, so it never blocks the executor thread for a meaningful duration.
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
     * Precomputed quantile Phi^{-1}(1 - beta) of the standard normal distribution,
     * where beta = 1e-5 (confidence level from Appendix D of the paper).
     * <p>
     * Used in {@link #computeTau} to avoid allocating a {@link NormalDistribution}
     * object on every call. Identity: N(0, sigma).inverseCDF(p) = sigma * N(0,1).inverseCDF(p).
     */
    private static final double PROBIT_1_MINUS_BETA =
            new NormalDistribution(0, 1).inverseCumulativeProbability(1.0 - 1e-5);

    /**
     * The current time step in the stream processing (tr_j).
     */
    private int timeStep = 0;

    /**
     * Initializes the streaming DP mechanism with the provided noise scales and parameters.
     *
     * @param sigmaKey                Noise scale for key selection (Algorithm 1), based on sensitivity 1.
     * @param sigmaHist               Noise scale for histogram release (Algorithm 2), calibrated for L1 sensitivity C*L_m.
     * @param maxTimeSteps            Maximum number of triggering times (T) to process in the stream.
     * @param mu                      Base threshold for key selection (minimum unique users).
     * @param maxContributionsPerUser Maximum contributions per user (C) for bounding sensitivity.
     */
    public StreamingDPMechanism(double sigmaKey,
                                double sigmaHist,
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
        this.sigmaKey = sigmaKey;
        this.sigmaHist = sigmaHist;
        this.maxTimeSteps = maxTimeSteps;
        this.mu = mu;
    }

    /**
     * Records a contribution from a user for a specific key in the current time window.
     * Enforces per-user contribution bounding (C) as required by Section 3.2.
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
         * Swap-and-drain mechanism to atomically grab the staging buffers and replace them
         * with fresh empty maps. The lock is held for O(1) (two reference swaps).
         * NOTE: needed in order to make `addContribution()` thread-safe and avoid Apache Storm's task threads being blocked.
        */
        final Map<String, Double> currentWindowCounts;
        final Map<String, Set<String>> currentWindowUniqueUsers;
        synchronized (bufferLock) {
            currentWindowCounts = this.stagingWindowCounts;
            currentWindowUniqueUsers = this.stagingWindowUniqueUsers;
            this.stagingWindowCounts = new HashMap<>();
            this.stagingWindowUniqueUsers = new HashMap<>();
        }

        // if timeStep exceeds maxTimeSteps, return final histogram
        // NOTE: no further processing as we won't have DP guarantees in the next release
        if (timeStep >= maxTimeSteps) {
            log.info("[DP-MECHANISM] timeStep >= maxTimeSteps, returning final histogram");
            // Free all accumulated per-key state that will never be used again
            trimExpiredState();
            return produceHistogram();
        }

        // 1. Identify all keys that need processing in this step
        // a) Unique set of keys in this stream window D_tr_i (Step 4 of Algo 1)
        // b) Keys predicted to be released at this specific timeStep (Algo 3 Case 2)

        // Step 1 of Algo 3 - DeltaD_tr_j = D_tr_j - D_tr_(j-1) (data in current micro-batch)
        // NOTE: currentWindowCounts represents DeltaD_tr_j (keys with contributions this window)
        Set<String> keysToProcess = new HashSet<>(currentWindowCounts.keySet());
        log.info("[DP-MECHANISM] Keys with contributions this window: {}", keysToProcess.size());

        // Algo 3 Case 2 - Load keys with predicted release time = current time step
        // "DP-SQLP loaded system states for key k at predicted time tr_p"
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

            // Step 7 of Algo 2 - Aggregate contributions for this key since last release to compute DeltaV_key
            unreleasedHistogramBuffer.merge(key, countInput, Double::sum);

            // --- Key Selection Logic (Algo 1 & 3) ---

            // Algo 3 Case 1 - Key appears before predicted time (j < n < p)
            // "The prior prediction result is discarded"
            if (keyAppearedThisStep && predictedReleaseTimes.containsKey(key)) {
                int predictedTime = predictedReleaseTimes.get(key);
                if (predictedTime > timeStep) {
                    // Key appeared before prediction - discard stale prediction
                    predictedReleaseTimes.remove(key);
                }
            }

            // Algo 1 step 5 - initialize or reuse tree; prefer reuse even if key went silent
            BinaryAggregationTree t_key = keySelectionForest.get(key);
            if (t_key == null) {
                t_key = new BinaryAggregationTree(maxTimeSteps, sigmaKey);
                keySelectionForest.put(key, t_key);
                // Reset user tracking for this key since we're starting fresh round
                observedUsersForKeySelection.remove(key);
                log.debug("[DP-MECHANISM] Key {} initialized with new key-selection tree", key);
            }

            // Step 7 of Algo 1: Add count_key(D_tr_i) - count_key(D_tr_(i-1)) to the tree
            Set<String> observedUsers = observedUsersForKeySelection.computeIfAbsent(key, k -> new HashSet<>());
            Set<String> windowUsers = currentWindowUniqueUsers.getOrDefault(key, Collections.emptySet());

            // Count NEW unique users in current round for this key
            int newUniqueUsers = 0;
            for (String userId : windowUsers) {
                if (observedUsers.add(userId)) {
                    newUniqueUsers++; // this is count_key(D_tr_i) - count_key(D_tr_(i-1)) for current round
                }
            }

            // Add the delta to the key-selection tree
            t_key.addToTree(timeStep, newUniqueUsers);

            // Step 8 of Algo 1 - compute the noisy unique users for this key at time step i
            double noisyUniqueUsers = t_key.getTotalSum(timeStep);

            // Step 9 of Algo 1 - check key selection condition
            double currentVariance = t_key.getHonakerVariance(timeStep);
            double tau_tr_i = computeTau(currentVariance);
            if (noisyUniqueUsers >= (double) mu + tau_tr_i) {
                // KEY SELECTED FOR RELEASE!

                // Run Algorithm 2 with accumulated unreleased buffer value
                updateHistogramTree(key);

                // Restart key selection round after release (Section 4.4)
                resetKeySelectionState(key);
                log.debug("[DP-MECHANISM] Key {} SELECTED (noisy users={} >= mu+tau={})",
                        key, noisyUniqueUsers, mu + tau_tr_i);
            } else {
                // NOT SELECTED

                // Step 2-3 of Algo 3 - For keys in S_tr_j (current micro-batch) not selected, run prediction
                if (keyAppearedThisStep) {
                    runEmptyKeyPrediction(key, t_key);
                }
            }
        }

        // Proceed to next time step
        timeStep++;

        // NOTE: GC will take care of cleaning up old hashmaps

        // Produce and return the noisy histogram
        Map<String, Long> result = produceHistogram();
        log.info("[DP-MECHANISM] snapshot() COMPLETE - returning {} keys", result.size());
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
            // Create new tree
            histTree = new BinaryAggregationTree(maxTimeSteps, sigmaHist);

            // Execute Algorithm 4 for steps 0 to (timeStep-1) with zeros
            for (int t = 0; t < timeStep; t++) {
                histTree.addToTree(t, 0.0);
            }

            histogramForest.put(key, histTree);
            log.debug("[DP-MECHANISM] Created new histogram tree for key {} and caught up to timeStep {}", key, timeStep);
        }

        // Step 7 of Algo 2 - get the DeltaV_key (aggregated value since last release)
        double deltaV = unreleasedHistogramBuffer.getOrDefault(key, 0.0);

        // Step 8 of Algo 2 - add DeltaV_key to the histogram tree at current time step
        histTree.addToTree(timeStep, deltaV);

        // Step 9 of algo 2 - compute noisy sum for this key at current time step
        double noisySum = histTree.getTotalSum(timeStep);
        currentSums.put(key, noisySum); // NOTE: store result for histogram production

        // Clear the buffer since we've now incorporated it into the tree
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

        // Step 4 of Algo 3 - for tr_p from tr_j+1 (which is the next time step) to tr_|Trw| (the final time step)
        int iterationCount = 0;
        for (int tr_p = timeStep + 1; tr_p < maxTimeSteps; tr_p++) {
            iterationCount++;
            // Step 5 of Algo 3 - D_tr_p = D_tr_j -> we assume no new contributions for this key in future steps

            // Step 6 of Algo 3 - Perform streaming private key selection via Algorithm 1 at time tr_p
            double predictedNoisyCount = keyTree.getTotalSum(tr_p);
            double futureVariance = keyTree.getHonakerVariance(tr_p);
            double futureTau = computeTau(futureVariance);

            // Step 7 of Algo 3 - if key is selected, then write <key, tr_p> to state store and break
            if (predictedNoisyCount >= (double) mu + futureTau) {
                // Key predicted to be selected at future time tr_p due to noise
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
     * Uses the identity N(0, sigma).inverseCDF(p) = sigma * N(0,1).inverseCDF(p)
     * with precomputed {@link #PROBIT_1_MINUS_BETA} to avoid per-call allocation.
     * <p>
     * Refer to Appendix D of the "Differentially Private Stream Processing at Scale" paper.
     *
     * @param lambda_square The total Honaker variance at time step i.
     * @return The computed tau value.
     */
    private static double computeTau(double lambda_square) {
        return FastMath.sqrt(lambda_square) * PROBIT_1_MINUS_BETA;
    }

    /**
     * Produces a differentially private histogram sorted by counts in descending order.
     * <p>
     * This function rounds negative counts to zero to avoid negative counts in the released histogram.
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
                    // Clamp to zero to avoid negative counts in the released histogram
                    sortedHistogram.put(entry.getKey(), FastMath.max(0L, rounded));
                });

        // return sorted histogram
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
        // acquire lock to safely clear staging buffers
        synchronized (bufferLock) {
            stagingWindowCounts.clear();
            stagingWindowUniqueUsers.clear();
        }
        log.info("[DP-MECHANISM] trimExpiredState: released all per-key state after maxTimeSteps={}", maxTimeSteps);
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
