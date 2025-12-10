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
     * FIXME: this statement needs to be double-checked!
     * This guarantees p-zCDP for each of the trees for each key.
     */
    private final Map<String, BinaryAggregationTree> keySelectionForest = new HashMap<>();


    private final Map<String, BinaryAggregationTree> histogramForest = new HashMap<>();
    
    /**
     * Set of keys that have been selected for release so far (from the beginning of the stream until now).
     */
    private final Set<String> selectedKeys = new HashSet<>();

    // sum of released noisy counts for each key (algorithm 2)
    private final Map<String, Double> currentSums = new HashMap<>();

    // keep track of predicted release times for keys (algorithm 3)

    /**
     * Map that links each key to its predicted release time step.
     * <p>
     * This is used in the Empty Key Release Prediction (Algorithm 3) to determine when a key is expected to be
     * released.
     */
    private final Map<String, Integer> predictedReleaseTimes = new HashMap<>();

    /**
     * Map that links each key to the set of unique user IDs that have contributed to it so far in the entire stream
     * (up to the current time step {@link #timeStep}).
     * <p>
     * This is essential for ensuring that the sensitivity of the key selection process remains 1, as required by
     * the DP mechanism.
     */
    private final Map<String, Set<String>> observedUsersForKeySelection = new HashMap<>();

    // Buffer for hierarchical perturbation (Algorithm 2)
    // Stores aggregated value (Delta V) since last release (or start)
    private final Map<String, Double> unreleasedHistogramBuffer = new HashMap<>();

    // Buffers for current time window contributions (needed for key selection)
    private final Map<String, Double> currentWindowCounts = new HashMap<>();

    /**
     * Links each key to the set of unique user IDs that have contributed to it in the current time step {@link #timeStep}.
     */
    private final Map<String, Set<String>> currentWindowUniqueUsers = new HashMap<>();

    /**
     * The noise scale parameters for the Gaussian noise added during key selection.
     */
    private final double sigmaKey;

    /**
     * The noise scale parameters for the Gaussian noise added during histogram release.
     */
    private final double sigmaHist;

    /**
     * The maximum number of time steps to process in the stream.
     */
    private final int maxTimeSteps;

    /**
     * The minimum number of unique users required for a key to be considered for release.
     * <p>
     * A key is selected for release if the count of unique users (with DP noise) contributing to that key satisfies:
     *  noisy_unique_users >= mu + tau_tr_i
     *      where
     *  tau_tr_i = time dependent threshold computed as the inverse CDF of the Gaussian distribution at confidence level {@link #BETA} - 1.
     * <p>
     * Refer to Appendix D of the "Differentially Private Stream Processing at Scale" paper for details.
     */
    private final long mu;

    /**
     * The confidence level for tau computation.
     * <p>
     */
    private final double BETA = 1e-5;

    /**
     * The current time step in the stream processing.
     */
    private int timeStep = 0;

    /**
     * Set of keys from the previous time step (S^(i-1)).
     * Used to determine which keys need new trees created (keys in S^(i) \ S^(i-1)).
     */
    private Set<String> s_i_prev = new HashSet<>();

    /**
     * Per-user contribution limiter enforcing C-bounded contributions.
     * Critical for maintaining L1 = C × Lm sensitivity assumption.
     */
    private final ContributionLimiter contributionLimiter = new ContributionLimiter();

    /**
     * Maximum contributions per user (C from Section 3.2).
     */
    private final long maxContributionsPerUser;

    /**
     * @param sigmaKey                Noise scale for key selection (based on sensitivity 1).
     * @param sigmaHist               Noise scale for histogram (based on sensitivity C*L).
     * @param maxTimeSteps            Maximum number of time steps to process.
     * @param mu                      Base threshold for key selection.
     * @param maxContributionsPerUser Maximum contributions per user (C) for bounding sensitivity.
     */
    public StreamingDPMechanism(double sigmaKey, double sigmaHist, int maxTimeSteps, long mu, long maxContributionsPerUser) {
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
        this.maxContributionsPerUser = maxContributionsPerUser;
    }

    /**
     * Records a contribution from a user for a specific key in the current time window.
     * Enforces per-user contribution bounding (C) as required by Section 3.2.
     *
     * @param key    The aggregation key
     * @param count  The contribution to add to the key
     * @param userId The user ID contributing this value
     * @return true if contribution was accepted, false if rejected due to exceeding C
     */
    public boolean addContribution(String key, double count, String userId) {
        // Enforce contribution bounding: each user can contribute at most C records
        // NOTE: needed to maintaining L_1 = C * L_m sensitivity assumption
        if (!contributionLimiter.allow(userId, maxContributionsPerUser)) {
            log.debug("[DP-MECHANISM] Rejected contribution from user {} (exceeded C={})",
                     userId, maxContributionsPerUser);
            return false;
        }

        // accumulate contribution for this key in the current window
        currentWindowCounts.merge(key, count, Double::sum);
        // record contribution from this user to this key in the current window
        currentWindowUniqueUsers.computeIfAbsent(key, k -> new HashSet<>()).add(userId);
        return true;
    }

    /**
     * Proceed by one time step processing all contributions in this window,
     * running the prediction algorithm (algorithm 3), and updating the DP trees.
     *
     * @return The noisy histogram.
     */
    public Map<String, Long> snapshot() {
        log.debug("[DP-MECHANISM] snapshot() START - timeStep={}, maxTimeSteps={}", timeStep, maxTimeSteps);

        // if timeStep exceeds maxTimeSteps, return final histogram
        // NOTE: no further processing as we won't have DP guarantees in the next release
        if (timeStep >= maxTimeSteps) {
            log.info("[DP-MECHANISM] timeStep >= maxTimeSteps, returning final histogram");
            return produceHistogram();
        }

        // 1. Identify all keys that need processing in this step
        // a) Unique set of keys in this stream window D_tr_i (Step 4 of Algo 1)
        // b) Keys predicted to be released at this specific timeStep (Algo 3 Case 2)

        // Step 1 of Algo 3 - DeltaD_tr_j = D_tr_j − D_tr_(j-1) (data in current micro-batch)
        // NOTE: currentWindowCounts represents DeltaD_tr_j (keys with contributions this window)
        Set<String> s_i = new HashSet<>(currentWindowCounts.keySet());
        log.info("[DP-MECHANISM] Keys with contributions this window: {}", s_i.size());

        // Algo 3 Case 2 - Load keys with predicted release time = current time step
        // "DP-SQLP loaded system states for key k at predicted time tr_p"
        Iterator<Map.Entry<String, Integer>> predictionIt = predictedReleaseTimes.entrySet().iterator();
        while (predictionIt.hasNext()) {
            Map.Entry<String, Integer> entry = predictionIt.next();
            if (entry.getValue() == timeStep) {
                s_i.add(entry.getKey());
                predictionIt.remove(); // remove prediction as we are processing it now
            }
        }

        // Step 3 of Algo 2 - ensure all previously selected keys are included (S^(i) includes all selected keys)
        s_i.addAll(selectedKeys);

        log.debug("[DP-MECHANISM] Total keys to process: {} (contributions={}, predicted={}, selected={})",
                s_i.size(), currentWindowCounts.size(), predictedReleaseTimes.size(), selectedKeys.size());

        // 2. Process each key
        for (String key : s_i) {
            boolean alreadySelected = selectedKeys.contains(key);
            boolean selectedThisStep = false;

            // Get inputs for this window
            double countInput = currentWindowCounts.getOrDefault(key, 0.0);

            // Step 7 of Algo 2 - Aggregate contributions for this key since last release to compute DeltaV_key
            unreleasedHistogramBuffer.merge(key, countInput, Double::sum);

            // --- Key Selection Logic (Algo 1 & 3) ---

            // Algo 3 Case 1 - Key appears before predicted time (j < n < p)
            // "The prior prediction result is discarded"
            if (predictedReleaseTimes.containsKey(key)) {
                int predictedTime = predictedReleaseTimes.get(key);
                if (predictedTime > timeStep) {
                    // Key appeared before prediction - discard stale prediction
                    predictedReleaseTimes.remove(key);
                }
            }

            // Algo 1 step 5 - for all key in S^(i)\S^(i-1) create a new tree and execute Algorithm 4 till (i-1)-th
            // step with all zeros as input
            BinaryAggregationTree t_key;
            if (!s_i_prev.contains(key)) {
                // Key is in S^(i) but NOT in S^(i-1) - create NEW tree
                t_key = new BinaryAggregationTree(maxTimeSteps, sigmaKey);

                // Execute Algorithm 4 for steps 0 to (timeStep-1) with zeros
                // This "catches up" the tree to the current time step
                for (int t = 0; t < timeStep; t++) {
                    t_key.addToTree(t, 0.0);
                }

                keySelectionForest.put(key, t_key);

                // Reset user tracking for this key since we're starting fresh
                observedUsersForKeySelection.remove(key);

                log.debug("[DP-MECHANISM] Key {} in S^(i)\\S^(i-1) - created new tree and caught up to timeStep {}",
                         key, timeStep);
            } else {
                // Key was in S^(i-1) - reuse existing tree (if it exists)
                t_key = keySelectionForest.get(key);

                // If tree was removed (key was selected and resetKeySelectionState was called), create a new tree.
                if (t_key == null) {
                    /*
                     FIXME: happens when a key is in selectedKeys (always included in s_i) but the relative tree was removed after being selected in a previous time step. We should refactor this.
                     */

                    log.warn("[DP-MECHANISM] Key {} was in S^(i-1) but tree is missing (likely selected previously) - creating new tree", key);
                    t_key = new BinaryAggregationTree(maxTimeSteps, sigmaKey);

                    // Catch up to current time step
                    for (int t = 0; t < timeStep; t++) {
                        t_key.addToTree(t, 0.0);
                    }

                    keySelectionForest.put(key, t_key);
                    observedUsersForKeySelection.remove(key);
                } else {
                    log.debug("[DP-MECHANISM] Key {} in both S^(i) and S^(i-1) - reusing existing tree", key);
                }
            }

            // Step 7 of Algo 1: Add count_key(D_tr_i) - count_key(D_tr_(i-1)) to the tree
            //
            // D_tr_i = cumulative sub-stream from time 0 to i -> need to track unique users across all time steps
            Set<String> observedUsers = observedUsersForKeySelection.computeIfAbsent(key, k -> new HashSet<>());
            Set<String> windowUsers = currentWindowUniqueUsers.getOrDefault(key, Collections.emptySet());

            // Count NEW unique users in current window (users not seen before for this key)
            int newUniqueUsers = 0;
            for (String userId : windowUsers) {
                if (observedUsers.add(userId)) {
                    newUniqueUsers++; // NOTE: this is count_key(D_tr_i) - count_key(D_tr_(i-1))
                }
            }

            // Add the delta to the tree
            t_key.addToTree(timeStep, newUniqueUsers);

            // Step 8 of Algo 1 - compute the noisy unique users for this key at time step i
            double noisyUniqueUsers = t_key.getTotalSum(timeStep);

            // Step 9 of Algo 1 - check key selection condition

            // Compute the Honaker variance at this time step
            double currentVariance = t_key.getHonakerVariance(timeStep);
            // Compute tau for the current time step using the inverse CDF of the Gaussian distribution at beta - 1
            double tau_tr_i = computeTau(currentVariance, this.BETA);
            if (noisyUniqueUsers >= (double) mu + tau_tr_i) {
                // KEY SELECTED FOR RELEASE!
                selectedKeys.add(key);
                selectedThisStep = true;

                // Reset state for fresh selection in future time steps
                resetKeySelectionState(key);
                log.debug("[DP-MECHANISM] Key {} SELECTED (noisy users={} >= mu+tau={})",
                        key, noisyUniqueUsers, mu + tau_tr_i);
            } else {
                // NOT SELECTED

                // Step 2-3 of Algo 3 - For keys in S_tr_j not selected, run prediction
                // NOTE: S_tr_j = All key \in DeltaD_tr_j not selected via Algorithm 1

                // Predict if this key will be selected in future time steps due to noise
                runEmptyKeyPrediction(key, t_key);
            }

            // Hierarchical perturbation (Algo 2) for selected keys
            if (alreadySelected || selectedThisStep) {
                updateHistogramTree(key);
            }
        }

        // Cleanup trees for keys in S^(i-1) \ S^(i) (keys that disappeared this time step)
        // These keys no longer need tracking until they reappear
        for (String previousKey : s_i_prev) {
            if (!s_i.contains(previousKey)) {
                keySelectionForest.remove(previousKey);
                observedUsersForKeySelection.remove(previousKey);
                log.debug("[DP-MECHANISM] Key {} in S^(i-1)\\S^(i) - removed tree and user tracking (key disappeared)", previousKey);
            }
        }

        // Update S^(i-1) for next iteration
        s_i_prev = new HashSet<>(s_i);

        // Cleanup current window buffers
        currentWindowCounts.clear();
        currentWindowUniqueUsers.clear();

        // Proceed to next time step
        timeStep++;

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
        unreleasedHistogramBuffer.put(key, 0.0);
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
            double futureTau = computeTau(futureVariance, this.BETA);

            // Step 7 of Algo 3 - if key is selected, then write <key, tr_p> to state store and break
            if (predictedNoisyCount >= (double) mu + futureTau) {
                // Key predicted to be selected at future time tr_p due to noise
                predictedReleaseTimes.put(key, tr_p);

                log.info("[PREDICTION] Key {} will be released at future step tr_p={} (checked {} iterations)",
                        key, tr_p, iterationCount);
                break; // Found the earliest release time
            }
        }
    }

    /**
     * Computes the tau value based on the Gaussian distribution.
     * <p>
     * Refer to Appendix D of the "Differentially Private Stream Processing at Scale" paper.
     *
     * @param lambda_square The total Honaker variance at time step i.
     * @param beta       The desired confidence level.
     * @return The computed tau value.
     */
    private static double computeTau(double lambda_square, double beta) {
        double std_dev = FastMath.sqrt(lambda_square);
        NormalDistribution distribution = new NormalDistribution(0, std_dev);
        return distribution.inverseCumulativeProbability(1.0 - beta);
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
