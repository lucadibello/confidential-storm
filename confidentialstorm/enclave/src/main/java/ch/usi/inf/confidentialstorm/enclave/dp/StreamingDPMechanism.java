package ch.usi.inf.confidentialstorm.enclave.dp;

import java.util.*;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.util.FastMath;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLoggerFactory;

/**
 * This class implements Differentially Private Stream Aggregation mechanism (DP-SQLP) from the paper: "Differentially Private Stream Processing at Scale".
 * <p>
 * This class encapsulates:
 * 1. Streaming Key Selection (Algorithm 1): Identifying keys with sufficient unique user contributions.
 * 2. Hierarchical Perturbation (Algorithm 2): Releasing noisy aggregate counts for selected keys.
 * 3. Empty Key Release Prediction (Algorithm 3): Predicting when keys might be released due to noise.
 * 4. Use of Binary Aggregation Trees (Algorithm 4) for efficient DP aggregation over time.
 */
public class StreamingDPMechanism {
    private static final EnclaveLogger log = EnclaveLoggerFactory.getLogger(StreamingDPMechanism.class);

    // Aggregation trees for key selection and histogram (algorithm 4)
    private final Map<String, BinaryAggregationTree> keySelectionForest = new HashMap<>();
    private final Map<String, BinaryAggregationTree> histogramForest = new HashMap<>();
    
    // keys that have been selected for release so far (throughout the entire stream)
    private final Set<String> selectedKeys = new HashSet<>();

    // sum of released noisy counts for each key (algorithm 2)
    private final Map<String, Double> currentSums = new HashMap<>();

    // keep track of predicted release times for keys (algorithm 3)
    private final Map<String, Integer> predictedReleaseTimes = new HashMap<>();

    // keep track of all users that have contributed to each key so far (map of key:set of userIds)
    // NOTE: this is needed to ensure sensitivity = 1 for key selection
    private final Map<String, Set<String>> observedUsersForKeySelection = new HashMap<>();

    // Buffer for hierarchical perturbation (Algorithm 2)
    // Stores aggregated value (Delta V) since last release (or start)
    private final Map<String, Double> unreleasedHistogramBuffer = new HashMap<>();

    // Buffers for current time window contributions (needed for key selection)
    private final Map<String, Double> currentWindowCounts = new HashMap<>();
    private final Map<String, Set<String>> currentWindowUniqueUsers = new HashMap<>();

    private final double sigmaKey;
    private final double sigmaHist;
    private final int maxTimeSteps;
    private final long mu;

    private final double beta = 1e-5; // Confidence level for tau computation

    private int timeStep = 0;

    /**
     * @param sigmaKey     Noise scale for key selection (based on sensitivity 1).
     * @param sigmaHist    Noise scale for histogram (based on sensitivity C*L).
     * @param maxTimeSteps Maximum number of time steps to process.
     * @param mu           Threshold for key selection (if exceeded, key is selected for release).
     */
    public StreamingDPMechanism(double sigmaKey, double sigmaHist, int maxTimeSteps, long mu) {
        this.sigmaKey = sigmaKey;
        this.sigmaHist = sigmaHist;
        this.maxTimeSteps = maxTimeSteps;
        this.mu = mu;
    }

    /**
     * Stores the count and tracks unique users for key selection in the current time window.
     *
     * @param key    The aggregation key
     * @param count  The value to aggregate
     * @param userId The unique user identifier (needed for key selection).
     */
    public void addContribution(String key, double count, String userId) {
        currentWindowCounts.merge(key, count, Double::sum);
        currentWindowUniqueUsers.computeIfAbsent(key, k -> new HashSet<>()).add(userId); // record that userId contributed to this key
    }

    /**
     * Proceed by one time step processing all contributions in this window,
     * running the prediction algorithm (algorithm 3), and updating the DP trees.
     *
     * @return The noisy histogram (map of key:count).
     */
    public Map<String, Long> snapshot() {
        log.debug("[DP-MECHANISM] snapshot() START - timeStep={}, maxTimeSteps={}", timeStep, maxTimeSteps);

        // FIXME: handle case where snapshot is called after maxTimeSteps
        if (timeStep >= maxTimeSteps) {
            log.info("[DP-MECHANISM] timeStep >= maxTimeSteps, returning final histogram");
            return produceHistogram();
        }

        // 1. Identify all keys that need processing in this step
        // a) Keys with active contributions in this window
        // b) Keys predicted to be released at this specific timeStep (Case 2 of Algo 3)

        // Case a
        Set<String> keysToProcess = new HashSet<>(currentWindowCounts.keySet());
        log.info("[DP-MECHANISM] Keys with contributions this window: {}", keysToProcess.size());

        // Case b
        Iterator<Map.Entry<String, Integer>> predictionIt = predictedReleaseTimes.entrySet().iterator();
        while (predictionIt.hasNext()) {
            Map.Entry<String, Integer> entry = predictionIt.next();
            if (entry.getValue() == timeStep) {
                keysToProcess.add(entry.getKey());
                predictionIt.remove();  // remove prediction as we are processing it now
            }
        }

        // NOTE: add also all previously selected keys to keep updating their histograms
        keysToProcess.addAll(selectedKeys);

        log.debug("[DP-MECHANISM] Total keys to process: {} (contributions={}, predicted={}, selected={})",
                keysToProcess.size(), currentWindowCounts.size(), predictedReleaseTimes.size(), selectedKeys.size());

        // 2. Process each key
        int processedCount = 0;
        for (String key : keysToProcess) {
            // FIXME: log for debugging, remove later
            processedCount++;
            if (processedCount % 100 == 0) {
                log.info("[DP-MECHANISM] Processing key {}/{}", processedCount, keysToProcess.size());
            }

            boolean alreadySelected = selectedKeys.contains(key);
            boolean selectedThisStep = false;

            // Get inputs for this window
            double countInput = currentWindowCounts.getOrDefault(key, 0.0);
            Set<String> windowUsers = currentWindowUniqueUsers.getOrDefault(key, Collections.emptySet());

            // Accumulate histogram buffer (Algo 2: Delta V accumulates until release)
            unreleasedHistogramBuffer.merge(key, countInput, Double::sum);

            // --- Key Selection Logic (Algo 1 & 3) ---
            
            // Check if key is predicted to be released in the future, but we process it now instead
            if (predictedReleaseTimes.containsKey(key)) {
                int predictedTime = predictedReleaseTimes.get(key);
                if (predictedTime > timeStep) {
                    predictedReleaseTimes.remove(key); // remove wrong prediction
                }
            }

            // get the binary aggregation tree for this key
            BinaryAggregationTree keyTree = keySelectionForest.computeIfAbsent(
                    key, k -> new BinaryAggregationTree(maxTimeSteps, sigmaKey)
            );

            // Filter for unique users in the entire stream (sensitivity = 1, refer to 4.3 of the paper)
            Set<String> observedUsers = observedUsersForKeySelection.computeIfAbsent(key, k -> new HashSet<>());
            int newUniqueUsers = 0;
            for (String userId : windowUsers) {
                if (observedUsers.add(userId)) {
                    newUniqueUsers++;
                }
            }

            // Add unique users count to the tree
            double noisyUniqueUsers = keyTree.addToTree(timeStep, newUniqueUsers);

            // Calculate time-dependent threshold tau_i to compare against the noisy unique users
            // NOTE: Refer to Appendix D of the "Differentially Private Stream Processing at Scale" paper for details

            // Compute the Honaker variance at this time step
            double currentVariance = keyTree.getHonakerVariance(timeStep);
            // Compute tau using the inverse CDF of the Gaussian distribution at beta - 1
            double tauTimeDependent = computeTau(currentVariance, this.beta);

            // check if noisy unique users exceed threshold
            if (noisyUniqueUsers >= (double) mu + tauTimeDependent) {
                // KEY SELECTED FOR RELEASE!
                selectedKeys.add(key);
                selectedThisStep = true;

                // Reset state for fresh selection in future time steps
                resetKeySelectionState(key);
                log.debug("[DP-MECHANISM] Key {} SELECTED (noisy users={} >= mu+tau={})",
                        key, noisyUniqueUsers, mu + tauTimeDependent);
            } else {
                // NOT SELECTED

                // FIXME: log for debugging, remove later
                if (processedCount % 100 == 0) {
                    log.debug("[DP-MECHANISM] Key not {} SELECTED (noisy users={} >= mu+tau={})",
                            key, noisyUniqueUsers, mu + tauTimeDependent);
                }

                // Predict if this key will be selected in future time steps (Algo 3)
                runEmptyKeyPrediction(key, keyTree);
            }

            // Hierarchical perturbation (Algo 2) for selected keys
            if (alreadySelected || selectedThisStep) {
                updateHistogramTree(key);
            }
        }

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

    private void updateHistogramTree(String key) {
        BinaryAggregationTree histTree = histogramForest.computeIfAbsent(
                key, k -> new BinaryAggregationTree(maxTimeSteps, sigmaHist)
        );
        
        // Algo 2: add the accumulated Delta V to the tree
        double deltaV = unreleasedHistogramBuffer.getOrDefault(key, 0.0);
        double noisySum = histTree.addToTree(timeStep, deltaV);
        
        // Store current result
        currentSums.put(key, noisySum);
        
        // Clear the buffer since we've now incorporated it into the tree
        unreleasedHistogramBuffer.put(key, 0.0);
    }

    private void runEmptyKeyPrediction(String key, BinaryAggregationTree keyTree) {
        if (predictedReleaseTimes.containsKey(key)) {
            log.debug("[PREDICTION] Key {} already has prediction, skipping", key);
            return;
        }

        int futureSteps = maxTimeSteps - timeStep - 1;
        log.debug("[PREDICTION] Starting prediction for key {} ({} future steps to check)", key, futureSteps);

        // Simulate future steps -> we add 0 contribution for each future step and check if it would be selected
        int iterationCount = 0;
        for (int t = timeStep + 1; t < maxTimeSteps; t++) {
            iterationCount++;

            // compute predicted noisy unique count at future step t
            double predictedNoisyCount = keyTree.query(t);

            // compute future tau in the same way as before
            double futureVariance = keyTree.getHonakerVariance(t);
            double futureTau = computeTau(futureVariance, this.beta);

            // check if predicted noisy count exceeds threshold (selection threshold)
            if (predictedNoisyCount >= (double) mu + futureTau) {
                predictedReleaseTimes.put(key, t);

                // log for debugging
                log.info("[PREDICTION] Key {} will be released at future step t={} (checked {} iterations)",
                        key, t, iterationCount);
                break; // Found the earliest release time
            }
        }
    }

    /**
     * Computes the tau value based on the Gaussian distribution.
     * <p>
     * Refer to Appendix D of the "Differentially Private Stream Processing at Scale" paper.
     *
     * @param lambda_square The total Honaker variance at time step i
     * @param beta       The desired confidence level
     * @return The computed tau value
     */
    private static double computeTau(double lambda_square, double beta) {
        double std_dev = FastMath.sqrt(lambda_square);
        NormalDistribution distribution = new NormalDistribution(0, std_dev);
        return distribution.inverseCumulativeProbability(1.0 - beta);
    }

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

    private void resetKeySelectionState(String key) {
        keySelectionForest.remove(key);
        observedUsersForKeySelection.remove(key);
        predictedReleaseTimes.remove(key);
    }
}
