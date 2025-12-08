package ch.usi.inf.confidentialstorm.enclave.dp;

import java.util.*;

import org.apache.commons.math3.distribution.NormalDistribution;

/**
 * Implements the Differentially Private Stream Aggregation mechanism (DP-SQLP).
 * <p>
 * This class encapsulates:
 * 1. Streaming Key Selection (Algorithm 1): Identifying keys with sufficient unique user contributions.
 * 2. Hierarchical Perturbation (Algorithm 2): Releasing noisy aggregate counts for selected keys.
 * 3. Empty Key Release Prediction (Algorithm 3): Predicting when keys might be released due to noise.
 * <p>
 * It manages the state (DP Trees) for all observed keys.
 */
public class StreamingDPMechanism {
    // Trees
    private final Map<String, BinaryAggregationTree> keySelectionForest = new HashMap<>();
    private final Map<String, BinaryAggregationTree> histogramForest = new HashMap<>();
    
    // State
    private final Set<String> selectedKeys = new HashSet<>();
    private final Map<String, Double> currentSums = new HashMap<>();

    // Algorithm 3: Prediction State
    // Key -> TimeStep when it is predicted to be released
    private final Map<String, Integer> predictedReleaseTimes = new HashMap<>();

    // Buffer for hierarchical perturbation (Algorithm 2)
    // Stores aggregated value (Delta V) since last release (or start)
    private final Map<String, Double> unreleasedHistogramBuffer = new HashMap<>();

    private final Map<String, Double> currentWindowCounts = new HashMap<>();
    private final Map<String, Set<String>> currentWindowUniqueUsers = new HashMap<>();

    private final double sigmaKey;
    private final double sigmaHist;
    private final int maxTimeSteps;
    private final long mu;

    private final double beta = 1e-5; // Confidence level for tau computation

    private int timeStep = 0;

    /**
     * @param sigmaKey     Noise scale for Key Selection (based on sensitivity 1).
     * @param sigmaHist    Noise scale for Histogram (based on sensitivity C*L).
     * @param maxTimeSteps Maximum number of triggering steps.
     * @param mu           Threshold for key selection (minimum unique users).
     */
    public StreamingDPMechanism(double sigmaKey, double sigmaHist, int maxTimeSteps, long mu) {
        this.sigmaKey = sigmaKey;
        this.sigmaHist = sigmaHist;
        this.maxTimeSteps = maxTimeSteps;
        this.mu = mu;
    }

    /**
     * Buffer a contribution for the current time window.
     *
     * @param key    The aggregation key (e.g., word).
     * @param count  The value to aggregate (e.g., 1.0).
     * @param userId The unique user identifier (for key selection).
     */
    public void addContribution(String key, double count, String userId) {
        currentWindowCounts.merge(key, count, Double::sum);
        currentWindowUniqueUsers.computeIfAbsent(key, k -> new HashSet<>()).add(userId);
    }

    /**
     * Advances the mechanism by one time step, processing all buffered contributions,
     * running prediction logic, and updating the DP trees.
     *
     * @return The current noisy histogram (map of Key -> Count).
     */
    public Map<String, Long> snapshot() {
        if (timeStep >= maxTimeSteps) {
            return produceHistogram();
        }

        // 1. Identify all keys that need processing in this step
        // a) Keys with active contributions in this window
        Set<String> keysToProcess = new HashSet<>(currentWindowCounts.keySet());

        // b) Keys predicted to be released at this specific timeStep (Case 2 of Algo 3)
        // We iterate predictedReleaseTimes to find matches
        Iterator<Map.Entry<String, Integer>> predictionIt = predictedReleaseTimes.entrySet().iterator();
        while (predictionIt.hasNext()) {
            Map.Entry<String, Integer> entry = predictionIt.next();
            if (entry.getValue() == timeStep) {
                keysToProcess.add(entry.getKey());
                // Remove the prediction as we are handling it now
                predictionIt.remove(); 
            }
        }

        // c) Keys already selected
        keysToProcess.addAll(selectedKeys);


        // 2. Process each key
        for (String key : keysToProcess) {
            // Get inputs for this window
            double countInput = currentWindowCounts.getOrDefault(key, 0.0);
            int uniqueUsersInput = currentWindowUniqueUsers.getOrDefault(key, Collections.emptySet()).size();

            // Accumulate histogram buffer (Algo 2: Delta V accumulates until release)
            unreleasedHistogramBuffer.merge(key, countInput, Double::sum);

            // If already selected, we just update the histogram tree
            if (selectedKeys.contains(key)) {
                updateHistogramTree(key);
                continue;
            }

            // --- Key Selection Logic (Algo 1 & 3) ---
            
            // Check if we have a stale prediction for the future (Case 1 of Algo 3)
            // If we are processing this key now (due to input) but it was predicted for later,
            // we must invalidate that prediction because the state is changing.
            if (predictedReleaseTimes.containsKey(key)) {
                int predictedTime = predictedReleaseTimes.get(key);
                if (predictedTime > timeStep) {
                    predictedReleaseTimes.remove(key);
                }
            }

            // Update Key Selection Tree
            BinaryAggregationTree keyTree = keySelectionForest.computeIfAbsent(
                    key, k -> new BinaryAggregationTree(maxTimeSteps, sigmaKey)
            );
            // Add unique users count to the tree
            double noisyUniqueUsers = keyTree.addToTree(timeStep, uniqueUsersInput);

            // Calculate time-dependent threshold tau_i

            // we compute the standard deviation of the noise at this time step
            // NOTE: Refer to Appendix D of the "Differentially Private Stream Processing at Scale" paper for details

            // To do so, we retrieve the Honaker variance at this time step
            double currentVariance = keyTree.getHonakerVariance(timeStep);
            // double futureTau = 5.0 * Math.sqrt(futureVariance); -> previous heuristic
            double tauTimeDependent = computeTau(currentVariance, this.beta);

            if (noisyUniqueUsers >= (double) mu + tauTimeDependent) {
                // SELECTED!
                selectedKeys.add(key);
                // Flush accumulated buffer to histogram tree
                updateHistogramTree(key);
            } else {
                // NOT SELECTED
                // Run Prediction (Algo 3): check if noise alone will trigger it in future
                runEmptyKeyPrediction(key, keyTree);
            }
        }

        // Cleanup current window buffers
        currentWindowCounts.clear();
        currentWindowUniqueUsers.clear();
        
        // Advance time
        timeStep++;

        return produceHistogram();
    }

    private void updateHistogramTree(String key) {
        BinaryAggregationTree histTree = histogramForest.computeIfAbsent(
                key, k -> new BinaryAggregationTree(maxTimeSteps, sigmaHist)
        );
        
        // Algo 2: Add the accumulated Delta V to the tree
        double deltaV = unreleasedHistogramBuffer.getOrDefault(key, 0.0);
        double noisySum = histTree.addToTree(timeStep, deltaV);
        
        // Store current result
        currentSums.put(key, noisySum);
        
        // Clear the buffer since we've now incorporated it into the tree
        unreleasedHistogramBuffer.put(key, 0.0);
    }

    private void runEmptyKeyPrediction(String key, BinaryAggregationTree keyTree) {
        if (predictedReleaseTimes.containsKey(key)) {
            return;
        }

        // Simulate future steps
        for (int t = timeStep + 1; t < maxTimeSteps; t++) {
            // "Simulate" adding 0 to the tree and querying
            double predictedNoisyCount = keyTree.query(t);
            
            // Reverting to previous heuristic: 5 * sigma covers very high confidence
            double futureVariance = keyTree.getHonakerVariance(t);
            double futureTau = computeTau(futureVariance, this.beta);
            
            if (predictedNoisyCount >= (double) mu + futureTau) {
                predictedReleaseTimes.put(key, t);
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
        double std_dev = Math.sqrt(lambda_square);
        NormalDistribution distribution = new NormalDistribution(0, std_dev);
        return distribution.inverseCumulativeProbability(1.0 - beta);
    }

    private Map<String, Long> produceHistogram() {
        Map<String, Long> sortedHistogram = new LinkedHashMap<>();

        // Sort by value descending
        currentSums.entrySet().stream()
                .sorted((a, b) -> Double.compare(b.getValue(), a.getValue()))
                .forEach(entry -> sortedHistogram.put(entry.getKey(), Math.round(entry.getValue())));

        return sortedHistogram;
    }
}