package ch.usi.inf.examples.synthetic_baseline.dp;

import java.util.*;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.util.FastMath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Differentially Private Stream Aggregation mechanism (DP-SQLP).
 * <p>
 * This DP mechanism includes the following components as described in the paper:
 * 1. Streaming Key Selection (Algorithm 1): Identifying keys with sufficient unique user contributions.
 * 2. Hierarchical Perturbation (Algorithm 2): Releasing noisy aggregate counts for selected keys.
 * 3. Empty Key Release Prediction (Algorithm 3): Predicting when keys might be released due to noise.
 * 4. Use of Binary Aggregation Trees (Algorithm 4) for efficient DP aggregation over time.
 */
public class StreamingDPMechanism {
    private static final Logger log = LoggerFactory.getLogger(StreamingDPMechanism.class);

    private final Map<String, BinaryAggregationTree> keySelectionForest = new HashMap<>();
    private final Map<String, BinaryAggregationTree> histogramForest = new HashMap<>();
    private final Set<String> selectedKeys = new HashSet<>();
    private final Map<String, Double> currentSums = new HashMap<>();
    private final Map<String, Integer> predictedReleaseTimes = new HashMap<>();
    private final Map<String, Set<String>> observedUsersForKeySelection = new HashMap<>();
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
     * Links each key to the set of unique user IDs that have contributed to it in the current time step.
     * Written by {@code addContribution()}, swapped and drained by {@code snapshot()}.
     */
    private Map<String, Set<String>> stagingWindowUniqueUsers = new HashMap<>();

    /**
     * Lightweight lock that protects only the reference swap between the staging
     * buffers and the snapshot drain buffers. Held for O(1).
     */
    private final Object bufferLock = new Object();

    private final double sigmaKey;
    private final double sigmaHist;
    private final int maxTimeSteps;
    private final long mu;

    private static final double PROBIT_1_MINUS_BETA =
            new NormalDistribution(0, 1).inverseCumulativeProbability(1.0 - 1e-5);

    private int timeStep = 0;

    public StreamingDPMechanism(double sigmaKey,
                                double sigmaHist,
                                int maxTimeSteps,
                                long mu,
                                long maxContributionsPerUser) {
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
     * This method is thread-safe and can be called concurrently by multiple executor threads.
     */
    public void addContribution(String userId, String key, double clamped_count) {
        synchronized (bufferLock) {
            stagingWindowCounts.merge(key, clamped_count, Double::sum);
            stagingWindowUniqueUsers.computeIfAbsent(key, k -> new HashSet<>()).add(userId);
        }
    }

    /**
     * Proceeds by one time step, processing all contributions in the current window.
     */
    public Map<String, Long> snapshot() {
        log.debug("[DP-MECHANISM] snapshot() START - timeStep={}, maxTimeSteps={}", timeStep, maxTimeSteps);

        final Map<String, Double> currentWindowCounts;
        final Map<String, Set<String>> currentWindowUniqueUsers;
        synchronized (bufferLock) {
            currentWindowCounts = this.stagingWindowCounts;
            currentWindowUniqueUsers = this.stagingWindowUniqueUsers;
            this.stagingWindowCounts = new HashMap<>();
            this.stagingWindowUniqueUsers = new HashMap<>();
        }

        if (timeStep >= maxTimeSteps) {
            log.info("[DP-MECHANISM] timeStep >= maxTimeSteps, returning final histogram");
            trimExpiredState();
            return produceHistogram();
        }

        currentSums.clear();

        Set<String> s_i = new HashSet<>(currentWindowCounts.keySet());
        log.info("[DP-MECHANISM] Keys with contributions this window: {}", s_i.size());

        Iterator<Map.Entry<String, Integer>> predictionIt = predictedReleaseTimes.entrySet().iterator();
        while (predictionIt.hasNext()) {
            Map.Entry<String, Integer> entry = predictionIt.next();
            if (entry.getValue() == timeStep) {
                s_i.add(entry.getKey());
                predictionIt.remove();
            }
        }

        log.debug("[DP-MECHANISM] Keys with contributions this window: {}, predicted releases: {}, total selected keys: {}",
                    currentWindowCounts.size(), predictedReleaseTimes.size(), selectedKeys.size());

        for (String key : s_i) {
            boolean alreadySelected = selectedKeys.contains(key);
            boolean selectedThisStep = false;

            double countInput = currentWindowCounts.getOrDefault(key, 0.0);
            unreleasedHistogramBuffer.merge(key, countInput, Double::sum);

            if (!alreadySelected) {
                if (predictedReleaseTimes.containsKey(key)) {
                    int predictedTime = predictedReleaseTimes.get(key);
                    if (predictedTime > timeStep) {
                        predictedReleaseTimes.remove(key);
                    }
                }

                BinaryAggregationTree t_key = keySelectionForest.get(key);
                if (t_key == null) {
                    t_key = new BinaryAggregationTree(maxTimeSteps, sigmaKey);
                    keySelectionForest.put(key, t_key);
                    observedUsersForKeySelection.remove(key);
                    log.debug("[DP-MECHANISM] Key {} initialized with new key-selection tree", key);
                }

                Set<String> observedUsers = observedUsersForKeySelection.computeIfAbsent(key, k -> new HashSet<>());
                Set<String> windowUsers = currentWindowUniqueUsers.getOrDefault(key, Collections.emptySet());

                int newUniqueUsers = 0;
                for (String userId : windowUsers) {
                    if (observedUsers.add(userId)) {
                        newUniqueUsers++;
                    }
                }

                t_key.addToTree(timeStep, newUniqueUsers);

                double noisyUniqueUsers = t_key.getTotalSum(timeStep);

                double currentVariance = t_key.getHonakerVariance(timeStep);
                double tau_tr_i = computeTau(currentVariance);
                if (noisyUniqueUsers >= (double) mu + tau_tr_i) {
                    selectedKeys.add(key);
                    selectedThisStep = true;
                    resetKeySelectionState(key);
                    log.debug("[DP-MECHANISM] Key {} SELECTED (noisy users={} >= mu+tau={})",
                            key, noisyUniqueUsers, mu + tau_tr_i);
                } else {
                    runEmptyKeyPrediction(key, t_key);
                }
            }

            if (alreadySelected || selectedThisStep) {
                updateHistogramTree(key);
            }
        }

        timeStep++;

        Map<String, Long> result = produceHistogram();
        log.info("[DP-MECHANISM] snapshot() COMPLETE - returning {} keys", result.size());
        return result;
    }

    private void updateHistogramTree(String key) {
        BinaryAggregationTree histTree = histogramForest.get(key);
        if (histTree == null) {
            histTree = new BinaryAggregationTree(maxTimeSteps, sigmaHist);
            for (int t = 0; t < timeStep; t++) {
                histTree.addToTree(t, 0.0);
            }
            histogramForest.put(key, histTree);
            log.debug("[DP-MECHANISM] Created new histogram tree for key {} and caught up to timeStep {}", key, timeStep);
        }

        double deltaV = unreleasedHistogramBuffer.getOrDefault(key, 0.0);
        histTree.addToTree(timeStep, deltaV);
        double noisySum = histTree.getTotalSum(timeStep);
        currentSums.put(key, noisySum);
        unreleasedHistogramBuffer.remove(key);
    }

    private void runEmptyKeyPrediction(String key, BinaryAggregationTree keyTree) {
        if (predictedReleaseTimes.containsKey(key)) {
            log.debug("[PREDICTION] Key {} already has prediction, skipping", key);
            return;
        }

        int futureSteps = maxTimeSteps - timeStep - 1;
        log.debug("[PREDICTION] Starting prediction for key {} ({} future steps to check)", key, futureSteps);

        for (int tr_p = timeStep + 1; tr_p < maxTimeSteps; tr_p++) {
            double predictedNoisyCount = keyTree.getTotalSum(tr_p);
            double futureVariance = keyTree.getHonakerVariance(tr_p);
            double futureTau = computeTau(futureVariance);

            if (predictedNoisyCount >= (double) mu + futureTau) {
                predictedReleaseTimes.put(key, tr_p);
                log.debug("[PREDICTION] Key {} will be released at future step tr_p={}", key, tr_p);
                break;
            }
        }
    }

    private static double computeTau(double lambda_square) {
        return FastMath.sqrt(lambda_square) * PROBIT_1_MINUS_BETA;
    }

    private Map<String, Long> produceHistogram() {
        Map<String, Long> sortedHistogram = new LinkedHashMap<>();

        currentSums.entrySet().stream()
                .sorted((a, b) -> Double.compare(b.getValue(), a.getValue()))
                .forEach(entry -> {
                    long rounded = FastMath.round(entry.getValue());
                    sortedHistogram.put(entry.getKey(), FastMath.max(0L, rounded));
                });

        return sortedHistogram;
    }

    private void trimExpiredState() {
        keySelectionForest.clear();
        histogramForest.clear();
        selectedKeys.clear();
        observedUsersForKeySelection.clear();
        predictedReleaseTimes.clear();
        unreleasedHistogramBuffer.clear();
        synchronized (bufferLock) {
            stagingWindowCounts.clear();
            stagingWindowUniqueUsers.clear();
        }
        log.info("[DP-MECHANISM] trimExpiredState: released all per-key state after maxTimeSteps={}", maxTimeSteps);
    }

    private void resetKeySelectionState(String key) {
        keySelectionForest.remove(key);
        observedUsersForKeySelection.remove(key);
        predictedReleaseTimes.remove(key);
    }
}
