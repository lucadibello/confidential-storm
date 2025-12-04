package ch.usi.inf.examples.confidential_word_count.enclave.service.bolts.histogram;

import ch.usi.inf.confidentialstorm.common.crypto.exception.CipherInitializationException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.SealedPayloadProcessingException;
import ch.usi.inf.confidentialstorm.enclave.dp.BinaryAggregationTree;
import ch.usi.inf.confidentialstorm.enclave.util.DPUtil;
import ch.usi.inf.examples.confidential_word_count.common.api.HistogramService;
import ch.usi.inf.examples.confidential_word_count.common.api.model.HistogramSnapshotResponse;
import ch.usi.inf.examples.confidential_word_count.common.api.model.HistogramUpdateRequest;
import ch.usi.inf.examples.confidential_word_count.common.config.DPConfig;
import com.google.auto.service.AutoService;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@AutoService(HistogramService.class)
public final class HistogramServiceImpl extends HistogramServiceVerifier {
    private final Map<String, BinaryAggregationTree> forest = new HashMap<>();
    private final Map<String, Double> currentSums = new HashMap<>();
    private final Map<String, Double> pendingCounts = new HashMap<>();
    private final double sigma;
    private int triggerIndex = 0;

    @SuppressWarnings("unused")
    public HistogramServiceImpl() {
        // Calibrate noise with user-level sensitivity C * L_m (refer to section 3.2 of the paper)
        double rho = DPUtil.cdpRho(DPConfig.EPSILON, DPConfig.DELTA);
        double l1Sensitivity = DPConfig.l1Sensitivity();
        this.sigma = DPUtil.calculateSigma(rho, DPConfig.MAX_TIME_STEPS, l1Sensitivity);
    }

    private static Map<String, Long> produceHistogram(List<Map.Entry<String, Double>> sortedEntries) {
        Map<String, Long> sortedHistogram = new LinkedHashMap<>();
        for (Map.Entry<String, Double> entry : sortedEntries) {
            long rounded = Math.round(entry.getValue());

            // if the rounded value is negative (due to noise), we don't include the entry in the histogram!
            // this can happen when the true count is low and the noise is high
            if (rounded < 0L) {
                continue;
            }
            sortedHistogram.put(entry.getKey(), rounded);
        }
        return sortedHistogram;
    }

    @Override
    public void updateImpl(HistogramUpdateRequest update) throws SealedPayloadProcessingException, CipherInitializationException {
        String word = sealedPayload.decryptToString(update.word());
        double count = Double.parseDouble(sealedPayload.decryptToString(update.count()));

        // Aggregate contributions within the current trigger window; noise will be added on snapshot().
        pendingCounts.merge(word, count, Double::sum);
    }

    @Override
    public HistogramSnapshotResponse snapshot() {
        // Apply hierarchical perturbation for all keys seen in the current trigger window.
        if (!pendingCounts.isEmpty()) {
            for (Map.Entry<String, Double> entry : pendingCounts.entrySet()) {
                String word = entry.getKey();
                double count = entry.getValue();

                BinaryAggregationTree tree = forest.computeIfAbsent(
                        word,
                        k -> new BinaryAggregationTree(DPConfig.MAX_TIME_STEPS, sigma)
                );
                if (triggerIndex < DPConfig.MAX_TIME_STEPS) {
                    double noisySum = tree.addToTree(triggerIndex, count);
                    currentSums.put(word, noisySum);
                }
            }
            pendingCounts.clear();
            triggerIndex++;
        }

        // Get the entries from the current histogram + sort them by value (bigger first)
        List<Map.Entry<String, Double>> sortedEntries =
                this.currentSums.entrySet()
                        .stream()
                        .sorted((a, b) -> {
                            int cmp = Double.compare(b.getValue(), a.getValue());
                            return cmp != 0 ? cmp : a.getKey().compareTo(b.getKey());
                        })
                        .toList();

        // Reconstruct a sorted histogram as LinkedHashMap to preserve order
        Map<String, Long> sortedHistogram = produceHistogram(sortedEntries);

        // return a copy to avoid external modification
        // NOTE: made immutable by the HistogramSnapshot constructor to avoid serialization issues
        return new HistogramSnapshotResponse(sortedHistogram);
    }
}
