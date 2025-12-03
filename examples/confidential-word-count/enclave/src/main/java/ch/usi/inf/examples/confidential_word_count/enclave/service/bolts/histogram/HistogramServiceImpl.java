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
import java.util.stream.Collectors;

@AutoService(HistogramService.class)
public class HistogramServiceImpl extends HistogramServiceVerifier {
    private final Map<String, BinaryAggregationTree> forest = new HashMap<>();
    private final Map<String, Integer> indices = new HashMap<>();
    private final Map<String, Double> currentSums = new HashMap<>();
    
    private final double sigma;

    public HistogramServiceImpl() {
        // Calibrate noise with user-level sensitivity C * L_m (refer to section 3.2 of the paper)
        double rho = DPUtil.cdpRho(DPConfig.EPSILON, DPConfig.DELTA);
        double l1Sensitivity = DPConfig.l1Sensitivity();
        this.sigma = DPUtil.calculateSigma(rho, DPConfig.MAX_TIME_STEPS, l1Sensitivity);
    }

    @Override
    public void updateImpl(HistogramUpdateRequest update) throws SealedPayloadProcessingException, CipherInitializationException {
        String word = sealedPayload.decryptToString(update.word());
        double count = Double.parseDouble(sealedPayload.decryptToString(update.count()));

        BinaryAggregationTree tree = forest.computeIfAbsent(
                word,
                k -> new BinaryAggregationTree(DPConfig.MAX_TIME_STEPS, sigma)
        );
        int index = indices.getOrDefault(word, 0);

        if (index < DPConfig.MAX_TIME_STEPS) {
            double noisySum = tree.addToTree(index, count);
            currentSums.put(word, noisySum);
            indices.put(word, index + 1);
        }
    }

    @Override
    public HistogramSnapshotResponse snapshot() {
        // Get the entries from the current histogram + sort them by value (bigger first)
        List<Map.Entry<String, Double>> sortedEntries =
                this.currentSums.entrySet()
                        .stream()
                        .sorted((a, b) -> {
                            int cmp = Double.compare(b.getValue(), a.getValue());
                            return cmp != 0 ? cmp : a.getKey().compareTo(b.getKey());
                        })
                        .collect(Collectors.toList());

        // Reconstruct a sorted histogram as LinkedHashMap to preserve order
        Map<String, Long> sortedHistogram = new LinkedHashMap<>();
        for (Map.Entry<String, Double> entry : sortedEntries) {
            sortedHistogram.put(entry.getKey(), Math.round(entry.getValue()));
        }

        // return a copy to avoid external modification
        // NOTE: made immutable by the HistogramSnapshot constructor to avoid serialization issues
        return new HistogramSnapshotResponse(sortedHistogram);
    }
}
