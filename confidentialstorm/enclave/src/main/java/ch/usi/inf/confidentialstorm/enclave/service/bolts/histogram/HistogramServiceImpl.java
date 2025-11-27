package ch.usi.inf.confidentialstorm.enclave.service.bolts.histogram;

import ch.usi.inf.confidentialstorm.common.api.HistogramService;
import ch.usi.inf.confidentialstorm.common.api.model.HistogramSnapshotResponse;
import ch.usi.inf.confidentialstorm.common.api.model.HistogramUpdateRequest;
import ch.usi.inf.confidentialstorm.common.crypto.exception.CipherInitializationException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.SealedPayloadProcessingException;
import com.google.auto.service.AutoService;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@AutoService(HistogramService.class)
public class HistogramServiceImpl extends HistogramServiceVerifier {
    private final Map<String, Long> histogram = new HashMap<>();

    @Override
    public void updateImpl(HistogramUpdateRequest update) throws SealedPayloadProcessingException, CipherInitializationException {
        String word = sealedPayload.decryptToString(update.word());
        long count = Long.parseLong(sealedPayload.decryptToString(update.count()));
        histogram.put(word, count);
    }

    @Override
    public HistogramSnapshotResponse snapshot() {
        // Get the entries from the current histogram + sort them by value (bigger first)
        List<Map.Entry<String, Long>> sortedEntries =
                this.histogram.entrySet()
                        .stream()
                        .sorted((a, b) -> {
                            int cmp = Long.compare(b.getValue(), a.getValue());
                            return cmp != 0 ? cmp : a.getKey().compareTo(b.getKey());
                        })
                        .collect(Collectors.toList());

        // Reconstruct a sorted histogram as LinkedHashMap to preserve order
        Map<String, Long> sortedHistogram = new LinkedHashMap<>();
        for (Map.Entry<String, Long> entry : sortedEntries) {
            sortedHistogram.put(entry.getKey(), entry.getValue());
        }

        // return a copy to avoid external modification
        // NOTE: made immutable by the HistogramSnapshot constructor to avoid serialization issues
        return new HistogramSnapshotResponse(sortedHistogram);
    }
}
