package ch.usi.inf.confidentialstorm.enclave.service.bolts.histogram;

import ch.usi.inf.confidentialstorm.common.api.HistogramService;
import ch.usi.inf.confidentialstorm.common.api.model.HistogramSnapshotResponse;
import ch.usi.inf.confidentialstorm.common.api.model.HistogramUpdateRequest;
import ch.usi.inf.confidentialstorm.enclave.crypto.SealedPayload;
import com.google.auto.service.AutoService;

import java.util.HashMap;
import java.util.Map;

@AutoService(HistogramService.class)
public class HistogramServiceImpl extends HistogramServiceVerifier {
    private final Map<String, Long> histogram = new HashMap<>();

    @Override
    public void updateImpl(HistogramUpdateRequest update) {
        String word = SealedPayload.decryptToString(update.word());
        long count = Long.parseLong(SealedPayload.decryptToString(update.count()));
        histogram.put(word, count);
    }

    @Override
    public HistogramSnapshotResponse snapshot() {
        // return a copy to avoid external modification
        // NOTE: made immutable by the HistogramSnapshot constructor to avoid serialization issues
        return new HistogramSnapshotResponse(histogram);
    }
}
