package ch.usi.inf.confidentialstorm.enclave.histogram;

import ch.usi.inf.confidentialstorm.common.api.HistogramService;
import ch.usi.inf.confidentialstorm.common.model.HistogramSnapshotResponse;
import ch.usi.inf.confidentialstorm.common.model.HistogramUpdateRequest;
import com.google.auto.service.AutoService;

import java.util.HashMap;
import java.util.Map;

@AutoService(HistogramService.class)
public class HistogramServiceImpl implements HistogramService {
    private final Map<String, Long> histogram = new HashMap<>();

    @Override
    public void update(HistogramUpdateRequest update) {
        histogram.put(update.word(), update.count());
    }

    @Override
    public HistogramSnapshotResponse snapshot() {
        // return a copy to avoid external modification
        // NOTE: made immutable by the HistogramSnapshot constructor to avoid serialization issues
        return new HistogramSnapshotResponse(histogram);
    }
}
