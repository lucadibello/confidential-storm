package ch.usi.inf.confidentialstorm.enclave.histogram;

import ch.usi.inf.confidentialstorm.common.api.HistogramService;
import ch.usi.inf.confidentialstorm.common.model.HistogramSnapshot;
import ch.usi.inf.confidentialstorm.common.model.HistogramUpdate;
import com.google.auto.service.AutoService;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@AutoService(HistogramService.class)
public class HistogramServiceImpl implements HistogramService {
    private final ConcurrentMap<String, Long> histogram = new ConcurrentHashMap<>();

    @Override
    public void update(HistogramUpdate update) {
        histogram.put(update.word(), update.count());
    }

    @Override
    public HistogramSnapshot snapshot() {
        return new HistogramSnapshot(new HashMap<>(histogram));
    }
}
