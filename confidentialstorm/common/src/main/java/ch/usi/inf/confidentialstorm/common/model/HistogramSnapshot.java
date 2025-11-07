package ch.usi.inf.confidentialstorm.common.model;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public record HistogramSnapshot(Map<String, Long> counts) {
    public HistogramSnapshot {
        if (counts == null) {
            throw new IllegalArgumentException("Counts cannot be null");
        }
        counts = Collections.unmodifiableMap(new HashMap<>(counts));
    }
}
