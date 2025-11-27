package ch.usi.inf.confidentialstorm.common.api.model;

import ch.usi.inf.confidentialstorm.common.api.model.base.IServiceMessage;

import java.io.Serial;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public record HistogramSnapshotResponse(Map<String, Long> counts) implements IServiceMessage {
    @Serial
    private static final long serialVersionUID = 1L;

    public HistogramSnapshotResponse {
        if (counts == null) {
            throw new IllegalArgumentException("Counts cannot be null");
        }
        counts = Collections.unmodifiableMap(new LinkedHashMap<>(counts));
    }
}
