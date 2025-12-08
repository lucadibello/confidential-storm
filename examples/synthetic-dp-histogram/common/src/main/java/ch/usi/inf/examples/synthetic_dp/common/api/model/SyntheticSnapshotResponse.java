package ch.usi.inf.examples.synthetic_dp.common.api.model;

import java.io.Serial;
import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public record SyntheticSnapshotResponse(Map<String, Long> counts) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public SyntheticSnapshotResponse {
        if (counts == null) {
            throw new IllegalArgumentException("Counts cannot be null");
        }
        counts = Collections.unmodifiableMap(new LinkedHashMap<>(counts));
    }
}
