package ch.usi.inf.confidentialstorm.common.api.dp.perturbation.model;

import java.io.Serial;
import java.io.Serializable;
import java.util.Map;

/**
 * Snapshot of the differentially private histogram.
 *
 * @param histogramSnapshot the noisy histogram as a map of word to count
 * @param ready whether the snapshot contains a completed result;
 *              {@code false} indicates the async computation has not finished yet
 */
public record DataPerturbationSnapshot(Map<String, Long> histogramSnapshot, boolean ready) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Convenience constructor for callers that always produce a ready snapshot.
     */
    public DataPerturbationSnapshot(Map<String, Long> histogramSnapshot) {
        this(histogramSnapshot, true);
    }

    /**
     * Returns a sentinel snapshot indicating the async computation is not yet complete.
     */
    public static DataPerturbationSnapshot notReady() {
        return new DataPerturbationSnapshot(Map.of(), false);
    }
}
