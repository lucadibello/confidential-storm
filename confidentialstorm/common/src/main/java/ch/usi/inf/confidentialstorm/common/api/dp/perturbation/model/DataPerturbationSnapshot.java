package ch.usi.inf.confidentialstorm.common.api.dp.perturbation.model;

import java.io.Serial;
import java.io.Serializable;
import java.util.Map;

public record DataPerturbationSnapshot(Map<String, Long> histogramSnapshot) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
}
