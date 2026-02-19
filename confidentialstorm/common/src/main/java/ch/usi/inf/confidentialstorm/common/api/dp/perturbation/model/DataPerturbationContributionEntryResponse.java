package ch.usi.inf.confidentialstorm.common.api.dp.perturbation.model;

import java.io.Serial;
import java.io.Serializable;

/**
 * Empty response for a data perturbation contribution entry. This is used to indicate that the contribution entry was
 * successfully processed, but there is no additional information to return.
 */
public record DataPerturbationContributionEntryResponse() implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
}
