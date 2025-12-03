package ch.usi.inf.confidentialstorm.common.api.model;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

import java.io.Serial;
import java.io.Serializable;

public record UserContributionBoundingResponse(EncryptedValue word) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    // If word is null, it means the contribution was dropped.
}
