package ch.usi.inf.examples.synthetic_dp.common.api.model;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import java.io.Serial;
import java.io.Serializable;

public record SyntheticEncryptedRecord(
    EncryptedValue key,
    EncryptedValue count,
    EncryptedValue userId
) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public SyntheticEncryptedRecord {
        if (key == null || count == null || userId == null) {
            throw new IllegalArgumentException("Fields cannot be null");
        }
    }
}
