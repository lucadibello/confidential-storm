package ch.usi.inf.examples.microbatch_dp.common.api.model;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

import java.io.Serial;
import java.io.Serializable;

public record MicroBatchEncryptedRecord(EncryptedValue key, EncryptedValue count, EncryptedValue userId, byte[] routingKey) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public MicroBatchEncryptedRecord {
        if (key == null || count == null || userId == null || routingKey == null) {
            throw new IllegalArgumentException("Fields cannot be null");
        }
    }
}
