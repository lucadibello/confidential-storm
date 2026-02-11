package ch.usi.inf.confidentialstorm.common.api.dp.perturbation.model;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Snapshot of the DP histogram that is encrypted inside the enclave before crossing the trust boundary.
 * The encrypted histogram can only be decrypted by another enclave service (e.g., an aggregation bolt).
 */
public record EncryptedDataPerturbationSnapshot(EncryptedValue encryptedHistogram) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public EncryptedDataPerturbationSnapshot {
        Objects.requireNonNull(encryptedHistogram, "encryptedHistogram cannot be null");
    }
}
