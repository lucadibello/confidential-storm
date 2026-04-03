package ch.usi.inf.confidentialstorm.common.api.dp.perturbation.model;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Snapshot of the DP histogram that is encrypted inside the enclave before crossing the trust boundary.
 * The encrypted histogram can only be decrypted by another enclave service (e.g., an aggregation service).
 *
 * @param encryptedHistogram the encrypted histogram snapshot (may be null when {@code ready} is false)
 * @param ready whether the snapshot contains a completed result;
 *              {@code false} indicates the async computation has not finished yet
 */
public record EncryptedDataPerturbationSnapshot(EncryptedValue encryptedHistogram, boolean ready) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new EncryptedDataPerturbationSnapshot.
     *
     * @param encryptedHistogram the encrypted histogram snapshot
     * @param ready whether the snapshot is ready
     * @throws NullPointerException if encryptedHistogram is null when ready is true
     */
    public EncryptedDataPerturbationSnapshot {
        if (ready) {
            Objects.requireNonNull(encryptedHistogram, "encryptedHistogram cannot be null when ready");
        }
    }

    /**
     * Convenience constructor for callers that always produce a ready snapshot.
     */
    public EncryptedDataPerturbationSnapshot(EncryptedValue encryptedHistogram) {
        this(encryptedHistogram, true);
    }

    /**
     * Returns a sentinel snapshot indicating the async computation is not yet complete.
     */
    public static EncryptedDataPerturbationSnapshot notReady() {
        return new EncryptedDataPerturbationSnapshot(null, false);
    }
}
