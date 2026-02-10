package ch.usi.inf.confidentialstorm.common.api.dp.perturbation.model;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

import java.io.Serial;
import java.util.Objects;

public final class DataPerturbationContributionEntryRequest extends DataPerturbationRequest {

    @Serial
    private static final long serialVersionUID = 1L;

    private final EncryptedValue userId;
    private final EncryptedValue word;
    private final EncryptedValue clampedCount;


    public DataPerturbationContributionEntryRequest(EncryptedValue userId, EncryptedValue word, EncryptedValue clampedCount) {
        if (userId == null || word == null || clampedCount == null) {
            throw new IllegalArgumentException("All fields must be non-null");
        }
        this.userId = userId;
        this.word = word;
        this.clampedCount = clampedCount;
    }

    public EncryptedValue userId() {
        return userId;
    }

    public EncryptedValue word() {
        return word;
    }

    public EncryptedValue clampedCount() {
        return clampedCount;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (DataPerturbationContributionEntryRequest) obj;
        return Objects.equals(this.userId, that.userId) &&
                Objects.equals(this.word, that.word) &&
                Objects.equals(this.clampedCount, that.clampedCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, word, clampedCount);
    }

    @Override
    public String toString() {
        return "DataPerturbationContributionEntryRequest[" +
                "userId=" + userId + ", " +
                "word=" + word + ", " +
                "clampedCount=" + clampedCount + ']';
    }

}
