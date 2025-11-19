package ch.usi.inf.confidentialstorm.common.api.model;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedWord;

public record SplitSentenceResponse(List<EncryptedWord> words) implements Serializable {
    @Serial
    private static final long serialVersionUID = 2L;

    public SplitSentenceResponse {
        Objects.requireNonNull(words, "Words cannot be null");
        // NOTE: List.copyOf would return a list whose implementation is not guaranteed to be serializable.
        // Therefore, we return an unmodifiable view over a new array list.
        words = Collections.unmodifiableList(new ArrayList<>(words));
    }
}
