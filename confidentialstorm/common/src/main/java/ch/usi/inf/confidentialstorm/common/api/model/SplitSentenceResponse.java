package ch.usi.inf.confidentialstorm.common.api.model;

import ch.usi.inf.confidentialstorm.common.api.model.base.IServiceMessage;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedWord;

import java.io.Serial;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public record SplitSentenceResponse(List<EncryptedWord> words) implements IServiceMessage {
    @Serial
    private static final long serialVersionUID = 2L;

    public SplitSentenceResponse {
        Objects.requireNonNull(words, "Words cannot be null");
        // NOTE: List.copyOf would return a list whose implementation is not guaranteed to be serializable.
        // Therefore, we return an unmodifiable view over a new array list.
        words = Collections.unmodifiableList(new ArrayList<>(words));
    }
}
