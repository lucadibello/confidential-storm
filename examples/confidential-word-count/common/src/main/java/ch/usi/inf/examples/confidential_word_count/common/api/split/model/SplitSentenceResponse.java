package ch.usi.inf.examples.confidential_word_count.common.api.split.model;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Response from the SplitSentenceService containing a list of (word, userId) pairs.
 * Each SealedWord contains the encrypted word and its associated encrypted userId.
 *
 * @param words List of sealed words with their userId
 */
public record SplitSentenceResponse(List<SealedWord> words) implements Serializable {
    @Serial
    private static final long serialVersionUID = 3L;

    public SplitSentenceResponse {
        Objects.requireNonNull(words, "Words cannot be null");
        // NOTE: List.copyOf would return a list whose implementation is not guaranteed to be serializable.
        // Therefore, we return an unmodifiable view over a new array list.
        words = Collections.unmodifiableList(new ArrayList<>(words));
    }
}
