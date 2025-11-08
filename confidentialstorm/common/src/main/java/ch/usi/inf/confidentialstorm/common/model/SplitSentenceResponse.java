package ch.usi.inf.confidentialstorm.common.model;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public record SplitSentenceResponse(List<String> words) implements Serializable {
    @Serial
    private static final long serialVersionUID = 2L;

    public SplitSentenceResponse {
        if (words == null) {
            throw new IllegalArgumentException("Words cannot be null");
        }
        // NOTE: List.copyOf would return a list whose implementation is not guaranteed to be serializable.
        // Therefore, we return an unmodifiable view over a new array list.
        words = Collections.unmodifiableList(new ArrayList<>(words));
    }
}
