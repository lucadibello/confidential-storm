package ch.usi.inf.confidentialstorm.common.model;

import java.io.Serial;
import java.io.Serializable;

public record SplitSentenceRequest(String body) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public SplitSentenceRequest {
        if (body == null) {
            throw new IllegalArgumentException("Sentence body cannot be null");
        }
    }
}
