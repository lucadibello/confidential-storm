package ch.usi.inf.confidentialstorm.common.model;

import java.io.Serial;
import java.io.Serializable;

public record WordCountResponse(String word, long count) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public WordCountResponse {
        if (word == null || word.isBlank()) {
            throw new IllegalArgumentException("Word cannot be null or blank");
        }
        if (count < 0) {
            throw new IllegalArgumentException("Count cannot be negative");
        }
    }
}
