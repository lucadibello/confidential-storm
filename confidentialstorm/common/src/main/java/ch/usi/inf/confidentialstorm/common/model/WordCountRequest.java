package ch.usi.inf.confidentialstorm.common.model;

import java.io.Serial;
import java.io.Serializable;

public record WordCountRequest(String word) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public WordCountRequest {
        if (word == null || word.isBlank()) {
            throw new IllegalArgumentException("Word cannot be null or blank");
        }
    }
}
