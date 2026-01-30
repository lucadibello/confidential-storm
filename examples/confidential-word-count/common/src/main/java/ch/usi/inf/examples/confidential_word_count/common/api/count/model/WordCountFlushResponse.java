package ch.usi.inf.examples.confidential_word_count.common.api.count.model;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public record WordCountFlushResponse(List<WordCountResponse> histogram) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public WordCountFlushResponse {
        Objects.requireNonNull(histogram, "Histogram cannot be null");
    }
}
