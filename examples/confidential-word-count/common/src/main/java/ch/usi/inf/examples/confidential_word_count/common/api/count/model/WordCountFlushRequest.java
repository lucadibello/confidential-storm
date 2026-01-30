package ch.usi.inf.examples.confidential_word_count.common.api.count.model;

import java.io.Serial;
import java.io.Serializable;

public record WordCountFlushRequest() implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
}
