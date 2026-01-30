package ch.usi.inf.examples.confidential_word_count.common.api.count.model;

import java.io.Serial;
import java.io.Serializable;

/**
 * Represents an acknowledgment response for a WordCount operation,
 * indicating that the word was successfully processed and buffered.
 */
public record WordCountAckResponse() implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
}
