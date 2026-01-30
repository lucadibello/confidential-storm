package ch.usi.inf.examples.confidential_word_count.common.api.histogram.model;

import java.io.Serial;
import java.io.Serializable;

/**
 * Represents an acknowledgment response for a histogram update operation,
 * indicating that the word was successfully processed and buffered.
 */
public record HistogramUpdateAckResponse() implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
}
