package ch.usi.inf.confidentialstorm.common.model;

import java.util.List;

public record SplitSentenceResponse(List<String> words) {
    public SplitSentenceResponse {
        if (words == null) {
            throw new IllegalArgumentException("Words cannot be null");
        }
        words = List.copyOf(words);
    }
}
