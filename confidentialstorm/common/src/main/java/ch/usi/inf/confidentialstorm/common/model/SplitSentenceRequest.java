package ch.usi.inf.confidentialstorm.common.model;

public record SplitSentenceRequest(String body) {
    public SplitSentenceRequest {
        if (body == null) {
            throw new IllegalArgumentException("Sentence body cannot be null");
        }
    }
}
