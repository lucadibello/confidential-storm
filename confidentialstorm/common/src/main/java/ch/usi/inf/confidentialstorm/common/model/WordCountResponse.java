package ch.usi.inf.confidentialstorm.common.model;

public record WordCountResponse(String word, long count) {
    public WordCountResponse {
        if (word == null || word.isBlank()) {
            throw new IllegalArgumentException("Word cannot be null or blank");
        }
        if (count < 0) {
            throw new IllegalArgumentException("Count cannot be negative");
        }
    }
}
