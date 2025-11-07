package ch.usi.inf.confidentialstorm.common.model;

public record WordCountRequest(String word) {
    public WordCountRequest {
        if (word == null || word.isBlank()) {
            throw new IllegalArgumentException("Word cannot be null or blank");
        }
    }
}
