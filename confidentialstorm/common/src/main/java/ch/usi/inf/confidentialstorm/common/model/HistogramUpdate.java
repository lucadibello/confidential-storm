package ch.usi.inf.confidentialstorm.common.model;

public record HistogramUpdate(String word, long count) {
    public HistogramUpdate {
        if (word == null || word.isBlank()) {
            throw new IllegalArgumentException("Word cannot be null or blank");
        }
        if (count < 0) {
            throw new IllegalArgumentException("Count cannot be negative");
        }
    }
}
