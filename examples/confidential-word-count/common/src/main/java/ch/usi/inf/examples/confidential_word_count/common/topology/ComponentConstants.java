package ch.usi.inf.examples.confidential_word_count.common.topology;

import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;

public final class ComponentConstants {
    public static final TopologySpecification.Component RANDOM_JOKE_SPOUT = TopologySpecification.Component.of("random-joke-spout");
    public static final TopologySpecification.Component SENTENCE_SPLIT = TopologySpecification.Component.of("sentence-split");
    public static final TopologySpecification.Component WORD_COUNT = TopologySpecification.Component.of("word-count");
    public static final TopologySpecification.Component HISTOGRAM_GLOBAL = TopologySpecification.Component.of("histogram-global");
    public static final TopologySpecification.Component DATASET = TopologySpecification.Component.of("_DATASET");
    public static final TopologySpecification.Component MAPPER = TopologySpecification.Component.of("_MAPPER");

    private ComponentConstants() {
    }
}
