package ch.usi.inf.examples.confidential_word_count.host;

import ch.usi.inf.confidentialstorm.common.annotation.ConfidentialTopologyBuilder;
import ch.usi.inf.confidentialstorm.host.profiling.ProfilerConfig;
import ch.usi.inf.examples.confidential_word_count.common.topology.ComponentConstants;
import ch.usi.inf.examples.confidential_word_count.host.bolts.DataPerturbationBolt;
import ch.usi.inf.examples.confidential_word_count.host.bolts.HistogramAggregatorBolt;
import ch.usi.inf.examples.confidential_word_count.host.bolts.SplitSentenceBolt;
import ch.usi.inf.examples.confidential_word_count.host.bolts.UserContributionBoundingBolt;
import ch.usi.inf.examples.confidential_word_count.host.spouts.RandomJokeSpout;
import org.apache.storm.Config;
import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountTopology extends ConfigurableTopology {

    public static void main(String[] args) {
        ConfigurableTopology.start(new WordCountTopology(), args);
    }

    @Override
    public int run(String[] args) {
        TopologyBuilder builder = getTopologyBuilder();
        Logger LOG = LoggerFactory.getLogger(WordCountTopology.class);

        // Configure spout wait strategy to prevent CPU starvation
        // See: https://storm.apache.org/releases/current/Performance.html
        conf.put(Config.TOPOLOGY_BACKPRESSURE_WAIT_STRATEGY, "org.apache.storm.policy.WaitStrategyProgressive");
        conf.put(Config.TOPOLOGY_BACKPRESSURE_WAIT_PROGRESSIVE_LEVEL1_COUNT, 1); // Wait after 1 consecutive empty emit
        conf.put(Config.TOPOLOGY_BACKPRESSURE_WAIT_PROGRESSIVE_LEVEL2_COUNT, 100); // Wait after 100 consecutive empty emits
        conf.put(Config.TOPOLOGY_BACKPRESSURE_WAIT_PROGRESSIVE_LEVEL3_SLEEP_MILLIS, 1); // Sleep 1 ms at level 3

        conf.setDebug(false);

        // Enable metrics consumer when profiler is active
        if (ProfilerConfig.ENABLED) {
            conf.registerMetricsConsumer(LoggingMetricsConsumer.class, 1);
        }

        LOG.info("Submitting WordCountTopology to cluster...");
        return submit("WordCountTopology", conf, builder);
    }

    @ConfidentialTopologyBuilder
    public static TopologyBuilder getTopologyBuilder() {
        TopologyBuilder builder = new TopologyBuilder();

        // Spout to emit random joke entries
        builder.setSpout(
                ComponentConstants.SPOUT_RANDOM_JOKE.toString(),
                new RandomJokeSpout(),
                1
        );

        // Bolt to split jokes into individual words
        builder.setBolt(
                ComponentConstants.BOLT_SENTENCE_SPLIT.toString(),
                new SplitSentenceBolt(),
                2
        ).shuffleGrouping(ComponentConstants.SPOUT_RANDOM_JOKE.toString());

        // Bolt to bound user contributions for differential privacy (DP)
        builder.setBolt(
                ComponentConstants.BOLT_USER_CONTRIBUTION_BOUNDING.toString(),
                new UserContributionBoundingBolt(),
                2
        ).fieldsGrouping(
                ComponentConstants.BOLT_SENTENCE_SPLIT.toString(),
                new Fields("routingKey")
        );

        // Bolt to compute and perturb the histogram to ensure differential privacy.
        // Uses routing key grouping so that all instances for a key route to the same replica.
        builder.setBolt(
                ComponentConstants.BOLT_DATA_PERTURBATION.toString(),
                new DataPerturbationBolt(),
                2
        ).fieldsGrouping(
                ComponentConstants.BOLT_USER_CONTRIBUTION_BOUNDING.toString(),
                new Fields("dpRoutingKey")
        );

        // Bolt to aggregate encrypted partial histograms and merge them inside the enclave.
        builder.setBolt(
                ComponentConstants.BOLT_HISTOGRAM_AGGREGATION.toString(),
                new HistogramAggregatorBolt(),
                1
        ).shuffleGrouping(
                ComponentConstants.BOLT_DATA_PERTURBATION.toString()
        );

        return builder;
    }
}
