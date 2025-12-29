package ch.usi.inf.examples.confidential_word_count.host;

import ch.usi.inf.examples.confidential_word_count.common.topology.ComponentConstants;
import ch.usi.inf.examples.confidential_word_count.host.bolts.HistogramBolt;
import ch.usi.inf.examples.confidential_word_count.host.bolts.SplitSentenceBolt;
import ch.usi.inf.examples.confidential_word_count.host.bolts.WordCounterBolt;
import ch.usi.inf.examples.confidential_word_count.host.spouts.RandomJokeSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountTopology extends ConfigurableTopology {
    private static final String PROD_SYSTEM_PROPERTY = "storm.prod";
    private static final String PROD_ENV_VAR = "STORM_PROD";

    public static void main(String[] args) {
        ConfigurableTopology.start(new WordCountTopology(), args);
    }

    @Override
    public int run(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        Logger LOG = LoggerFactory.getLogger(WordCountTopology.class);
        boolean isProd = isProdEnvironment(args);
        LOG.info("Starting WordCountTopology in {} mode", isProd ? "PROD" : "LOCAL");

        // RandomJokeSpout: emits random jokes (json entries with "body" field)
        builder.setSpout(
                ComponentConstants.RANDOM_JOKE_SPOUT.toString(),
                new RandomJokeSpout(),
                1
        );

        // SplitSentenceBolt: splits body into words
        builder.setBolt(
                ComponentConstants.SENTENCE_SPLIT.toString(),
                new SplitSentenceBolt(),
                2
        ).shuffleGrouping(ComponentConstants.RANDOM_JOKE_SPOUT.toString());

        // WordCountBolt: counts the words that are emitted
        builder.setBolt(
                ComponentConstants.WORD_COUNT.toString(),
                new WordCounterBolt(),
                2
        ).shuffleGrouping(
                ComponentConstants.SENTENCE_SPLIT.toString()
        );

        // HistogramBolt: merges partial counters into a single (global) histogram
        builder.setBolt(
                ComponentConstants.HISTOGRAM_GLOBAL.toString(),
                new HistogramBolt(),
                1
        ).globalGrouping(ComponentConstants.WORD_COUNT.toString());

        // configure spout wait strategy to avoid starving other bolts
        // NOTE: learn more here https://storm.apache.org/releases/current/Performance.html
        conf.put(Config.TOPOLOGY_BACKPRESSURE_WAIT_STRATEGY, "org.apache.storm.policy.WaitStrategyProgressive");
        conf.put(Config.TOPOLOGY_BACKPRESSURE_WAIT_PROGRESSIVE_LEVEL1_COUNT, 1); // wait after 1 consecutive empty emit
        conf.put(Config.TOPOLOGY_BACKPRESSURE_WAIT_PROGRESSIVE_LEVEL2_COUNT, 100); // wait after 100 consecutive empty emits
        conf.put(Config.TOPOLOGY_BACKPRESSURE_WAIT_PROGRESSIVE_LEVEL3_SLEEP_MILLIS, 1); // sleep 1 ms at level 3

        // run the topology (locally if not production, otherwise submit to nimbus)
        conf.setDebug(false);
        if (!isProd) {
            // if not in production

            // Mock enclaves - don't use in production
            conf.put("confidentialstorm.enclave.type", "MOCK_IN_SVM");
            // Enable debug for local runs
            conf.setDebug(false);
            // Enable verbose exception propagation for local/debug runs
            System.setProperty("confidentialstorm.debug.exceptions.enabled", "false");
            // Enable enclave service null-check proxy for local runs
            System.setProperty("confidentialstorm.enclave.proxy.enable", "true");
        }
        if (!isProd) {
            LOG.warn("Running in local mode");
            try (LocalCluster cluster = new LocalCluster()) {
                // submit topology to local cluster
                LOG.info("Building topology...");
                StormTopology topo = builder.createTopology();
                LOG.info("Topology built correctly! {}", topo.toString());

                LOG.info("Submitting WordCountTopology to local cluster");
                cluster.submitTopology("WordCountTopology", conf, topo);
                LOG.info("WordCountTopology submitted to local cluster");

                // set upper bound for local execution
                // NOTE: this is needed to avoid local mode to exit immediately. Control the timeout using --local-ttl
                // argument when launching the topology locally (i.e. storm local --local-ttl 150 ...)
                try {
                    Thread.sleep(150_000);
                } catch (Exception exception) {
                    System.out.println("Thread interrupted exception : " + exception);
                    LOG.error("Thread interrupted exception : ", exception);
                }

                // kill topology
                LOG.info("Killing WordCountTopology");
                cluster.killTopology("WordCountTopology");
                LOG.info("WordCountTopology killed");
                return 0;
            } catch (Exception e) {
                LOG.error("Failed to run WordCountTopology in local mode", e);
                return 1;
            }
        } else {
            // submit topology
            return submit("WordCountTopology", conf, builder);
        }
    }

    private boolean isProdEnvironment(String[] args) {
        if (args != null) {
            for (String arg : args) {
                if ("--prod".equalsIgnoreCase(arg) || "--production".equalsIgnoreCase(arg)) {
                    return true;
                }
                if ("--local".equalsIgnoreCase(arg)) {
                    return false;
                }
            }
        }
        String sysProp = System.getProperty(PROD_SYSTEM_PROPERTY);
        if (sysProp != null) {
            return Boolean.parseBoolean(sysProp);
        }
        String envVar = System.getenv(PROD_ENV_VAR);
        if (envVar != null) {
            return Boolean.parseBoolean(envVar);
        }
        return false;
    }
}
