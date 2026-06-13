package ch.usi.inf.examples.synthetic_dp.host;

import ch.usi.inf.confidentialstorm.common.annotation.ConfidentialTopologyBuilder;
import ch.usi.inf.confidentialstorm.host.profiling.ProfilerConfig;
import ch.usi.inf.examples.synthetic_dp.common.topology.ComponentConstants;
import ch.usi.inf.examples.synthetic_dp.common.config.DPConfig;
import ch.usi.inf.examples.synthetic_dp.host.bolts.SyntheticDataPerturbationBolt;
import ch.usi.inf.examples.synthetic_dp.host.bolts.SyntheticHistogramAggregationBolt;
import ch.usi.inf.examples.synthetic_dp.host.bolts.SyntheticUserContributionBoundingBolt;
import ch.usi.inf.examples.synthetic_dp.host.spouts.SyntheticSpout;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Synthetic DP Histogram (from paper section 5.1)
 * <p>
 * Pipeline: SyntheticSpout -> UserContributionBounding -> DataPerturbation -> HistogramAggregation
 * <p>
 * This topology can be configured via the following system properties:
 * - synthetic.runtime.seconds: runtime in seconds before shutdown (default: 120s)
 * - synthetic.num.users: number of users (default: 10M)
 * - synthetic.num.keys: number of keys (default: 1M)
 * - synthetic.seed: random seed (default: 42)
 * - synthetic.run.id: identifier for output file (default: 1)
 * - dp.max.time.steps: number of micro-batches (default: 100)
 * - dp.mu: threshold for key selection (default: 50)
 * - synthetic.parallelism: parallelism for bounding/perturbation bolts (default: 8)
 * <p>
 * Flags:
 * - --test: test mode (sets parallelism=1 for faster startup)
 */
public class SyntheticTopology {
    private static final Logger LOG = LoggerFactory.getLogger(SyntheticTopology.class);

    @ConfidentialTopologyBuilder
    public static TopologyBuilder getTopologyBuilder() {
        int parallelism = DPConfig.parallelism();
        System.out.println("[STARTUP] Using parallelism: " + parallelism);

        TopologyBuilder builder = new TopologyBuilder();

        // Spout generates synthetic user contributions
        builder.setSpout(
                ComponentConstants.SPOUT.toString(),
                new SyntheticSpout(),
                1
        );

        // Bolt bounds per-user contributions and clamps per-record values
        builder.setBolt(
                ComponentConstants.BOLT_USER_CONTRIBUTION_BOUNDING.toString(),
                new SyntheticUserContributionBoundingBolt(),
                parallelism
        ).fieldsGrouping(
                ComponentConstants.SPOUT.toString(),
                new Fields("routingKey")
        );

        // Bolt applies differential privacy noise via streaming
        // Epoch synchronization is handled via ZooKeeper barriers
        builder.setBolt(
                ComponentConstants.BOLT_DATA_PERTURBATION.toString(),
                new SyntheticDataPerturbationBolt(),
                parallelism
        ).fieldsGrouping(
                ComponentConstants.BOLT_USER_CONTRIBUTION_BOUNDING.toString(),
                new Fields("dpRoutingKey")
        );

        // Bolt merges encrypted histograms and writes reports.
        // Also subscribes to ground-truth stream for utility evaluation.
        builder.setBolt(
                ComponentConstants.BOLT_HISTOGRAM_AGGREGATION.toString(),
                new SyntheticHistogramAggregationBolt(),
                1
        ).shuffleGrouping(
                ComponentConstants.BOLT_DATA_PERTURBATION.toString()
        ).localOrShuffleGrouping(
                ComponentConstants.SPOUT.toString(),
                ComponentConstants.GROUND_TRUTH_STREAM
        );

        return builder;
    }

    public static void main(String[] args) throws Exception {
        // Parse CLI args to set system properties before DPConfig initialization
        Map<String, String> cliArgs = new HashMap<>();
        boolean testMode = false;
        for (int i = 0; i < args.length; i++) {
            if ("--test".equalsIgnoreCase(args[i])) {
                testMode = true;
            } else if (args[i].startsWith("--") && i + 1 < args.length) {
                String key = args[i].substring(2);
                String value = args[i + 1];
                cliArgs.put(key, value);
                i++;
            }
        }

        // Propagate DP/parallelism parameters to system properties
        if (cliArgs.containsKey("mu")) {
            System.setProperty("dp.mu", cliArgs.get("mu"));
        }
        if (cliArgs.containsKey("max-time-steps")) {
            System.setProperty("dp.max.time.steps", cliArgs.get("max-time-steps"));
        }
        if (cliArgs.containsKey("parallelism")) {
            System.setProperty("synthetic.parallelism", cliArgs.get("parallelism"));
        }
        if (cliArgs.containsKey("tick-interval")) {
            System.setProperty("dp.tick.interval.secs", cliArgs.get("tick-interval"));
        }

        // Test mode defaults to parallelism=1
        if (testMode && !cliArgs.containsKey("parallelism")) {
            System.setProperty("synthetic.parallelism", "1");
        }

        int numUsers = cliArgs.containsKey("num-users") ? Integer.parseInt(cliArgs.get("num-users")) : Integer.getInteger("synthetic.num.users", 10_000_000);
        int numKeys = cliArgs.containsKey("num-keys") ? Integer.parseInt(cliArgs.get("num-keys")) : Integer.getInteger("synthetic.num.keys", 1_000_000);
        long seed = cliArgs.containsKey("seed") ? Long.parseLong(cliArgs.get("seed")) : Long.getLong("synthetic.seed", 42L);
        int runId = cliArgs.containsKey("run-id") ? Integer.parseInt(cliArgs.get("run-id")) : Integer.getInteger("synthetic.run.id", 1);
        boolean groundTruth = cliArgs.containsKey("ground-truth")
                ? Boolean.parseBoolean(cliArgs.get("ground-truth"))
                : Boolean.getBoolean("synthetic.ground-truth.enabled");
        String outputDir = cliArgs.getOrDefault("output-dir", System.getProperty("synthetic.output.dir", "/logs/storm/profiler"));

        LOG.info("=== Synthetic DP Histogram Topology ===");
        LOG.info("Mode: {}", testMode ? "TEST (parallelism=1)" : "BENCHMARK");
        LOG.info("Run ID: {}", runId);
        LOG.info("DP Config: {}", DPConfig.describe());
        LOG.info("Tick interval: {}s", Integer.getInteger("dp.tick.interval.secs", 5));
        LOG.info("Profiler: enabled={}, sampleRate={}, reportTicks={}, outputDir={}",
                ProfilerConfig.ENABLED, ProfilerConfig.SAMPLE_RATE,
                ProfilerConfig.REPORT_INTERVAL_TICKS, ProfilerConfig.OUTPUT_DIR);
        LOG.info("=======================================");

        TopologyBuilder builder = getTopologyBuilder();

        Config conf = new Config();
        conf.setDebug(false);

        // Disable enclave proxy for maximum performance
        conf.put("confidentialstorm.enclave.proxy.enable", "false");

        // Populate topology configuration
        conf.put("synthetic.num.users", numUsers);
        conf.put("synthetic.num.keys", numKeys);
        conf.put("synthetic.seed", seed);
        conf.put("synthetic.run.id", runId);
        conf.put("synthetic.output.dir", outputDir);
        conf.put("synthetic.ground-truth.enabled", String.valueOf(groundTruth));
        conf.put("dp.max.time.steps", DPConfig.maxTimeSteps());
        conf.put("dp.mu", DPConfig.mu());
        conf.put("dp.tick.interval.secs", Integer.getInteger("dp.tick.interval.secs", 5));

        // Register metrics consumer if profiling is enabled
        if (ProfilerConfig.ENABLED) {
            conf.registerMetricsConsumer(LoggingMetricsConsumer.class, 1);
        }

        LOG.info("Topology Config: numUsers={}, numKeys={}, seed={}",
                numUsers, numKeys, seed);

        LOG.info("Submitting topology...");
        String topoName = "SyntheticDP" + runId;
        StormTopology topo = builder.createTopology();
        StormSubmitter.submitTopologyWithProgressBar(topoName, conf, topo);
        LOG.info("Topology '{}' submitted to cluster.", topoName);
    }
}
