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

        // SyntheticSpout: generates synthetic user contributions
        builder.setSpout(
                ComponentConstants.SPOUT.toString(),
                new SyntheticSpout(),
                1
        );

        // UserContributionBoundingBolt: bounds per-user contributions and clamps per-record values
        builder.setBolt(
                ComponentConstants.BOLT_USER_CONTRIBUTION_BOUNDING.toString(),
                new SyntheticUserContributionBoundingBolt(),
                parallelism
        ).fieldsGrouping(
                ComponentConstants.SPOUT.toString(),
                new Fields("routingKey")
        );

        // DataPerturbationBolt: applies DP noise via streaming mechanism
        // Epoch synchronization is handled out-of-band via ZooKeeper barriers
        builder.setBolt(
                ComponentConstants.BOLT_DATA_PERTURBATION.toString(),
                new SyntheticDataPerturbationBolt(),
                parallelism
        ).fieldsGrouping(
                ComponentConstants.BOLT_USER_CONTRIBUTION_BOUNDING.toString(),
                new Fields("dpRoutingKey")
        );

        // HistogramAggregationBolt: merges encrypted partial histograms and writes report
        builder.setBolt(
                ComponentConstants.BOLT_HISTOGRAM_AGGREGATION.toString(),
                new SyntheticHistogramAggregationBolt(),
                1
        ).shuffleGrouping(
                ComponentConstants.BOLT_DATA_PERTURBATION.toString()
        );

        return builder;
    }

    public static void main(String[] args) throws Exception {
        // Parse command line arguments to override system properties.
        // NOTE: System.setProperty calls must happen before DPConfig methods are first invoked
        // (i.e., before getTopologyBuilder()), since DPConfig reads properties lazily via getters.
        Map<String, String> cliArgs = new HashMap<>();
        boolean testMode = false;
        for (int i = 0; i < args.length; i++) {
            if ("--test".equalsIgnoreCase(args[i])) {
                testMode = true;
            } else if (args[i].startsWith("--") && i + 1 < args.length) {
                String key = args[i].substring(2);
                String value = args[i + 1];
                cliArgs.put(key, value);
                i++; // skip value
            }
        }

        // Apply DP/parallelism args as system properties so DPConfig picks them up.
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

        // Apply test mode: force parallelism=1 unless already overridden by --parallelism
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

        LOG.info("=== Synthetic DP Histogram Topology ===");
        LOG.info("Mode: {}", testMode ? "TEST (parallelism=1)" : "BENCHMARK");
        LOG.info("Run ID: {}", runId);
        LOG.info("DP Config: {}", DPConfig.describe());
        LOG.info("Tick interval: {}s", Integer.getInteger("dp.tick.interval.secs", 5));
        LOG.info("Profiler: enabled={}, sampleRate={}, reportTicks={}, outputDir={}",
                ProfilerConfig.ENABLED, ProfilerConfig.SAMPLE_RATE,
                ProfilerConfig.REPORT_INTERVAL_TICKS, ProfilerConfig.OUTPUT_DIR);
        LOG.info("=======================================");

        // build topology
        TopologyBuilder builder = getTopologyBuilder();

        // configure topology
        Config conf = new Config();
        conf.setDebug(false);

        // Enclave proxy to detect enclave-side errors (disabled for max performance)
        conf.put("confidentialstorm.enclave.proxy.enable", "false");

        // Pass synthetic configuration to topology config (read by spouts/bolts via TopologyContext)
        conf.put("synthetic.num.users", numUsers);
        conf.put("synthetic.num.keys", numKeys);
        conf.put("synthetic.seed", seed);
        conf.put("synthetic.run.id", runId);
        conf.put("synthetic.ground-truth.enabled", String.valueOf(groundTruth));
        conf.put("dp.max.time.steps", DPConfig.maxTimeSteps());
        conf.put("dp.mu", DPConfig.mu());
        conf.put("dp.tick.interval.secs", Integer.getInteger("dp.tick.interval.secs", 5));

        // Enable Storm's built-in metric collection when profiler is active
        if (ProfilerConfig.ENABLED) {
            conf.registerMetricsConsumer(LoggingMetricsConsumer.class, 1);
        }

        LOG.info("Topology Config: numUsers={}, numKeys={}, seed={}",
                numUsers, numKeys, seed);

        // submit topology to cluster
        LOG.info("Submitting topology...");
        String topoName = "SyntheticDP" + runId;
        StormTopology topo = builder.createTopology();
        StormSubmitter.submitTopologyWithProgressBar(topoName, conf, topo);
        LOG.info("Topology '{}' submitted to cluster.", topoName);
    }
}
