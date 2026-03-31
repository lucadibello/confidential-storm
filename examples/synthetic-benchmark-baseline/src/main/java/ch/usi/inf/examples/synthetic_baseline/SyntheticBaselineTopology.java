package ch.usi.inf.examples.synthetic_baseline;

import ch.usi.inf.examples.synthetic_baseline.bolts.BaselineContributionBoundingBolt;
import ch.usi.inf.examples.synthetic_baseline.bolts.BaselineDataPerturbationBolt;
import ch.usi.inf.examples.synthetic_baseline.bolts.BaselineHistogramAggregationBolt;
import ch.usi.inf.examples.synthetic_baseline.config.DPConfig;
import ch.usi.inf.examples.synthetic_baseline.profiling.ProfilerConfig;
import ch.usi.inf.examples.synthetic_baseline.spouts.BaselineSpout;
import ch.usi.inf.examples.synthetic_baseline.util.ComponentConstants;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Baseline Synthetic DP Histogram topology (no SGX / Teaclave).
 * <p>
 * Same DP-SQLP pipeline as SyntheticTopology but running entirely on a trusted host.
 * <p>
 * Pipeline: BaselineSpout -> ContributionBounding -> DataPerturbation -> HistogramAggregation
 */
public class SyntheticBaselineTopology {
    private static final Logger LOG = LoggerFactory.getLogger(SyntheticBaselineTopology.class);

    public static TopologyBuilder getTopologyBuilder() {
        int parallelism = DPConfig.parallelism();
        System.out.println("[STARTUP] Using parallelism: " + parallelism);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(
                ComponentConstants.SPOUT,
                new BaselineSpout(),
                1
        );

        builder.setBolt(
                ComponentConstants.BOLT_USER_CONTRIBUTION_BOUNDING,
                new BaselineContributionBoundingBolt(),
                parallelism
        ).fieldsGrouping(
                ComponentConstants.SPOUT,
                new Fields("routingKey")
        );

        builder.setBolt(
                ComponentConstants.BOLT_DATA_PERTURBATION,
                new BaselineDataPerturbationBolt(),
                parallelism
        ).fieldsGrouping(
                ComponentConstants.BOLT_USER_CONTRIBUTION_BOUNDING,
                new Fields("dpRoutingKey")
        );

        builder.setBolt(
                ComponentConstants.BOLT_HISTOGRAM_AGGREGATION,
                new BaselineHistogramAggregationBolt(),
                1
        ).shuffleGrouping(
                ComponentConstants.BOLT_DATA_PERTURBATION
        );

        return builder;
    }

    public static void main(String[] args) throws Exception {
        // Parse CLI arguments
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

        // Apply DP/parallelism args as system properties
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

        if (testMode && !cliArgs.containsKey("parallelism")) {
            System.setProperty("synthetic.parallelism", "1");
        }

        int numUsers = cliArgs.containsKey("num-users") ? Integer.parseInt(cliArgs.get("num-users")) : Integer.getInteger("synthetic.num.users", 10_000_000);
        int numKeys = cliArgs.containsKey("num-keys") ? Integer.parseInt(cliArgs.get("num-keys")) : Integer.getInteger("synthetic.num.keys", 1_000_000);
        long seed = cliArgs.containsKey("seed") ? Long.parseLong(cliArgs.get("seed")) : Long.getLong("synthetic.seed", 42L);
        int runId = cliArgs.containsKey("run-id") ? Integer.parseInt(cliArgs.get("run-id")) : Integer.getInteger("synthetic.run.id", 1);
        int runtimeSeconds = cliArgs.containsKey("runtime-seconds")
                ? Integer.parseInt(cliArgs.get("runtime-seconds"))
                : Integer.getInteger("synthetic.runtime.seconds", 120);
        boolean groundTruth = cliArgs.containsKey("ground-truth")
                ? Boolean.parseBoolean(cliArgs.get("ground-truth"))
                : Boolean.getBoolean("synthetic.ground-truth.enabled");

        LOG.info("=== Synthetic DP Histogram Baseline Topology (No SGX) ===");
        LOG.info("Mode: {}", testMode ? "TEST (parallelism=1)" : "BENCHMARK");
        LOG.info("Run ID: {}", runId);
        LOG.info("Runtime: {} seconds ({} minutes)", runtimeSeconds, runtimeSeconds / 60.0);
        LOG.info("DP Config: {}", DPConfig.describe());
        LOG.info("Tick interval: {}s", Integer.getInteger("dp.tick.interval.secs", 5));
        LOG.info("Profiler: enabled={}, sampleRate={}, reportTicks={}, outputDir={}",
                ProfilerConfig.ENABLED, ProfilerConfig.SAMPLE_RATE,
                ProfilerConfig.REPORT_INTERVAL_TICKS, ProfilerConfig.OUTPUT_DIR);
        LOG.info("=========================================================");

        TopologyBuilder builder = getTopologyBuilder();

        Config conf = new Config();
        conf.setDebug(false);

        conf.put("synthetic.num.users", numUsers);
        conf.put("synthetic.num.keys", numKeys);
        conf.put("synthetic.seed", seed);
        conf.put("synthetic.runtime.seconds", runtimeSeconds);
        conf.put("synthetic.run.id", runId);
        conf.put("synthetic.ground-truth.enabled", String.valueOf(groundTruth));
        conf.put("dp.max.time.steps", DPConfig.maxTimeSteps());
        conf.put("dp.mu", DPConfig.mu());
        conf.put("dp.tick.interval.secs", Integer.getInteger("dp.tick.interval.secs", 5));

        // Enable Storm's built-in metric collection when profiler is active
        if (ProfilerConfig.ENABLED) {
            conf.registerMetricsConsumer(LoggingMetricsConsumer.class, 1);
        }

        LOG.info("Topology Config: numUsers={}, numKeys={}, seed={}", numUsers, numKeys, seed);

        LOG.info("Submitting topology...");
        String topoName = "SyntheticDPBaseline" + runId;
        StormTopology topo = builder.createTopology();
        StormSubmitter.submitTopologyWithProgressBar(topoName, conf, topo);
        LOG.info("Topology '{}' submitted to cluster.", topoName);
    }
}
