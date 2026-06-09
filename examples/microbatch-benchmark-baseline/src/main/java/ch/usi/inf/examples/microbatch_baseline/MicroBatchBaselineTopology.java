package ch.usi.inf.examples.microbatch_baseline;

import ch.usi.inf.examples.microbatch_baseline.bolts.BaselineContributionBoundingBolt;
import ch.usi.inf.examples.microbatch_baseline.bolts.BaselineDataPerturbationBolt;
import ch.usi.inf.examples.microbatch_baseline.bolts.BaselineHistogramAggregationBolt;
import ch.usi.inf.examples.microbatch_baseline.config.DPConfig;
import ch.usi.inf.examples.microbatch_baseline.config.MicroBatchConfig;
import ch.usi.inf.examples.microbatch_baseline.profiling.ProfilerConfig;
import ch.usi.inf.examples.microbatch_baseline.spouts.MicroBatchBaselineSpout;
import ch.usi.inf.examples.microbatch_baseline.util.ComponentConstants;
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
 * Baseline micro-batch DP topology (no SGX / Teaclave).
 *
 * <p>Pipeline:
 * <pre>
 *   MicroBatchBaselineSpout
 *     ├── data    → fieldsGrouping(routingKey) → ContributionBoundingBolt
 *     └── control → allGrouping                → ContributionBoundingBolt
 *                                                 ├── data    → fieldsGrouping(dpRoutingKey) → DataPerturbationBolt
 *                                                 └── control → allGrouping                  → DataPerturbationBolt
 *                                                                                              ├── data    → shuffleGrouping → HistogramAggregationBolt
 *                                                                                              └── control → allGrouping     → HistogramAggregationBolt
 * </pre>
 *
 * <p>The aggregator times every BEGIN→END pair, appends a CSV row per batch,
 * and publishes per-batch completion via ZooKeeper so the spout can advance
 * sequentially.
 */
public class MicroBatchBaselineTopology {
    private static final Logger LOG = LoggerFactory.getLogger(MicroBatchBaselineTopology.class);

    public static TopologyBuilder getTopologyBuilder() {
        int parallelism = DPConfig.parallelism();
        System.out.println("[STARTUP] Using parallelism: " + parallelism);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(
                ComponentConstants.SPOUT,
                new MicroBatchBaselineSpout(),
                1
        );

        builder.setBolt(
                ComponentConstants.BOLT_USER_CONTRIBUTION_BOUNDING,
                new BaselineContributionBoundingBolt(),
                parallelism
        )
                .fieldsGrouping(ComponentConstants.SPOUT, new Fields("routingKey"))
                .allGrouping(ComponentConstants.SPOUT, ComponentConstants.CONTROL_STREAM);

        builder.setBolt(
                ComponentConstants.BOLT_DATA_PERTURBATION,
                new BaselineDataPerturbationBolt(),
                parallelism
        )
                .fieldsGrouping(ComponentConstants.BOLT_USER_CONTRIBUTION_BOUNDING, new Fields("dpRoutingKey"))
                .allGrouping(ComponentConstants.BOLT_USER_CONTRIBUTION_BOUNDING, ComponentConstants.CONTROL_STREAM);

        builder.setBolt(
                ComponentConstants.BOLT_HISTOGRAM_AGGREGATION,
                new BaselineHistogramAggregationBolt(),
                1
        )
                .shuffleGrouping(ComponentConstants.BOLT_DATA_PERTURBATION)
                .allGrouping(ComponentConstants.BOLT_DATA_PERTURBATION, ComponentConstants.CONTROL_STREAM);

        return builder;
    }

    public static void main(String[] args) throws Exception {
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

        if (cliArgs.containsKey("mu"))                System.setProperty("dp.mu", cliArgs.get("mu"));
        if (cliArgs.containsKey("max-time-steps"))    System.setProperty("dp.max.time.steps", cliArgs.get("max-time-steps"));
        if (cliArgs.containsKey("parallelism"))       System.setProperty("synthetic.parallelism", cliArgs.get("parallelism"));
        if (testMode && !cliArgs.containsKey("parallelism")) System.setProperty("synthetic.parallelism", "1");

        int numUsers = cliArgs.containsKey("num-users") ? Integer.parseInt(cliArgs.get("num-users"))
                : Integer.getInteger("synthetic.num.users", 10_000_000);
        int numKeys  = cliArgs.containsKey("num-keys")  ? Integer.parseInt(cliArgs.get("num-keys"))
                : Integer.getInteger("synthetic.num.keys", 1_000_000);
        long seed    = cliArgs.containsKey("seed")      ? Long.parseLong(cliArgs.get("seed"))
                : Long.getLong("synthetic.seed", 42L);
        int runId    = cliArgs.containsKey("run-id")    ? Integer.parseInt(cliArgs.get("run-id"))
                : Integer.getInteger("synthetic.run.id", 1);
        String outputDir = cliArgs.getOrDefault("output-dir",
                System.getProperty("synthetic.output.dir", "/logs/storm/profiler"));

        String sizesGbCsv = cliArgs.getOrDefault("batch-sizes-gb",
                System.getProperty("microbatch.sizes.gb", "1,2,5"));
        int runsPerSize = cliArgs.containsKey("runs-per-size") ? Integer.parseInt(cliArgs.get("runs-per-size"))
                : Integer.getInteger("microbatch.runs.per.size", (int) MicroBatchConfig.DEFAULT_RUNS_PER_SIZE);
        long bytesPerTuple = cliArgs.containsKey("bytes-per-tuple") ? Long.parseLong(cliArgs.get("bytes-per-tuple"))
                : Long.getLong("microbatch.bytes.per.tuple", MicroBatchConfig.DEFAULT_BYTES_PER_TUPLE);
        long endGraceMs = cliArgs.containsKey("end-grace-ms") ? Long.parseLong(cliArgs.get("end-grace-ms"))
                : Long.getLong("microbatch.end.grace.ms", 250L);
        long completionTimeoutMs = cliArgs.containsKey("completion-timeout-ms")
                ? Long.parseLong(cliArgs.get("completion-timeout-ms"))
                : Long.getLong("microbatch.completion.timeout.ms", MicroBatchConfig.DEFAULT_COMPLETION_TIMEOUT_MS);

        LOG.info("=== Micro-batch DP Baseline Topology (No SGX) ===");
        LOG.info("Mode: {}", testMode ? "TEST (parallelism=1)" : "BENCHMARK");
        LOG.info("Run ID: {}", runId);
        LOG.info("DP Config: {}", DPConfig.describe());
        LOG.info("Micro-batch: sizesGb={}, runsPerSize={}, bytesPerTuple={}, endGraceMs={}, completionTimeoutMs={}",
                sizesGbCsv, runsPerSize, bytesPerTuple, endGraceMs, completionTimeoutMs);
        LOG.info("Profiler: enabled={}, sampleRate={}, reportTicks={}, outputDir={}",
                ProfilerConfig.ENABLED, ProfilerConfig.SAMPLE_RATE,
                ProfilerConfig.REPORT_INTERVAL_TICKS, ProfilerConfig.OUTPUT_DIR);
        LOG.info("==================================================");

        TopologyBuilder builder = getTopologyBuilder();

        Config conf = new Config();
        conf.setDebug(false);

        conf.put("synthetic.num.users", numUsers);
        conf.put("synthetic.num.keys", numKeys);
        conf.put("synthetic.seed", seed);
        conf.put("synthetic.run.id", runId);
        conf.put("synthetic.output.dir", outputDir);
        conf.put("dp.max.time.steps", DPConfig.maxTimeSteps());
        conf.put("dp.mu", DPConfig.mu());

        conf.put(MicroBatchConfig.CONF_BATCH_SIZES_GB, sizesGbCsv);
        conf.put(MicroBatchConfig.CONF_RUNS_PER_SIZE, runsPerSize);
        conf.put(MicroBatchConfig.CONF_BYTES_PER_TUPLE, bytesPerTuple);
        conf.put(MicroBatchConfig.CONF_COMPLETION_TIMEOUT_MS, completionTimeoutMs);
        conf.put("microbatch.end.grace.ms", endGraceMs);

        conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 4096.0);

        conf.registerSerialization(java.util.LinkedHashMap.class);
        conf.registerSerialization(java.util.Collections.emptyMap().getClass());

        if (ProfilerConfig.ENABLED) {
            conf.registerMetricsConsumer(LoggingMetricsConsumer.class, 1);
        }

        LOG.info("Topology Config: numUsers={}, numKeys={}, seed={}", numUsers, numKeys, seed);

        String topoName = "MicroBatchBaseline" + runId;
        StormTopology topo = builder.createTopology();
        StormSubmitter.submitTopologyWithProgressBar(topoName, conf, topo);
        LOG.info("Topology '{}' submitted to cluster.", topoName);
    }
}
