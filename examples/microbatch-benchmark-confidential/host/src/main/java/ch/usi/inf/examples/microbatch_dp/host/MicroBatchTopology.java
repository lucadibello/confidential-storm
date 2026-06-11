package ch.usi.inf.examples.microbatch_dp.host;

import ch.usi.inf.confidentialstorm.common.annotation.ConfidentialTopologyBuilder;
import ch.usi.inf.confidentialstorm.host.profiling.ProfilerConfig;
import ch.usi.inf.examples.microbatch_dp.common.config.DPConfig;
import ch.usi.inf.examples.microbatch_dp.host.config.MicroBatchConfig;
import ch.usi.inf.examples.microbatch_dp.common.topology.ComponentConstants;
import ch.usi.inf.examples.microbatch_dp.host.bolts.MicroBatchDataPerturbationBolt;
import ch.usi.inf.examples.microbatch_dp.host.bolts.MicroBatchHistogramAggregationBolt;
import ch.usi.inf.examples.microbatch_dp.host.bolts.MicroBatchUserContributionBoundingBolt;
import ch.usi.inf.examples.microbatch_dp.host.spouts.MicroBatchSpout;
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
 * Confidential micro-batch DP topology.
 *
 * <p>Pipeline (data + control side-channel):
 * <pre>
 *   MicroBatchSpout
 *     ├── data    → fieldsGrouping(routingKey) → UserContributionBoundingBolt
 *     └── control → allGrouping                → UserContributionBoundingBolt
 *                                                 ├── data    → fieldsGrouping(dpRoutingKey) → DataPerturbationBolt
 *                                                 └── control → allGrouping                  → DataPerturbationBolt
 *                                                                                              ├── data    → shuffleGrouping → HistogramAggregationBolt
 *                                                                                              └── control → allGrouping     → HistogramAggregationBolt
 * </pre>
 *
 * <p>Tick-based DP snapshot release is disabled in micro-batch mode (the DP
 * bolt sets an effectively-infinite tick interval). Timing is anchored on
 * BEGIN/END control markers and recorded by the aggregator, which publishes
 * per-batch completion via ZooKeeper to gate the spout's next batch.
 */
public class MicroBatchTopology {
    private static final Logger LOG = LoggerFactory.getLogger(MicroBatchTopology.class);

    @ConfidentialTopologyBuilder
    public static TopologyBuilder getTopologyBuilder() {
        int parallelism = DPConfig.parallelism();
        System.out.println("[STARTUP] Using parallelism: " + parallelism);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(
                ComponentConstants.SPOUT.toString(),
                new MicroBatchSpout(),
                1
        );

        builder.setBolt(
                ComponentConstants.BOLT_USER_CONTRIBUTION_BOUNDING.toString(),
                new MicroBatchUserContributionBoundingBolt(),
                parallelism
        )
                .fieldsGrouping(ComponentConstants.SPOUT.toString(), new Fields("routingKey"))
                .allGrouping(ComponentConstants.SPOUT.toString(), ComponentConstants.CONTROL_STREAM);

        builder.setBolt(
                ComponentConstants.BOLT_DATA_PERTURBATION.toString(),
                new MicroBatchDataPerturbationBolt(),
                parallelism
        )
                .fieldsGrouping(ComponentConstants.BOLT_USER_CONTRIBUTION_BOUNDING.toString(), new Fields("dpRoutingKey"))
                .allGrouping(ComponentConstants.BOLT_USER_CONTRIBUTION_BOUNDING.toString(), ComponentConstants.CONTROL_STREAM);

        builder.setBolt(
                ComponentConstants.BOLT_HISTOGRAM_AGGREGATION.toString(),
                new MicroBatchHistogramAggregationBolt(),
                1
        )
                .shuffleGrouping(ComponentConstants.BOLT_DATA_PERTURBATION.toString())
                .allGrouping(ComponentConstants.BOLT_DATA_PERTURBATION.toString(), ComponentConstants.CONTROL_STREAM)
                .localOrShuffleGrouping(
                        ComponentConstants.SPOUT.toString(),
                        ComponentConstants.GROUND_TRUTH_STREAM);

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

        if (cliArgs.containsKey("mu"))             System.setProperty("dp.mu", cliArgs.get("mu"));
        if (cliArgs.containsKey("max-time-steps")) System.setProperty("dp.max.time.steps", cliArgs.get("max-time-steps"));
        if (cliArgs.containsKey("parallelism"))    System.setProperty("synthetic.parallelism", cliArgs.get("parallelism"));
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
                : Integer.getInteger("microbatch.runs.per.size", MicroBatchConfig.DEFAULT_RUNS_PER_SIZE);
        long bytesPerTuple = cliArgs.containsKey("bytes-per-tuple") ? Long.parseLong(cliArgs.get("bytes-per-tuple"))
                : Long.getLong("microbatch.bytes.per.tuple", MicroBatchConfig.DEFAULT_BYTES_PER_TUPLE);
        long completionTimeoutMs = cliArgs.containsKey("completion-timeout-ms")
                ? Long.parseLong(cliArgs.get("completion-timeout-ms"))
                : Long.getLong("microbatch.completion.timeout.ms", MicroBatchConfig.DEFAULT_COMPLETION_TIMEOUT_MS);

        LOG.info("=== Confidential Micro-batch DP Topology ===");
        LOG.info("Mode: {}", testMode ? "TEST (parallelism=1)" : "BENCHMARK");
        LOG.info("Run ID: {}", runId);
        LOG.info("DP Config: {}", DPConfig.describe());
        LOG.info("Micro-batch: sizesGb={}, runsPerSize={}, bytesPerTuple={}, completionTimeoutMs={}",
                sizesGbCsv, runsPerSize, bytesPerTuple, completionTimeoutMs);
        LOG.info("Profiler: enabled={}, sampleRate={}, reportTicks={}, outputDir={}",
                ProfilerConfig.ENABLED, ProfilerConfig.SAMPLE_RATE,
                ProfilerConfig.REPORT_INTERVAL_TICKS, ProfilerConfig.OUTPUT_DIR);
        LOG.info("=============================================");

        TopologyBuilder builder = getTopologyBuilder();

        Config conf = new Config();
        conf.setDebug(false);
        conf.put("confidentialstorm.enclave.proxy.enable", "false");

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

        // Micro-batch DP pre-generates the full batch in the spout (two int[]
        // arrays of size ~173M for 5 GB at 31 B/tuple = ~1.4 GB) before the
        // run begins. The default 768 MB worker heap is far too small; mirror
        // the baseline's 8 GB cap so the confidential workers can hold the
        // pre-generated batch plus the in-flight tuples flowing into enclaves.
        // TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB is just the scheduler-side cap and
        // must match (or exceed) the -Xmx in TOPOLOGY_WORKER_CHILDOPTS.
        conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 8192.0);
        conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS,
                "-Xmx8192m -Xms2048m -XX:+UseG1GC -XX:+HeapDumpOnOutOfMemoryError");

        if (ProfilerConfig.ENABLED) {
            conf.registerMetricsConsumer(LoggingMetricsConsumer.class, 1);
        }

        String topoName = "MicroBatchDP" + runId;
        StormTopology topo = builder.createTopology();
        StormSubmitter.submitTopologyWithProgressBar(topoName, conf, topo);
        LOG.info("Topology '{}' submitted to cluster.", topoName);
    }
}
