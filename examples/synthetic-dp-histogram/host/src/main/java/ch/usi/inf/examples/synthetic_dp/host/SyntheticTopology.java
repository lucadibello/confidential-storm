package ch.usi.inf.examples.synthetic_dp.host;

import ch.usi.inf.examples.synthetic_dp.common.topology.ComponentConstants;
import ch.usi.inf.examples.synthetic_dp.common.config.DPConfig;
import ch.usi.inf.examples.synthetic_dp.host.bolts.SyntheticHistogramBolt;
import ch.usi.inf.examples.synthetic_dp.host.spouts.SyntheticSpout;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Synthetic DP Histogram (from paper section 5.1)
 * <p>
 * This topology can be configured via the following system properties:
 * - synthetic.runtime.seconds: runtime in seconds before shutdown (default: 120s)
 * - synthetic.num.users: number of users (default: 10M)
 * - synthetic.num.keys: number of keys (default: 1M)
 * - synthetic.batch.size: records per emit (default: 20k)
 * - synthetic.sleep.ms: delay between batches (default: 100ms)
 * - synthetic.seed: random seed (default: 42)
 * - synthetic.run.id: identifier for output file (default: 1)
 * - dp.max.time.steps: number of micro-batches (default: 1000, paper uses 100 or 1000)
 * - dp.mu: threshold for key selection (default: 50)
 */
public class SyntheticTopology {
    private static final Logger LOG = LoggerFactory.getLogger(SyntheticTopology.class);

    public static void main(String[] args) throws Exception {
        // Parse command line arguments to override system properties
        Map<String, String> cliArgs = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--") && i + 1 < args.length) {
                String key = args[i].substring(2);
                String value = args[i + 1];
                cliArgs.put(key, value);
                i++; // skip value
            }
        }

        int numUsers = cliArgs.containsKey("num-users") ? Integer.parseInt(cliArgs.get("num-users")) : Integer.getInteger("synthetic.num.users", 10_000_000);
        int numKeys = cliArgs.containsKey("num-keys") ? Integer.parseInt(cliArgs.get("num-keys")) : Integer.getInteger("synthetic.num.keys", 1_000_000);
        int batchSize = cliArgs.containsKey("batch-size") ? Integer.parseInt(cliArgs.get("batch-size")) : Integer.getInteger("synthetic.batch.size", 20_000);
        long sleepMs = cliArgs.containsKey("sleep-ms") ? Long.parseLong(cliArgs.get("sleep-ms")) : Long.getLong("synthetic.sleep.ms", 100L);
        long seed = cliArgs.containsKey("seed") ? Long.parseLong(cliArgs.get("seed")) : Long.getLong("synthetic.seed", 42L);
        // Also support run-id and runtime from args / system properties
        int runId = cliArgs.containsKey("run-id") ? Integer.parseInt(cliArgs.get("run-id")) : Integer.getInteger("synthetic.run.id", 1);
        int runtimeSeconds = cliArgs.containsKey("runtime-seconds")
                ? Integer.parseInt(cliArgs.get("runtime-seconds"))
                : Integer.getInteger("synthetic.runtime.seconds", 120);

        LOG.info("=== Synthetic DP Histogram Topology ===");
        LOG.info("Run ID: {}", runId);
        LOG.info("Runtime: {} seconds ({} minutes)", runtimeSeconds, runtimeSeconds / 60.0);
        LOG.info("DP Config: {}", DPConfig.describe());
        LOG.info("=======================================");

        // build topology: spout -> histogram bolt
        TopologyBuilder builder = new TopologyBuilder();
        // Tick frequency chosen so that roughly `maxTimeSteps` ticks occur during runtime.
        int maxTimeSteps = Integer.getInteger("dp.max.time.steps", 100);
        int tickSeconds = Integer.getInteger(
                "synthetic.tick.seconds",
                Math.max(1, (int) Math.ceil(runtimeSeconds / (double) maxTimeSteps))
        );

        // Ensure runtime covers all expected ticks; pad if needed.
        int minRuntime = tickSeconds * maxTimeSteps + 1;
        if (runtimeSeconds < minRuntime) {
            LOG.warn("runtimeSeconds={} too short for maxTimeSteps={} at tickSeconds={}; extending to {} seconds",
                    runtimeSeconds, maxTimeSteps, tickSeconds, minRuntime);
            runtimeSeconds = minRuntime;
        }

        builder.setSpout(ComponentConstants.SPOUT.toString(), new SyntheticSpout(), 1);
        builder.setBolt(ComponentConstants.HISTOGRAM_GLOBAL.toString(), new SyntheticHistogramBolt(tickSeconds), 1)
                .globalGrouping(ComponentConstants.SPOUT.toString());

        // configure topology
        Config conf = new Config();
        conf.setDebug(false);

        // Enclave proxy to detect enclave-side errors (disabled for max performance)
        conf.put("confidentialstorm.enclave.proxy.enable", "false");

        // Pass synthetic configuration to topology config
        conf.put("synthetic.num.users", numUsers);
        conf.put("synthetic.num.keys", numKeys);
        conf.put("synthetic.batch.size", batchSize);
        conf.put("synthetic.sleep.ms", sleepMs);
        conf.put("synthetic.seed", seed);
        conf.put("synthetic.runtime.seconds", runtimeSeconds);
        conf.put("synthetic.tick.seconds", tickSeconds);
        // Also pass run-id for the Bolt to use
        conf.put("synthetic.run.id", runId); 

        // Pass DP configuration
        long mu = Long.getLong("dp.mu", 50L);
        conf.put("dp.max.time.steps", maxTimeSteps);
        conf.put("dp.mu", mu);

        LOG.info("Topology Config: numUsers={}, numKeys={}, batchSize={}, sleepMs={}, seed={}, maxTimeSteps={}, tickSeconds={}, mu={}",
                numUsers, numKeys, batchSize, sleepMs, seed, maxTimeSteps, tickSeconds, mu);

        // start local cluster
        LOG.info("Starting topology for {} seconds...", runtimeSeconds);
        long startTime = System.currentTimeMillis();
        try (LocalCluster cluster = new LocalCluster()) {
            StormTopology topo = builder.createTopology();
            cluster.submitTopology("SyntheticDP" + runId, conf, topo);
            Thread.sleep(runtimeSeconds * 1000L); // run for specified time
            cluster.killTopology("SyntheticDP" + runId);
        }

        long elapsed = (System.currentTimeMillis() - startTime) / 1000;
        LOG.info("Topology completed after {} seconds", elapsed);
        LOG.info("Results written to: data/synthetic-report-run{}.txt", runId);
    }
}
