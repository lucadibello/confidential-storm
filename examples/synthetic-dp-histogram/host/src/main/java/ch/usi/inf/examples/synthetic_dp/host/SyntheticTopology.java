package ch.usi.inf.examples.synthetic_dp.host;

import ch.usi.inf.examples.synthetic_dp.common.topology.ComponentConstants;
import ch.usi.inf.examples.synthetic_dp.host.bolts.SyntheticHistogramBolt;
import ch.usi.inf.examples.synthetic_dp.host.spouts.SyntheticSpout;
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

    // Runtime configuration
    private static final int RUNTIME_SECONDS = Integer.getInteger("synthetic.runtime.seconds", 120);
    private static final int RUN_ID = Integer.getInteger("synthetic.run.id", 1);

    public static void main(String[] args) throws Exception {
        LOG.info("=== Synthetic DP Histogram Topology ===");
        LOG.info("Run ID: {}", RUN_ID);
        LOG.info("Runtime: {} seconds ({} minutes)", RUNTIME_SECONDS, RUNTIME_SECONDS / 60.0);
        LOG.info("DP Config: {}", ch.usi.inf.examples.synthetic_dp.common.config.DPConfig.describe());
        LOG.info("=======================================");

        // build topology: spout -> histogram bolt
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(ComponentConstants.SPOUT.toString(), new SyntheticSpout(), 1);
        builder.setBolt(ComponentConstants.HISTOGRAM_GLOBAL.toString(), new SyntheticHistogramBolt(), 1)
                .globalGrouping(ComponentConstants.SPOUT.toString());

        // configure topology
        Config conf = new Config();
        conf.setDebug(false);

        // Enclave proxy to detect enclave-side errors (disabled for max performance)
        conf.put("confidentialstorm.enclave.proxy.enable", "false");

        // start local cluster
        LOG.info("Starting topology for {} seconds...", RUNTIME_SECONDS);
        long startTime = System.currentTimeMillis();
        try (LocalCluster cluster = new LocalCluster()) {
            StormTopology topo = builder.createTopology();
            cluster.submitTopology("SyntheticDP" + RUN_ID, conf, topo);
            Thread.sleep(RUNTIME_SECONDS * 1000L); // run for specified time
            cluster.killTopology("SyntheticDP" + RUN_ID);
        }

        long elapsed = (System.currentTimeMillis() - startTime) / 1000;
        LOG.info("Topology completed after {} seconds", elapsed);
        LOG.info("Results written to: data/synthetic-report-run{}.txt", RUN_ID);
    }
}
