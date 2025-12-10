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

public class SyntheticTopology {
    private static final Logger LOG = LoggerFactory.getLogger(SyntheticTopology.class);

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(ComponentConstants.SPOUT.toString(), new SyntheticSpout(), 1);
        builder.setBolt(ComponentConstants.HISTOGRAM_GLOBAL.toString(), new SyntheticHistogramBolt(), 1)
                .globalGrouping(ComponentConstants.SPOUT.toString());

        Config conf = new Config();
        conf.setDebug(false);

        // Enclave proxy to detect enclave-side errors (disabled for max performance)
        conf.put("confidentialstorm.enclave.proxy.enable", "false");

        // start local cluster
        LOG.info("Starting topology for {} seconds...", RUNTIME_SECONDS);
        long startTime = System.currentTimeMillis();
        try (LocalCluster cluster = new LocalCluster()) {
            StormTopology topo = builder.createTopology();
            cluster.submitTopology("SyntheticDP", conf, topo);
            Thread.sleep(60_000);
            cluster.killTopology("SyntheticDP");
        }
    }
}
