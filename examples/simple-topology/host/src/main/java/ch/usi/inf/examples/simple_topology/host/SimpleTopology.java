package ch.usi.inf.examples.simple_topology.host;

import ch.usi.inf.confidentialstorm.host.profiling.ProfilerConfig;
import ch.usi.inf.examples.simple_topology.host.bolts.SimpleBolt;
import ch.usi.inf.examples.simple_topology.host.spouts.RandomNumberSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleTopology {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleTopology.class);

    public static void main(String[] args) throws Exception {
        // Build topology: Spout -> SimpleBolt
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new RandomNumberSpout());
        builder.setBolt("simple-confidential-bolt", new SimpleBolt())
               .globalGrouping("spout");

        // Configure topology
        Config conf = new Config();
        conf.setDebug(false);

        // Enable Storm's built-in metric collection when profiler is active
        if (ProfilerConfig.ENABLED) {
            conf.registerMetricsConsumer(LoggingMetricsConsumer.class, 1);
        }

        // Use MOCK_IN_SVM for development (TEE_SDK for production SGX)
        conf.put("confidentialstorm.enclave.type", "MOCK_IN_SVM");

        // Enclave proxy to detect enclave-side errors (disabled for maximum performance)
        conf.put("confidentialstorm.enclave.proxy.enable", "false");

        int runtimeSeconds = 120;

        // Start local cluster
        try (LocalCluster cluster = new LocalCluster()) {
            StormTopology topo = builder.createTopology();
            cluster.submitTopology("simple-topology", conf, topo);
            Thread.sleep(runtimeSeconds * 1000L);
            cluster.killTopology("simple-topology");
        }
    }
}