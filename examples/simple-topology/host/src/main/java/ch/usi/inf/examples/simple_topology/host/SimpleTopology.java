package ch.usi.inf.examples.simple_topology.host;

import ch.usi.inf.examples.simple_topology.host.bolts.SimpleBolt;
import ch.usi.inf.examples.simple_topology.host.spouts.RandomNumberSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleTopology {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleTopology.class);

    public static void main(String[] args) throws Exception {
        // build topology: spout -> histogram bolt
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new RandomNumberSpout());
        builder.setBolt("simple-confidential-bolt", new SimpleBolt())
               .globalGrouping("spout");

        // configure topology
        Config conf = new Config();
        conf.setDebug(false);

        // Use MOCK_IN_SVM for development (change to TEE_SDK for production SGX)
        conf.put("confidentialstorm.enclave.type", "MOCK_IN_SVM");

        // Enclave proxy to detect enclave-side errors (disabled for max performance)
        conf.put("confidentialstorm.enclave.proxy.enable", "false");

        int runtimeSeconds = 120;

        // start local cluster
        try (LocalCluster cluster = new LocalCluster()) {
            StormTopology topo = builder.createTopology();
            cluster.submitTopology("simple-topology", conf, topo);
            Thread.sleep(runtimeSeconds * 1000L); // run for specified time
            cluster.killTopology("simple-topology");
        }
    }
}