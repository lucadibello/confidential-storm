package ch.usi.inf.examples.simple_topology.host.spouts;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class RandomNumberSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(RandomNumberSpout.class);
    private SpoutOutputCollector collector;

    /**
     * Random number generator for sampling.
     */
    private Random rng;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("num"));
    }

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.rng = new SecureRandom();
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        float num = rng.nextFloat();
        this.collector.emit(List.of(num));
    }
}
