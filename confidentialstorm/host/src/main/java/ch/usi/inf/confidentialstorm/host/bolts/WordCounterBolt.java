package ch.usi.inf.confidentialstorm.host.bolts;

import ch.usi.inf.confidentialstorm.common.api.WordCountService;
import ch.usi.inf.confidentialstorm.common.model.WordCountRequest;
import ch.usi.inf.confidentialstorm.host.bolts.base.ConfidentialBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCounterBolt extends ConfidentialBolt<WordCountService> {
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    public WordCounterBolt() {
        super(WordCountService.class);
    }

    @Override
    protected void processTuple(Tuple input, WordCountService service) {
        String word = input.getStringByField("word");
        long count = service.count(new WordCountRequest(word)).count();
        collector.emit(input, new Values(word, count));
        collector.ack(input);
        LOG.info("[WordCounterBolt] Word: {} Current count: {}", word, count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
