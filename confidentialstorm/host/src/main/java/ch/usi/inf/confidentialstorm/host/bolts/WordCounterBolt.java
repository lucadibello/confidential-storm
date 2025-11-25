package ch.usi.inf.confidentialstorm.host.bolts;

import ch.usi.inf.confidentialstorm.common.api.WordCountService;
import ch.usi.inf.confidentialstorm.common.api.model.WordCountRequest;
import ch.usi.inf.confidentialstorm.common.api.model.WordCountResponse;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.host.bolts.base.ConfidentialBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

import java.util.Map;

public class WordCounterBolt extends ConfidentialBolt<WordCountService> {
    private static final Logger LOG = LoggerFactory.getLogger(WordCounterBolt.class);
    private int boltId;

    public WordCounterBolt() {
        super(WordCountService.class);
    }

    @Override
    protected void afterPrepare(Map<String, Object> topoConf, TopologyContext context) {
        super.afterPrepare(topoConf, context);
        this.boltId = context.getThisTaskId();
        LOG.info("[WordCounterBolt {}] Prepared with componentId {}", boltId, context.getThisComponentId());
    }

    @Override
    protected void processTuple(Tuple input, WordCountService service) throws EnclaveServiceException {
        // extract routing key and encrypted word from the input tuple
        String routingKey = input.getStringByField("wordKey");
        EncryptedValue word = (EncryptedValue) input.getValueByField("encryptedWord");

        // confidentially count the occurrences of the word
        WordCountRequest req = new WordCountRequest(routingKey, word);
        WordCountResponse resp = service.count(req);
        EncryptedValue count = resp.count();

        // emit the word and its current count
        getCollector().emit(input, new Values(resp.word(), count));
        getCollector().ack(input);
        LOG.info("[WordCounterBolt {}] Word: {} Current count: {}", boltId, resp.word(), count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
