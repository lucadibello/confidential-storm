package ch.usi.inf.examples.confidential_word_count.host.bolts;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.host.bolts.ConfidentialBolt;
import ch.usi.inf.examples.confidential_word_count.common.api.WordCountService;
import ch.usi.inf.examples.confidential_word_count.common.api.model.*;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
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
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> config = new HashMap<>();
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 2); // Flush every 2 seconds
        return config;
    }

    private boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    @Override
    protected void processTuple(Tuple input, WordCountService service) throws EnclaveServiceException {
        // check for tick tuple to trigger flush (processing-time)
        if (isTickTuple(input)) {
            LOG.info("[WordCounterBolt {}] Flushing partial histogram...", boltId);
            WordCountFlushResponse response = service.flush(new WordCountFlushRequest());
            int count = 0;
            for (WordCountResponse item : response.histogram()) {
                // emit each word returned by the service
                getCollector().emit(new Values(item.word(), item.count()));
                count++;
            }
            LOG.info("[WordCounterBolt {}] Flushed {} items.", boltId, count);
            return;
        }

        // otherwise ingest normal tuple (event-time)

        // extract encrypted word from the input tuple
        EncryptedValue word = (EncryptedValue) input.getValueByField("encryptedWord");
        LOG.debug("[WordCounterBolt {}] Received tuple", boltId);

        // confidentially count the occurrences of the word
        WordCountRequest req = new WordCountRequest(word);
        WordCountAckResponse ack = service.count(req); // Receive and store the ack
        LOG.debug("[WordCounterBolt {}] Word counted and buffered. Received ack: {}", boltId, ack); // Log the ack

        // acknowledge the tuple
        getCollector().ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}