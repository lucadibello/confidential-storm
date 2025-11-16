package ch.usi.inf.confidentialstorm.host.bolts;

import ch.usi.inf.confidentialstorm.common.api.SplitSentenceService;
import ch.usi.inf.confidentialstorm.common.api.model.SplitSentenceRequest;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedWord;
import ch.usi.inf.confidentialstorm.common.api.model.SplitSentenceResponse;
import ch.usi.inf.confidentialstorm.host.bolts.base.ConfidentialBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SplitSentenceBolt extends ConfidentialBolt<SplitSentenceService> {
    private int boltId;
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    public SplitSentenceBolt() {
        super(SplitSentenceService.class);
    }

    @Override
    protected void afterPrepare(Map<String, Object> topoConf, TopologyContext context) {
        super.afterPrepare(topoConf, context);
        this.boltId = context.getThisTaskId();
        LOG.info("[SplitSentenceBolt {}] Prepared with componentId {}", boltId, context.getThisComponentId());
    }

    @Override
    protected void processTuple(Tuple input, SplitSentenceService service) {
        // read encrypted body
        EncryptedValue encryptedBody = (EncryptedValue) input.getValueByField("body");
        int jokeId = input.getIntegerByField("id");

        // request enclave to split the sentence
        SplitSentenceResponse response = service.split(new SplitSentenceRequest(encryptedBody));
        LOG.info("[SplitSentenceBolt {}] Emitting {} encrypted words for joke {}", boltId, response.words().size(), jokeId);

        // send out each encrypted word
        for (EncryptedWord word : response.words()) {
            collector.emit(input, new Values(word.routingKey(), word.payload()));
        }
        collector.ack(input);
    }

    @Override
    protected void beforeCleanup() {
        super.beforeCleanup();
        LOG.info("[SplitSentenceBolt {}] Cleaning up.", boltId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("wordKey", "encryptedWord"));
    }
}
