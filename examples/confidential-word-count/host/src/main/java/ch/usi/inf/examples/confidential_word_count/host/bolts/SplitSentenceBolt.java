package ch.usi.inf.examples.confidential_word_count.host.bolts;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.host.bolts.ConfidentialBolt;
import ch.usi.inf.examples.confidential_word_count.common.api.split.SplitSentenceService;
import ch.usi.inf.examples.confidential_word_count.common.api.split.model.SealedWord;
import ch.usi.inf.examples.confidential_word_count.common.api.split.model.SplitSentenceRequest;
import ch.usi.inf.examples.confidential_word_count.common.api.split.model.SplitSentenceResponse;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SplitSentenceBolt extends ConfidentialBolt<SplitSentenceService> {
    private static final Logger LOG = LoggerFactory.getLogger(SplitSentenceBolt.class);
    private int boltId;

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
    protected void processTuple(Tuple input, SplitSentenceService service) throws EnclaveServiceException {
        // Read input tuple format: (payload, userId)
        EncryptedValue encryptedPayload = (EncryptedValue) input.getValueByField("payload");
        EncryptedValue encryptedUserId = (EncryptedValue) input.getValueByField("userId");
        LOG.debug("[SplitSentenceBolt {}] Received encrypted joke entry", boltId);

        // Request enclave to split the sentence
        SplitSentenceResponse response = service.split(new SplitSentenceRequest(encryptedPayload, encryptedUserId));
        LOG.info("[SplitSentenceBolt {}] Emitting {} encrypted words", boltId, response.words().size());

        // Send out each (word, userId) pair as a separate tuple
        for (SealedWord sealedWord : response.words()) {
            // Emit tuple format: (word, userId)
            getCollector().emit(input, new Values(sealedWord.word(), sealedWord.userId()));
        }
        getCollector().ack(input);
        LOG.debug("[SplitSentenceBolt {}] Acked encrypted joke", boltId);
    }

    @Override
    protected void beforeCleanup() {
        super.beforeCleanup();
        LOG.info("[SplitSentenceBolt {}] Cleaning up.", boltId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Tuple format: (word, userId)
        declarer.declare(new Fields("word", "userId"));
    }
}
