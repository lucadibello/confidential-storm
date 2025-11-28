package ch.usi.inf.examples.confidential_word_count.host.bolts;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedWord;
import ch.usi.inf.confidentialstorm.host.bolts.ConfidentialBolt;
import ch.usi.inf.examples.confidential_word_count.common.api.SplitSentenceService;
import ch.usi.inf.examples.confidential_word_count.common.api.model.SplitSentenceRequest;
import ch.usi.inf.examples.confidential_word_count.common.api.model.SplitSentenceResponse;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

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
        // read encrypted body
        EncryptedValue encryptedBody = (EncryptedValue) input.getValueByField("body");
        LOG.debug("[SplitSentenceBolt {}] Received encrypted joke payload {}", boltId, encryptedBody);

        // request enclave to split the sentence
        SplitSentenceResponse response = service.split(new SplitSentenceRequest(encryptedBody));
        LOG.info("[SplitSentenceBolt {}] Emitting {} encrypted words for encrypted joke {}", boltId, response.words().size(), encryptedBody);

        // Ensure that the response contains words
        Objects.requireNonNull(response, "SplitSentenceResponse is null. Enclave service has failed.");

        // send out each encrypted word
        for (EncryptedWord word : response.words()) {
            getCollector().emit(input, new Values(word.routingKey(), word.payload()));
        }
        getCollector().ack(input);
        LOG.debug("[SplitSentenceBolt {}] Acked encrypted joke {}", boltId, encryptedBody);
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
