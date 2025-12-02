package ch.usi.inf.confidentialstorm.host.bolts;

import ch.usi.inf.confidentialstorm.common.api.UserContributionBoundingService;
import ch.usi.inf.confidentialstorm.common.api.model.UserContributionBoundingRequest;
import ch.usi.inf.confidentialstorm.common.api.model.UserContributionBoundingResponse;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class UserContributionBoundingBolt extends ConfidentialBolt<UserContributionBoundingService> {
    private static final Logger LOG = LoggerFactory.getLogger(UserContributionBoundingBolt.class);
    private int boltId;

    public UserContributionBoundingBolt() {
        super(UserContributionBoundingService.class);
    }

    @Override
    protected void afterPrepare(Map<String, Object> topoConf, TopologyContext context) {
        super.afterPrepare(topoConf, context);
        this.boltId = context.getThisTaskId();
        LOG.info("[UserContributionBoundingBolt {}] Prepared with componentId {}", boltId, context.getThisComponentId());
    }

    @Override
    protected void processTuple(Tuple input, UserContributionBoundingService service) throws EnclaveServiceException {
        // extract routing key and encrypted word from the input tuple
        String routingKey = input.getStringByField("wordKey");
        EncryptedValue word = (EncryptedValue) input.getValueByField("encryptedWord");
        LOG.debug("[UserContributionBoundingBolt {}] Received tuple with routingKey {}", boltId, routingKey);

        // Check contribution limit
        UserContributionBoundingRequest req = new UserContributionBoundingRequest(routingKey, word);
        UserContributionBoundingResponse resp = service.check(req);
        
        if (resp.word() != null) {
            // If authorized, emit
            LOG.info("[UserContributionBoundingBolt {}] Forwarding word {}", boltId, routingKey);
            getCollector().emit(input, new Values(routingKey, resp.word()));
        } else {
            LOG.info("[UserContributionBoundingBolt {}] Dropping word {} (limit exceeded)", boltId, routingKey);
        }
        
        getCollector().ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("wordKey", "encryptedWord"));
    }
}
