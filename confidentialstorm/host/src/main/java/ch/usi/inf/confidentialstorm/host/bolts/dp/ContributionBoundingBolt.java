package ch.usi.inf.confidentialstorm.host.bolts.dp;

import ch.usi.inf.confidentialstorm.common.api.UserContributionBoundingService;
import ch.usi.inf.confidentialstorm.common.api.model.UserContributionBoundingRequest;
import ch.usi.inf.confidentialstorm.common.api.model.UserContributionBoundingResponse;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.host.bolts.ConfidentialBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public abstract class ContributionBoundingBolt extends ConfidentialBolt<UserContributionBoundingService> {
    private static final Logger LOG = LoggerFactory.getLogger(ContributionBoundingBolt.class);
    private int boltId;

    public ContributionBoundingBolt() {
        super(UserContributionBoundingService.class);
    }

    @Override
    protected void afterPrepare(Map<String, Object> topoConf, TopologyContext context) {
        super.afterPrepare(topoConf, context);
        this.boltId = context.getThisTaskId();
        LOG.info("[UserContributionBoundingBolt {}] Prepared with componentId {}", boltId, context.getThisComponentId());
    }

    /**
     * Strategy to extract the encrypted payload (word/key) from the input tuple.
     *
     * @param input The input tuple from which to extract the encrypted value.
     * @return The extracted EncryptedValue.
     */
    protected abstract EncryptedValue getEncryptedPayload(Tuple input);

    /**
     * Strategy to extract the encrypted count from the input tuple.
     *
     * @param input The input tuple from which to extract the encrypted count.
     * @return The extracted EncryptedValue representing the count.
     */
    protected abstract EncryptedValue getEncryptedCount(Tuple input);

    /**
     * Strategy to extract the encrypted user ID from the input tuple.
     *
     * @param input The input tuple from which to extract the encrypted user ID.
     * @return The extracted EncryptedValue representing the user ID.
     */
    protected abstract EncryptedValue getEncryptedUserId(Tuple input);

    @Override
    protected void processTuple(Tuple input, UserContributionBoundingService service) throws EnclaveServiceException {
        // Extract tuple format: (word, count, userId, ...)
        EncryptedValue word = getEncryptedPayload(input);
        EncryptedValue count = getEncryptedCount(input);
        EncryptedValue userId = getEncryptedUserId(input);
        LOG.debug("[UserContributionBoundingBolt {}] Received tuple", boltId);

        // Check contribution limit
        UserContributionBoundingRequest req = new UserContributionBoundingRequest(word, count, userId);
        UserContributionBoundingResponse resp = service.checkAndClamp(req);

        // check if authorized
        if (!resp.isDropped()) {
            // If authorized, emit tuple format: (word, clampedCount, userId)
            LOG.info("[UserContributionBoundingBolt {}] Forwarding word", boltId);
            getCollector().emit(input, new Values(resp.word(), resp.clampedCount(), resp.userId()));
        } else {
            LOG.info("[UserContributionBoundingBolt {}] Dropping word (limit exceeded)", boltId);
        }

        getCollector().ack(input);
    }
}
