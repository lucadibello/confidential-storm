package ch.usi.inf.examples.microbatch_dp.host.bolts;

import ch.usi.inf.confidentialstorm.common.api.dp.bounding.UserContributionBoundingService;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.host.bolts.dp.AbstractContributionBoundingBolt;
import ch.usi.inf.examples.microbatch_dp.host.topology.BatchMarker;
import ch.usi.inf.examples.microbatch_dp.common.topology.ComponentConstants;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class MicroBatchUserContributionBoundingBolt extends AbstractContributionBoundingBolt {

    @Override
    protected EncryptedValue getEncryptedPayload(Tuple input) {
        return (EncryptedValue) input.getValueByField("key");
    }

    @Override
    protected EncryptedValue getEncryptedCount(Tuple input) {
        return (EncryptedValue) input.getValueByField("count");
    }

    @Override
    protected EncryptedValue getEncryptedUserId(Tuple input) {
        return (EncryptedValue) input.getValueByField("userId");
    }

    @Override
    protected void processTuple(Tuple input, UserContributionBoundingService service) throws EnclaveServiceException {
        if (ComponentConstants.CONTROL_STREAM.equals(input.getSourceStreamId())) {
            getCollector().emit(ComponentConstants.CONTROL_STREAM, input,
                    new Values(input.getStringByField(BatchMarker.F_TYPE),
                            input.getIntegerByField(BatchMarker.F_BATCH_ID),
                            input.getDoubleByField(BatchMarker.F_SIZE_GB),
                            input.getLongByField(BatchMarker.F_RECORD_COUNT),
                            input.getLongByField(BatchMarker.F_BYTES_PER_TUPLE),
                            input.getLongByField(BatchMarker.F_T_NANOS),
                            input.getLongByField(BatchMarker.F_T_EPOCH_MS)));
            getCollector().ack(input);
            return;
        }
        super.processTuple(input, service);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count", "userId", "dpRoutingKey"));
        declarer.declareStream(ComponentConstants.CONTROL_STREAM, BatchMarker.fields());
    }
}
