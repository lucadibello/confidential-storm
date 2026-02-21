package ch.usi.inf.confidentialstorm.host.bolts.tunnel;

import ch.usi.inf.confidentialstorm.common.api.tunnel.CloakedTunnelSenderService;
import ch.usi.inf.confidentialstorm.common.api.tunnel.model.TunnelBatchRequest;
import ch.usi.inf.confidentialstorm.common.api.tunnel.model.TunnelBatchResponse;
import ch.usi.inf.confidentialstorm.common.api.tunnel.model.TunnelSubmitRequest;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.host.bolts.ConfidentialBolt;
import org.apache.storm.Config;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract sender bolt for a Pacer-style cloaked tunnel.
 * <p>
 * On regular tuples: extracts encrypted fields and submits them to the enclave buffer.
 * On tick tuples: asks the enclave to assemble and encrypt a fixed-size batch, then emits it.
 * <p>
 * Subclasses must implement:
 * <ul>
 *   <li>{@link #extractTupleFields(Tuple)} — extract encrypted fields from the input tuple</li>
 *   <li>{@link #getTickFrequencySecs()} — the epoch interval in seconds</li>
 * </ul>
 */
public abstract class AbstractCloakedTunnelSenderBolt extends ConfidentialBolt<CloakedTunnelSenderService> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractCloakedTunnelSenderBolt.class);

    private long nextEpoch = 0;

    protected AbstractCloakedTunnelSenderBolt() {
        super(CloakedTunnelSenderService.class);
    }

    /**
     * Extracts the encrypted fields from an input tuple for buffering.
     *
     * @param input the input tuple
     * @return the list of encrypted values representing this tuple's fields
     */
    protected abstract List<EncryptedValue> extractTupleFields(Tuple input);

    /**
     * Returns the tick tuple frequency in seconds (epoch interval).
     *
     * @return tick frequency in seconds
     */
    protected abstract int getTickFrequencySecs();

    @Override
    protected void processTuple(Tuple input, CloakedTunnelSenderService service) throws EnclaveServiceException {
        if (isTickTuple(input)) {
            long epoch = nextEpoch++;
            TunnelBatchResponse resp = service.getNextTransmitBatch(new TunnelBatchRequest(epoch));
            if (resp != null && resp.encryptedBatch() != null) {
                getCollector().emit(new Values(resp.encryptedBatch()));
                LOG.debug("[CloakedTunnelSender] Emitted batch for epoch {}", epoch);
            }
        } else {
            List<EncryptedValue> fields = extractTupleFields(input);
            service.submitForTransmission(new TunnelSubmitRequest(fields));
            getCollector().ack(input);
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, getTickFrequencySecs());
        return conf;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("batch"));
    }
}
