package ch.usi.inf.confidentialstorm.host.bolts.tunnel;

import ch.usi.inf.confidentialstorm.common.api.tunnel.CloakedTunnelReceiverService;
import ch.usi.inf.confidentialstorm.common.api.tunnel.model.TunnelReceiveRequest;
import ch.usi.inf.confidentialstorm.common.api.tunnel.model.TunnelReceiveResponse;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.host.bolts.ConfidentialBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Abstract receiver bolt for a Pacer-style cloaked tunnel.
 * <p>
 * Receives encrypted batch blobs from the sender, passes them to the enclave for decryption,
 * and re-emits the individual real tuples.
 * <p>
 * Subclasses must implement {@link #emitRealTuple(Tuple, List)} to emit the decoded fields.
 */
public abstract class AbstractCloakedTunnelReceiverBolt extends ConfidentialBolt<CloakedTunnelReceiverService> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractCloakedTunnelReceiverBolt.class);

    protected AbstractCloakedTunnelReceiverBolt() {
        super(CloakedTunnelReceiverService.class);
    }

    /**
     * Emits a single real tuple extracted from a decrypted batch.
     *
     * @param anchor the original batch tuple for anchoring
     * @param fields the encrypted fields of the real tuple
     */
    protected abstract void emitRealTuple(Tuple anchor, List<EncryptedValue> fields);

    @Override
    protected void processTuple(Tuple input, CloakedTunnelReceiverService service) throws EnclaveServiceException {
        EncryptedValue encryptedBatch = (EncryptedValue) input.getValueByField("batch");

        TunnelReceiveResponse resp = service.receiveBatch(new TunnelReceiveRequest(encryptedBatch));

        if (resp != null && resp.realTuples() != null) {
            for (List<EncryptedValue> tupleFields : resp.realTuples()) {
                emitRealTuple(input, tupleFields);
            }
            LOG.debug("[CloakedTunnelReceiver] Re-emitted {} real tuples from batch", resp.realTuples().size());
        }

        getCollector().ack(input);
    }
}
