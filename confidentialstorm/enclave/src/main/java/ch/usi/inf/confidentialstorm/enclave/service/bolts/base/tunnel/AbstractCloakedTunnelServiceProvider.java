package ch.usi.inf.confidentialstorm.enclave.service.bolts.base.tunnel;

import ch.usi.inf.confidentialstorm.common.api.tunnel.CloakedTunnelService;
import ch.usi.inf.confidentialstorm.common.api.tunnel.model.TunnelBatchRequest;
import ch.usi.inf.confidentialstorm.common.api.tunnel.model.TunnelBatchResponse;
import ch.usi.inf.confidentialstorm.common.api.tunnel.model.TunnelReceiveRequest;
import ch.usi.inf.confidentialstorm.common.api.tunnel.model.TunnelReceiveResponse;
import ch.usi.inf.confidentialstorm.common.api.tunnel.model.TunnelSubmitRequest;
import ch.usi.inf.confidentialstorm.common.api.tunnel.model.TunnelSubmitResponse;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.enclave.service.bolts.ConfidentialBoltService;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Abstract enclave service provider for cloaked tunnel bolts.
 * <p>
 * This service handles both the sender and receiver roles:
 * <ul>
 *   <li><b>Sender side:</b> Buffers incoming tuples via {@link #submitForTransmission},
 *       then packs them into fixed-size batches via {@link #getNextTransmitBatch}.</li>
 *   <li><b>Receiver side:</b> Decrypts batches and extracts real tuples via {@link #receiveBatch}.</li>
 * </ul>
 * <p>
 * Since each bolt instance loads its own enclave, a single concrete subclass can serve
 * both roles. The sender only calls submit+getBatch; the receiver only calls receiveBatch.
 *
 * @see CloakedTunnelService
 */
public abstract class AbstractCloakedTunnelServiceProvider
        extends ConfidentialBoltService<TunnelSubmitRequest>
        implements CloakedTunnelService {

    private final EnclaveLogger log = EnclaveLoggerFactory.getLogger(AbstractCloakedTunnelServiceProvider.class);

    /**
     * Outbound buffer of serialized slot payloads (each is a real slot byte array).
     */
    private final ArrayDeque<byte[]> outboundQueue = new ArrayDeque<>();

    /**
     * Returns the number of slots per batch.
     */
    protected abstract int getBatchSize();

    /**
     * Returns the byte size of each slot.
     */
    protected abstract int getSlotSizeBytes();

    @Override
    public Collection<EncryptedValue> valuesToVerify(TunnelSubmitRequest request) {
        // The tunnel receives fields from the upstream bolt; we verify all of them
        return request.tupleFields();
    }

    @Override
    public synchronized TunnelSubmitResponse submitForTransmission(TunnelSubmitRequest request)
            throws EnclaveServiceException {
        try {
            // Verify the incoming tuple fields (AAD routing + replay protection)
            verify(request);

            // Serialize the fields into a real slot and buffer it
            byte[] slot = CloakedTunnelSlotCodec.encodeRealSlot(request.tupleFields(), getSlotSizeBytes());
            outboundQueue.add(slot);

            log.debug("[CloakedTunnel] Buffered tuple for transmission, queue size: {}", outboundQueue.size());
            return new TunnelSubmitResponse(true);
        } catch (Throwable t) {
            exceptionCtx.handleException(t);
            return new TunnelSubmitResponse(false);
        }
    }

    @Override
    public synchronized TunnelBatchResponse getNextTransmitBatch(TunnelBatchRequest request)
            throws EnclaveServiceException {
        try {
            int batchSize = getBatchSize();
            int slotSize = getSlotSizeBytes();

            // Assemble the batch: drain real slots from the queue, pad with dummies
            byte[] batchBytes = new byte[batchSize * slotSize];
            ByteBuffer batchBuf = ByteBuffer.wrap(batchBytes);

            int realCount = 0;
            for (int i = 0; i < batchSize; i++) {
                byte[] slot = outboundQueue.poll();
                if (slot != null) {
                    batchBuf.put(slot);
                    realCount++;
                } else {
                    batchBuf.put(CloakedTunnelSlotCodec.encodeDummySlot(slotSize));
                }
            }

            log.debug("[CloakedTunnel] Assembled batch for epoch {}: {} real slots, {} dummy slots",
                    request.epochNumber(), realCount, batchSize - realCount);

            // Encrypt the entire batch as a single blob
            int seq = nextSequenceNumber();
            EncryptedValue encryptedBatch = encrypt(batchBytes, seq);

            return new TunnelBatchResponse(encryptedBatch);
        } catch (Throwable t) {
            exceptionCtx.handleException(t);
            return null;
        }
    }

    @Override
    public TunnelReceiveResponse receiveBatch(TunnelReceiveRequest request)
            throws EnclaveServiceException {
        try {
            // Decrypt the batch blob
            byte[] batchBytes = decryptToBytes(request.encryptedBatch());

            int batchSize = getBatchSize();
            int slotSize = getSlotSizeBytes();

            if (batchBytes.length != batchSize * slotSize) {
                throw new EnclaveServiceException(
                        "CloakedTunnel",
                        "Batch size mismatch: expected " + (batchSize * slotSize) +
                                " bytes, got " + batchBytes.length);
            }

            // Unpack each slot, discard dummies
            List<List<EncryptedValue>> realTuples = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                byte[] slot = new byte[slotSize];
                System.arraycopy(batchBytes, i * slotSize, slot, 0, slotSize);

                List<EncryptedValue> fields = CloakedTunnelSlotCodec.decodeSlot(slot, slotSize);
                if (fields != null) {
                    realTuples.add(fields);
                }
            }

            log.debug("[CloakedTunnel] Received batch: {} real tuples extracted", realTuples.size());
            return new TunnelReceiveResponse(realTuples);
        } catch (Throwable t) {
            exceptionCtx.handleException(t);
            return new TunnelReceiveResponse(Collections.emptyList());
        }
    }
}
