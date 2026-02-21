package ch.usi.inf.confidentialstorm.common.api.tunnel;

import ch.usi.inf.confidentialstorm.common.api.tunnel.model.TunnelBatchRequest;
import ch.usi.inf.confidentialstorm.common.api.tunnel.model.TunnelBatchResponse;
import ch.usi.inf.confidentialstorm.common.api.tunnel.model.TunnelReceiveRequest;
import ch.usi.inf.confidentialstorm.common.api.tunnel.model.TunnelReceiveResponse;
import ch.usi.inf.confidentialstorm.common.api.tunnel.model.TunnelSubmitRequest;
import ch.usi.inf.confidentialstorm.common.api.tunnel.model.TunnelSubmitResponse;
import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;

/**
 * Base interface for Pacer-style cloaked tunnel enclave services.
 * <p>
 * A cloaked tunnel transmits fixed-size encrypted batches on a predictable schedule,
 * making traffic shape independent of secret data. The sender buffers tuples and pads
 * each batch to a fixed size with dummy slots. The receiver decrypts and discards dummies.
 * <p>
 * This interface defines all three operations; concrete sender/receiver service interfaces
 * extend it for separate service discovery via {@code @AutoService}.
 */
public interface CloakedTunnelService {

    /**
     * Submits a tuple's encrypted fields into the outbound buffer for the next batch.
     *
     * @param request the tuple fields to buffer
     * @return response indicating acceptance
     * @throws EnclaveServiceException if an error occurs inside the enclave
     */
    TunnelSubmitResponse submitForTransmission(TunnelSubmitRequest request) throws EnclaveServiceException;

    /**
     * Drains the outbound buffer, pads to the configured batch size with dummy slots,
     * and encrypts the entire batch as a single blob.
     *
     * @param request the batch request containing the epoch number
     * @return response containing the encrypted batch
     * @throws EnclaveServiceException if an error occurs inside the enclave
     */
    TunnelBatchResponse getNextTransmitBatch(TunnelBatchRequest request) throws EnclaveServiceException;

    /**
     * Decrypts a received batch, discards dummy slots, and returns the real tuples.
     *
     * @param request the encrypted batch to unpack
     * @return response containing the real tuple field lists
     * @throws EnclaveServiceException if an error occurs inside the enclave
     */
    TunnelReceiveResponse receiveBatch(TunnelReceiveRequest request) throws EnclaveServiceException;
}
