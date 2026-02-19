package ch.usi.inf.confidentialstorm.enclave.exception.strategies.base;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;

/**
 * Strategy interface for handling exceptions within the enclave.
 */
public interface IEnclaveExceptionStrategy {
    /**
     * Handles the given exception.
     *
     * @param t the exception to handle
     * @throws EnclaveServiceException if the strategy decides to propagate the error
     */
    void handleException(Throwable t) throws EnclaveServiceException;
}
