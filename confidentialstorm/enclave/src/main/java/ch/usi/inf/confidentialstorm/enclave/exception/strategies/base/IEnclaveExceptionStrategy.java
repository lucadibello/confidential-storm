package ch.usi.inf.confidentialstorm.enclave.exception.strategies.base;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;

public interface IEnclaveExceptionStrategy {
    void handleException(Throwable t) throws EnclaveServiceException;
}
