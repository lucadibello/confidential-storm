package ch.usi.inf.confidentialstorm.enclave.exception.strategies;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.enclave.exception.strategies.base.IEnclaveExceptionStrategy;

/**
 * Exception handling strategy that wraps exceptions in a serializable {@link EnclaveServiceException}
 * to let them reach the host application.
 */
public class PassthroughEnclaveExceptionStrategy implements IEnclaveExceptionStrategy {

    /**
     * Handles the exception by wrapping it and throwing an EnclaveServiceException.
     *
     * @param t the exception to handle
     * @throws EnclaveServiceException always
     */
    @Override
    public void handleException(Throwable t) throws EnclaveServiceException {
        String type = t.getClass().getName();
        String message = t.getMessage();
        StackTraceElement[] enclaveStack = t.getStackTrace();
        throw new EnclaveServiceException("enclave", type, message, enclaveStack);
    }
}
