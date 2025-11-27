package ch.usi.inf.confidentialstorm.enclave.exception.strategies;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.enclave.exception.strategies.base.IEnclaveExceptionStrategy;

/**
 * This exception handling strategy wraps any throwable in a serializable EnclaveServiceException
 * in order to let exceptions reach the untrusted applicat
 */
public class PassthroughEnclaveExceptionStrategy implements IEnclaveExceptionStrategy  {

    @Override
    public void handleException(Throwable t) throws EnclaveServiceException {
        String type = t.getClass().getName();
        String message = t.getMessage();
        StackTraceElement[] enclaveStack = t.getStackTrace();
        throw new EnclaveServiceException("enclave", type, message, enclaveStack);
    }
}
