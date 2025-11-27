package ch.usi.inf.confidentialstorm.enclave.exception;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.enclave.EnclaveConfig;
import ch.usi.inf.confidentialstorm.enclave.exception.strategies.IsolateEnclaveExceptionStrategy;
import ch.usi.inf.confidentialstorm.enclave.exception.strategies.PassthroughEnclaveExceptionStrategy;
import ch.usi.inf.confidentialstorm.enclave.exception.strategies.base.IEnclaveExceptionStrategy;

public class EnclaveExceptionContext {
    private IEnclaveExceptionStrategy strategy;
    private static EnclaveExceptionContext instance = null;

    private EnclaveExceptionContext() {
        // Default strategy can be set here
    }

    public static EnclaveExceptionContext getInstance() {
        if (instance == null) {
            instance = new EnclaveExceptionContext();

            // update the instance reference using the correct strategy
            if (EnclaveConfig.ENABLE_EXCEPTION_ISOLATION) {
                // isolate exceptions - log only
                instance.setStrategy(new IsolateEnclaveExceptionStrategy());
            } else {
                // let exceptions reach the untrusted application
                instance.setStrategy(new PassthroughEnclaveExceptionStrategy());
            }
        }
        return instance;
    }

    public void setStrategy(IEnclaveExceptionStrategy strategy) {
        this.strategy = strategy;
    }

    public void handleException(Throwable t) throws EnclaveServiceException {
        // if no strategy is set, throw an exception that would reach the host
        if (strategy == null) {
            throw new EnclaveServiceException(
                    EnclaveExceptionContext.class.getTypeName(),
                    "No strategy set for handling exceptions in EnclaveExceptionContext."
            );
        }
        // otherwise, delegate to the strategy
        strategy.handleException(t);
    }
}
