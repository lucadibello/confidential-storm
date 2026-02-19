package ch.usi.inf.confidentialstorm.enclave.exception;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.enclave.EnclaveConfig;
import ch.usi.inf.confidentialstorm.enclave.exception.strategies.IsolateEnclaveExceptionStrategy;
import ch.usi.inf.confidentialstorm.enclave.exception.strategies.PassthroughEnclaveExceptionStrategy;
import ch.usi.inf.confidentialstorm.enclave.exception.strategies.base.IEnclaveExceptionStrategy;

/**
 * Singleton context for handling exceptions within the enclave.
 * It uses a strategy pattern to decide how exceptions should be handled (isolation or passthrough).
 */
public class EnclaveExceptionContext {
    private static EnclaveExceptionContext instance = null;
    private IEnclaveExceptionStrategy strategy;

    private EnclaveExceptionContext() {
        // Default strategy can be set here
    }

    /**
     * Gets the singleton instance of EnclaveExceptionContext.
     *
     * @return the instance
     */
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

    /**
     * Sets the exception handling strategy.
     *
     * @param strategy the strategy to use
     */
    public void setStrategy(IEnclaveExceptionStrategy strategy) {
        this.strategy = strategy;
    }

    /**
     * Handles an exception using the current strategy.
     *
     * @param t the exception to handle
     * @throws EnclaveServiceException if the strategy decides to throw it
     */
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
