package ch.usi.inf.confidentialstorm.enclave.exception.strategies;

import ch.usi.inf.confidentialstorm.enclave.exception.strategies.base.IEnclaveExceptionStrategy;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLoggerFactory;

/**
 * Exception strategy that isolates exceptions occurring within the enclave, preventing them from affecting the host.
 * This strategy logs the exception details but does not throw any exceptions.
 */
public class IsolateEnclaveExceptionStrategy implements IEnclaveExceptionStrategy {

    /**
     * Logger instance for this class.
     */
    private final EnclaveLogger log = EnclaveLoggerFactory.getLogger(IsolateEnclaveExceptionStrategy.class);

    /**
     * Handles the exception by logging it and suppressing further propagation.
     * <p>
     * We log the exception in debug mode for further analysis.
     *
     * @param t the exception to handle
     */
    @Override
    public void handleException(Throwable t) {
        // show general error without leaking details across the enclave boundary
        log.debug("Ignoring exception due to exception isolation strategy", t);
        // NOTE: we don't throw any exceptions as attackers could infer information by
        // looking at CPU interrupts
    }
}
