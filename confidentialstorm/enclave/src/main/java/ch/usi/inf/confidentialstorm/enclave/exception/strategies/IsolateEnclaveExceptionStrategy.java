package ch.usi.inf.confidentialstorm.enclave.exception.strategies;

import ch.usi.inf.confidentialstorm.enclave.exception.strategies.base.IEnclaveExceptionStrategy;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLoggerFactory;

/**
 * This exception strategy segregates the exceptions occurring within the enclave from affecting the host.
 */
public class IsolateEnclaveExceptionStrategy implements IEnclaveExceptionStrategy {

    /**
     * Logger instance for this class.
     */
    private final EnclaveLogger log = EnclaveLoggerFactory.getLogger(IsolateEnclaveExceptionStrategy.class);

    /**
     * In this strategy, we simply isolate the enclave by not performing any action on exception.
     * <p>
     * We log the exception in debug mode for further analysis.
     */
    @Override
    public void handleException(Throwable t) {
        // show general error without leaking details across the enclave boundary
        log.debug("Ignoring exception due to exception isolation strategy", t);
        // NOTE: we don't throw any exceptions as attackers could infer information by
        // looking at CPU interrupts
    }
}
