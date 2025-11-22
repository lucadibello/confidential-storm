package ch.usi.inf.confidentialstorm.host.exceptions;

/**
 * Wrapper to signal enclave invocation failures
 */
public class EnclaveInvocationFailure extends RuntimeException {
    public EnclaveInvocationFailure(String message) {
        super(message);
    }

    public EnclaveInvocationFailure(String message, Throwable cause) {
        super(message, cause);
    }
}
