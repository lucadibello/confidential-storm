package ch.usi.inf.confidentialstorm.enclave.util;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Utility class to rethrow exceptions from within an enclave {@link EnclaveServiceException}
 * in order to propagate them outside the enclave boundary while preserving their context.
 *
 * NOTE: this was needed as with the original Teaclave implementation, exceptions would
 * silently fail the ECALL and return a null value (silent failure, no context to debug)
 */
public final class EnclaveExceptionUtil {
    private EnclaveExceptionUtil() { }

    /**
     * Transforms a given Throwable into an EnclaveServiceException to propagate it outside the enclave boundary.
     * @param operation the operation being performed when the exception occurred (for debugging purposes)
     * @param cause the original exception to be wrapped
     * @return an EnclaveServiceException encapsulating the original exception's details
     */
    public static EnclaveServiceException wrap(String operation, Throwable cause) {
        String type = cause.getClass().getName();
        String message = cause.getMessage();
        StackTraceElement[] enclaveStack = cause.getStackTrace();
        return new EnclaveServiceException(operation, type, message, enclaveStack);
    }
}
