package ch.usi.inf.confidentialstorm.host.util;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link EnclaveErrorUtils} unwrapping and formatting behavior.
 */
class EnclaveErrorUtilsTest {

    /**
     * Verifies that invocation-target wrappers are unwrapped to the target throwable.
     */
    @Test
    void unwrap_invocationTargetException_returnsTargetException() {
        IllegalArgumentException root = new IllegalArgumentException("bad input");

        Throwable unwrapped = EnclaveErrorUtils.unwrap(new InvocationTargetException(root));

        assertSame(root, unwrapped);
    }

    /**
     * Verifies that undeclared-throwable wrappers are unwrapped to the wrapped throwable.
     */
    @Test
    void unwrap_undeclaredThrowableException_returnsUndeclaredThrowable() {
        IllegalStateException root = new IllegalStateException("enclave failure");

        Throwable unwrapped = EnclaveErrorUtils.unwrap(new UndeclaredThrowableException(root));

        assertSame(root, unwrapped);
    }

    /**
     * Verifies that deep cause chains are unwrapped to the deepest cause.
     */
    @Test
    void unwrap_nestedCauseChain_returnsDeepestCause() {
        RuntimeException root = new RuntimeException("root");
        RuntimeException middle = new RuntimeException("middle", root);
        RuntimeException outer = new RuntimeException("outer", middle);

        Throwable unwrapped = EnclaveErrorUtils.unwrap(outer);

        assertSame(root, unwrapped);
    }

    /**
     * Verifies that formatted enclave exceptions include type and enclave metadata fields.
     */
    @Test
    void format_enclaveServiceException_includesOriginalTypeAndMessage() {
        EnclaveServiceException exception = new EnclaveServiceException("java.lang.IllegalStateException", "boom");

        String formatted = EnclaveErrorUtils.format(exception);

        assertTrue(formatted.contains("EnclaveServiceException"));
        assertTrue(formatted.contains("enclaveType=java.lang.IllegalStateException"));
        assertTrue(formatted.contains("enclaveMsg=boom"));
    }

    /**
     * Verifies that formatted errors include reflective detail messages when available.
     */
    @Test
    void format_exceptionWithDetailMethod_includesDetailSuffix() {
        DetailException exception = new DetailException("oops");

        String formatted = EnclaveErrorUtils.format(exception);

        assertTrue(formatted.contains("detail: precise diagnostic"));
    }

    /**
     * Exception type used to expose getDetailErrorMessage through reflection.
     */
    private static final class DetailException extends RuntimeException {

        /**
         * Creates a detail-bearing exception with a message.
         */
        private DetailException(String message) {
            super(message);
        }

        /**
         * Returns a synthetic detail error message for format testing.
         */
        public String getDetailErrorMessage() {
            return "precise diagnostic";
        }
    }
}
