package ch.usi.inf.confidentialstorm.host.util;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Objects;

public final class EnclaveErrorUtils {
    private EnclaveErrorUtils() { }

    /**
    * Unwrap common proxy/reflection wrappers to get to the underlying enclave error.
    */
    public static Throwable unwrap(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof InvocationTargetException ite && ite.getTargetException() != null) {
                current = ite.getTargetException();
                continue;
            }
            if (current instanceof UndeclaredThrowableException ute && ute.getUndeclaredThrowable() != null) {
                current = ute.getUndeclaredThrowable();
                continue;
            }
            Throwable cause = current.getCause();
            if (cause != null && cause != current) {
                current = cause;
                continue;
            }
            break;
        }
        return current == null ? throwable : current;
    }

    /**
    * Return string representation of the given throwable representing an exception from the enclave service.
    */
    public static String format(Throwable throwable) {
        Throwable root = unwrap(throwable);
        StringBuilder sb = new StringBuilder();
        sb.append(root.getClass().getName());
        if (root.getMessage() != null) {
            sb.append(": ").append(root.getMessage());
        }
        if (root instanceof EnclaveServiceException ese) {
            sb.append(" | enclaveType=").append(ese.getOriginalType());
            if (ese.getOriginalMessage() != null) {
                sb.append(" enclaveMsg=").append(ese.getOriginalMessage());
            }
        }
        String detail = extractDetailErrorMessage(root);
        if (detail != null && !detail.isBlank()) {
            sb.append(" | detail: ").append(detail);
        }
        return sb.toString();
    }

    private static String extractDetailErrorMessage(Throwable throwable) {
        try {
            Method detailMethod = throwable.getClass().getMethod("getDetailErrorMessage");
            Object value = detailMethod.invoke(throwable);
            return Objects.toString(value, null);
        } catch (ReflectiveOperationException ignored) {
            return null;
        }
    }
}
