package ch.usi.inf.confidentialstorm.common.crypto.exception;

import java.io.Serializable;

/**
 * Serializable exception that wraps exceptions thrown inside an enclave to propagate them to the host application.
 *
 * The exception extracts the original exception type, message, and stack trace for better diagnostics.
 */
public class EnclaveServiceException extends Exception implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String originalType;
    private final String originalMessage;

    public EnclaveServiceException(String operation,
                                   String originalType,
                                   String originalMessage,
                                   StackTraceElement[] enclaveStack) {
        super(buildMessage(operation, originalType, originalMessage));
        this.originalType = originalType;
        this.originalMessage = originalMessage;
        if (enclaveStack != null && enclaveStack.length > 0) {
            // propagate enclave stack to host for better diagnostics
            setStackTrace(enclaveStack);
        }
    }

    public EnclaveServiceException(String originalType, String message) {
        super(message);
        this.originalType = originalType;
        this.originalMessage = message;
    }

    private static String buildMessage(String operation, String type, String msg) {
        StringBuilder builder = new StringBuilder();
        builder.append(operation).append(" failed in enclave with ").append(type);
        if (msg != null && !msg.isBlank()) {
            builder.append(": ").append(msg);
        }
        return builder.toString();
    }

    public String getOriginalType() {
        return originalType;
    }

    public String getOriginalMessage() {
        return originalMessage;
    }
}
