package ch.usi.inf.confidentialstorm.common.crypto.exception;

import java.io.Serial;

/**
 * Serializable exception that wraps exceptions thrown inside an enclave to propagate them to the host application.
 * <p>
 * The exception extracts the original exception type, message, and stack trace for better diagnostics.
 */
public class EnclaveServiceException extends Exception {

    @Serial
    private static final long serialVersionUID = 1L;

    private final String originalType;
    private final String originalMessage;

    /**
     * Constructs a new EnclaveServiceException.
     *
     * @param operation       the name of the operation that failed
     * @param originalType    the type of the original exception
     * @param originalMessage the message of the original exception
     * @param enclaveStack    the stack trace from the enclave
     */
    public EnclaveServiceException(
        String operation,
        String originalType,
        String originalMessage,
        StackTraceElement[] enclaveStack
    ) {
        super(buildMessage(operation, originalType, originalMessage));
        this.originalType = originalType;
        this.originalMessage = originalMessage;
        if (enclaveStack != null && enclaveStack.length > 0) {
            // propagate enclave stack to host for better diagnostics
            setStackTrace(enclaveStack);
        }
    }

    /**
     * Constructs a new EnclaveServiceException with type and message.
     *
     * @param originalType the type of the original exception
     * @param message      the detail message
     */
    public EnclaveServiceException(String originalType, String message) {
        super(message);
        this.originalType = originalType;
        this.originalMessage = message;
    }

    /**
     * Constructs a new EnclaveServiceException with a cause.
     *
     * @param message the detail message
     * @param cause   the cause
     */
    public EnclaveServiceException(String message, Throwable cause) {
        super(message, cause);
        this.originalType = cause.getClass().getName();
        this.originalMessage = cause.getMessage();
    }

    /**
     * Constructs a new EnclaveServiceException with a message.
     *
     * @param message the detail message
     */
    public EnclaveServiceException(String message) {
        super(message);
        this.originalType = "Unknown";
        this.originalMessage = message;
    }

    private static String buildMessage(
        String operation,
        String type,
        String msg
    ) {
        StringBuilder builder = new StringBuilder();
        builder
            .append(operation)
            .append(" failed in enclave with ")
            .append(type);
        if (msg != null && !msg.isBlank()) {
            builder.append(": ").append(msg);
        }
        return builder.toString();
    }

    /**
     * Gets the original exception type.
     *
     * @return the original exception type
     */
    public String getOriginalType() {
        return originalType;
    }

    /**
     * Gets the original exception message.
     *
     * @return the original exception message
     */
    public String getOriginalMessage() {
        return originalMessage;
    }
}
