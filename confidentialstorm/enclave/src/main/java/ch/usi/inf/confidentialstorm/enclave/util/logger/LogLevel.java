package ch.usi.inf.confidentialstorm.enclave.util.logger;

/**
 * Log levels for the enclave logger.
 */
public enum LogLevel {
    /**
     * Debugging information.
     */
    DEBUG(0),
    /**
     * Normal operational messages.
     */
    INFO(1),
    /**
     * Warning messages for potential issues.
     */
    WARN(2),
    /**
     * Error messages for failures.
     */
    ERROR(3);

    private final int priority;

    LogLevel(int priority) {
        this.priority = priority;
    }

    /**
     * Gets the priority of the log level.
     *
     * @return the priority
     */
    public int getPriority() {
        return priority;
    }
}
