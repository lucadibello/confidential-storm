package ch.usi.inf.confidentialstorm.enclave.util.logger;

import java.io.Serializable;

/**
 * Minimal logger API for enclave code.
 * Similar to SLF4J but restricted to keep TCB small.
 */
public interface EnclaveLogger extends Serializable {
    /**
     * Logs a message at INFO level.
     *
     * @param message the message
     */
    void info(String message);

    /**
     * Logs a message at INFO level with arguments.
     *
     * @param message the message format
     * @param args    the arguments
     */
    void info(String message, Object... args);

    /**
     * Logs a message at WARN level.
     *
     * @param message the message
     */
    void warn(String message);

    /**
     * Logs a message at WARN level with arguments.
     *
     * @param message the message format
     * @param args    the arguments
     */
    void warn(String message, Object... args);

    /**
     * Logs a message at ERROR level.
     *
     * @param message the message
     */
    void error(String message);

    /**
     * Logs a message at ERROR level with a throwable.
     *
     * @param message the message
     * @param t       the throwable
     */
    void error(String message, Throwable t);

    /**
     * Logs a message at ERROR level with arguments.
     *
     * @param message the message format
     * @param args    the arguments
     */
    void error(String message, Object... args);

    /**
     * Logs a message at DEBUG level.
     *
     * @param message the message
     */
    void debug(String message);

    /**
     * Logs a message at DEBUG level with arguments.
     *
     * @param message the message format
     * @param args    the arguments
     */
    void debug(String message, Object... args);
}
