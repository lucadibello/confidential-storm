package ch.usi.inf.confidentialstorm.enclave.util.logger;

import java.io.Serializable;

/**
 * Minimal logger API for enclave code (API similar to SFL4J)
 */
public interface EnclaveLogger extends Serializable {
    void info(String message);

    void info(String message, Object... args);

    void warn(String message);

    void warn(String message, Object... args);

    void error(String message);

    void error(String message, Throwable t);

    void error(String message, Object... args);

    void debug(String message);

    void debug(String message, Object... args);
}
