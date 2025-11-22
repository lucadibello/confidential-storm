package ch.usi.inf.confidentialstorm.common.crypto.exception;

import java.io.Serializable;

/**
 * Base class for enclave cryptographic errors.
 */
public class EnclaveCryptoException extends RuntimeException implements Serializable {
    private static final long serialVersionUID = 1L;

    public EnclaveCryptoException(String message) {
        super(message);
    }

    public EnclaveCryptoException(String message, Throwable cause) {
        super(message, cause);
    }
}
