package ch.usi.inf.confidentialstorm.common.crypto.exception;

import java.io.Serial;
import java.io.Serializable;

/**
 * Base class for enclave cryptographic errors.
 */
public class EnclaveCryptoException extends Exception implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public EnclaveCryptoException(String message, Throwable cause) {
        super(message, cause);
    }
}
