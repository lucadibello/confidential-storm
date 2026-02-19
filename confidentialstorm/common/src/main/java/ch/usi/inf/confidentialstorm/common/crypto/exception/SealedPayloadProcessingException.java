package ch.usi.inf.confidentialstorm.common.crypto.exception;

import java.io.Serial;

/**
 * Exception thrown when there is an error processing a sealed payload (e.g., encryption or decryption failure).
 */
public class SealedPayloadProcessingException extends EnclaveCryptoException {
    @Serial
    private static final long serialVersionUID = 1L;

    public SealedPayloadProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}
