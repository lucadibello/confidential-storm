package ch.usi.inf.confidentialstorm.common.crypto.exception;

import java.io.Serial;

public class SealedPayloadProcessingException extends EnclaveCryptoException {
    @Serial
    private static final long serialVersionUID = 1L;

    public SealedPayloadProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}
