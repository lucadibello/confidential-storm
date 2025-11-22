package ch.usi.inf.confidentialstorm.common.crypto.exception;

public class SealedPayloadProcessingException extends EnclaveCryptoException {
    private static final long serialVersionUID = 1L;

    public SealedPayloadProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}
