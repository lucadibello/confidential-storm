package ch.usi.inf.confidentialstorm.common.crypto.exception;

public class CipherInitializationException extends EnclaveCryptoException {
    private static final long serialVersionUID = 1L;

    public CipherInitializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
