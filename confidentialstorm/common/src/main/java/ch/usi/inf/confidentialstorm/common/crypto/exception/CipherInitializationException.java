package ch.usi.inf.confidentialstorm.common.crypto.exception;

import java.io.Serial;

public class CipherInitializationException extends EnclaveCryptoException {
    @Serial
    private static final long serialVersionUID = 1L;

    public CipherInitializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
