package ch.usi.inf.confidentialstorm.common.crypto.exception;

import java.io.Serial;

public class AADEncodingException extends EnclaveCryptoException {
    @Serial
    private static final long serialVersionUID = 1L;

    public AADEncodingException(String message, Throwable cause) {
        super(message, cause);
    }
}
