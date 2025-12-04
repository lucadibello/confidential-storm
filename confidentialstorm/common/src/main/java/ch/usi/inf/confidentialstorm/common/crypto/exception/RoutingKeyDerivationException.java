package ch.usi.inf.confidentialstorm.common.crypto.exception;

import java.io.Serial;

public class RoutingKeyDerivationException extends EnclaveCryptoException {
    @Serial
    private static final long serialVersionUID = 1L;

    public RoutingKeyDerivationException(String message, Throwable cause) {
        super(message, cause);
    }
}
