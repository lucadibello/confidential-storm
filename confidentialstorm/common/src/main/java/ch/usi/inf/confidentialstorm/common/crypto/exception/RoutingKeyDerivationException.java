package ch.usi.inf.confidentialstorm.common.crypto.exception;

import java.io.Serial;

/**
 * Exception thrown when there is an error deriving a routing key.
 */
public class RoutingKeyDerivationException extends EnclaveCryptoException {
    @Serial
    private static final long serialVersionUID = 1L;

    public RoutingKeyDerivationException(String message, Throwable cause) {
        super(message, cause);
    }
}
