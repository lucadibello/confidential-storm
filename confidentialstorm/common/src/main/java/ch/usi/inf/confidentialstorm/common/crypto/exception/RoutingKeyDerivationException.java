package ch.usi.inf.confidentialstorm.common.crypto.exception;

public class RoutingKeyDerivationException extends EnclaveCryptoException {
    private static final long serialVersionUID = 1L;

    public RoutingKeyDerivationException(String message, Throwable cause) {
        super(message, cause);
    }
}
