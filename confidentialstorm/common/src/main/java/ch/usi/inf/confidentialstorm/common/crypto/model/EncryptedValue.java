package ch.usi.inf.confidentialstorm.common.crypto.model;

import java.io.Serial;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * Container for ChaCha20-Poly1305 sealed data (AAD + nonce + ciphertext).
 */
public record EncryptedValue(byte[] associatedData, byte[] nonce, byte[] ciphertext)
        implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    private static final int NONCE_SIZE = 12;

    public EncryptedValue {
        Objects.requireNonNull(associatedData, "Associated data cannot be null");
        Objects.requireNonNull(nonce, "Nonce cannot be null");
        Objects.requireNonNull(ciphertext, "Ciphertext cannot be null");
        if (nonce.length != NONCE_SIZE) {
            throw new IllegalArgumentException("Nonce must be " + NONCE_SIZE + " bytes");
        }
        if (ciphertext.length == 0) {
            throw new IllegalArgumentException("Ciphertext cannot be empty");
        }

        associatedData = Arrays.copyOf(associatedData, associatedData.length);
        nonce = Arrays.copyOf(nonce, nonce.length);
        ciphertext = Arrays.copyOf(ciphertext, ciphertext.length);
    }

    @Override
    public byte[] associatedData() {
        return Arrays.copyOf(associatedData, associatedData.length);
    }

    @Override
    public byte[] nonce() {
        return Arrays.copyOf(nonce, nonce.length);
    }

    @Override
    public byte[] ciphertext() {
        return Arrays.copyOf(ciphertext, ciphertext.length);
    }

    @Override
    public String toString() {
        return "EncryptedValue{aad=%dB, nonce=%dB, ciphertext=%dB}"
                .formatted(associatedData.length, nonce.length, ciphertext.length);
    }
}