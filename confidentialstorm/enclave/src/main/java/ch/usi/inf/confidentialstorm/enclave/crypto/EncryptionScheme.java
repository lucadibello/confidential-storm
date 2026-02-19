package ch.usi.inf.confidentialstorm.enclave.crypto;

/**
 * Encryption schemes supported by {@link SealedPayload}.
 * The scheme is selected at compile/initialization time inside the trusted enclave
 * and cannot be changed at runtime by an untrusted administrator.
 */
public enum EncryptionScheme {
    /**
     * ChaCha20-Poly1305 AEAD cipher (default).
     * 256-bit key, 12-byte nonce.
     */
    CHACHA20_POLY1305("ChaCha20-Poly1305", "ChaCha20", 12),

    /**
     * AES-256-GCM AEAD cipher.
     * 256-bit key, 12-byte nonce, 128-bit authentication tag.
     */
    AES_256_GCM("AES/GCM/NoPadding", "AES", 12),

    /**
     * No encryption (passthrough). Stores plaintext in the ciphertext field
     * with a zero nonce for API compatibility.
     * <p>
     * WARNING: This mode provides NO confidentiality or integrity protection.
     * Use only for benchmarking to isolate encryption overhead.
     */
    NONE(null, null, 12);

    private final String cipherAlgorithm;
    private final String keyAlgorithm;
    private final int nonceSize;

    EncryptionScheme(String cipherAlgorithm, String keyAlgorithm, int nonceSize) {
        this.cipherAlgorithm = cipherAlgorithm;
        this.keyAlgorithm = keyAlgorithm;
        this.nonceSize = nonceSize;
    }

    /**
     * Gets the JCE cipher algorithm string (e.g. "ChaCha20-Poly1305", "AES/GCM/NoPadding").
     *
     * @return the cipher algorithm, or null for {@link #NONE}
     */
    public String getCipherAlgorithm() {
        return cipherAlgorithm;
    }

    /**
     * Gets the JCE key algorithm string (e.g. "ChaCha20", "AES").
     *
     * @return the key algorithm, or null for {@link #NONE}
     */
    public String getKeyAlgorithm() {
        return keyAlgorithm;
    }

    /**
     * Gets the nonce size in bytes.
     *
     * @return the nonce size (12 for all schemes)
     */
    public int getNonceSize() {
        return nonceSize;
    }

    /**
     * Checks whether this scheme actually performs encryption.
     *
     * @return true if encryption is enabled, false for {@link #NONE}
     */
    public boolean isEncryptionEnabled() {
        return cipherAlgorithm != null;
    }
}
