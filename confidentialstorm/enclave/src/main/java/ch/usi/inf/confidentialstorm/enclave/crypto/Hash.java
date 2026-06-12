package ch.usi.inf.confidentialstorm.enclave.crypto;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Utility class for cryptographic hashing.
 */
public class Hash {

    private static final String ALGORITHM = "SHA-256";

    // Reuse MessageDigest per thread to avoid repeated JCA provider lookups.
    // MessageDigest.digest() resets the instance automatically after each call.
    private static final ThreadLocal<MessageDigest> DIGEST = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance(ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    });

    /**
     * Computes a simple SHA-256 hash of the given data.
     *
     * @param data The input data to hash.
     * @return The resulting hash bytes.
     * @throws NoSuchAlgorithmException retained for backward-compatible call sites; in practice
     *         the algorithm is resolved once at thread-local init and not on each call.
     */
    public static byte[] computeHash(byte[] data) throws NoSuchAlgorithmException {
        return DIGEST.get().digest(data);
    }
}
