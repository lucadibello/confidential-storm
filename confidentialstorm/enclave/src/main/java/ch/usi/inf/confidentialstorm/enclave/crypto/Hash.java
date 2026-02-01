package ch.usi.inf.confidentialstorm.enclave.crypto;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Hash {

    private static final String ALGORITHM = "SHA-256";

    /**
     * Computes a simple SHA-256 hash of the given data.
     *
     * @param data The input data to hash.
     * @return The resulting hash bytes.
     * @throws NoSuchAlgorithmException If SHA-256 is not available.
     */
    public static byte[] computeHash(byte[] data) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance(ALGORITHM);
        return digest.digest(data);
    }
}