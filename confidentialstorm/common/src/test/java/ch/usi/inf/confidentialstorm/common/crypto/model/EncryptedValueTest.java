package ch.usi.inf.confidentialstorm.common.crypto.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link EncryptedValue} validation and defensive-copy behavior.
 */
class EncryptedValueTest {

    /**
     * Verifies that constructor validation rejects null input arrays.
     */
    @Test
    void constructor_nullArrays_throwsNullPointerException() {
        byte[] nonce = new byte[12];
        byte[] ciphertext = new byte[]{1};

        assertThrows(NullPointerException.class, () -> new EncryptedValue(null, nonce, ciphertext));
        assertThrows(NullPointerException.class, () -> new EncryptedValue(new byte[0], null, ciphertext));
        assertThrows(NullPointerException.class, () -> new EncryptedValue(new byte[0], nonce, null));
    }

    /**
     * Verifies that constructor validation enforces the fixed 12-byte nonce size.
     */
    @Test
    void constructor_invalidNonceSize_throwsIllegalArgumentException() {
        byte[] aad = new byte[]{1};
        byte[] ciphertext = new byte[]{2};

        assertThrows(IllegalArgumentException.class, () -> new EncryptedValue(aad, new byte[11], ciphertext));
        assertThrows(IllegalArgumentException.class, () -> new EncryptedValue(aad, new byte[13], ciphertext));
    }

    /**
     * Verifies that constructor validation rejects empty ciphertext.
     */
    @Test
    void constructor_emptyCiphertext_throwsIllegalArgumentException() {
        byte[] aad = new byte[]{1};
        byte[] nonce = new byte[12];

        assertThrows(IllegalArgumentException.class, () -> new EncryptedValue(aad, nonce, new byte[0]));
    }

    /**
     * Verifies that constructor performs defensive copies of all provided byte arrays.
     */
    @Test
    void constructor_copiesInputArrays() {
        byte[] aad = new byte[]{1, 2};
        byte[] nonce = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
        byte[] ciphertext = new byte[]{9, 8, 7};

        EncryptedValue encryptedValue = new EncryptedValue(aad, nonce, ciphertext);

        aad[0] = 99;
        nonce[1] = 88;
        ciphertext[2] = 77;

        assertArrayEquals(new byte[]{1, 2}, encryptedValue.associatedData());
        assertArrayEquals(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, encryptedValue.nonce());
        assertArrayEquals(new byte[]{9, 8, 7}, encryptedValue.ciphertext());
    }

    /**
     * Verifies that accessors return defensive copies rather than exposing mutable internal arrays.
     */
    @Test
    void accessors_returnDefensiveCopies() {
        EncryptedValue encryptedValue = new EncryptedValue(
                new byte[]{1, 2},
                new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
                new byte[]{9, 8, 7}
        );

        byte[] aadView = encryptedValue.associatedData();
        byte[] nonceView = encryptedValue.nonce();
        byte[] ciphertextView = encryptedValue.ciphertext();

        aadView[0] = 42;
        nonceView[0] = 42;
        ciphertextView[0] = 42;

        assertArrayEquals(new byte[]{1, 2}, encryptedValue.associatedData());
        assertArrayEquals(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, encryptedValue.nonce());
        assertArrayEquals(new byte[]{9, 8, 7}, encryptedValue.ciphertext());
    }

    /**
     * Verifies that the diagnostic string includes the expected byte-length metadata.
     */
    @Test
    void toString_containsPayloadSizes() {
        EncryptedValue encryptedValue = new EncryptedValue(
                new byte[]{1, 2, 3},
                new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
                new byte[]{9, 8, 7, 6}
        );

        String text = encryptedValue.toString();

        assertTrue(text.contains("aad=3B"));
        assertTrue(text.contains("nonce=12B"));
        assertTrue(text.contains("ciphertext=4B"));
        assertNotEquals("", text);
    }
}
