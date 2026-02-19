package ch.usi.inf.confidentialstorm.enclave.crypto;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.AADSpecification;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.*;

public class SealedPayloadSerializationTest {

    private static final byte[] TEST_KEY = HexFormat.of()
            .parseHex("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f");

    /**
     * This test ensures that special characters in the AAD JSON are properly escaped.
     *
     * @throws Exception if any error occurs during encryption
     */
    @Test
    public void testSealedPayloadSerializationEscaping() throws Exception {
        SealedPayload sealer = SealedPayload.fromConfig();

        String specialString = "Line1\nLine2\tTabbed\"Quoted\"";

        EncryptedValue encrypted = sealer.encryptString("example-data-for-testing",
                AADSpecification.builder()
                        .put("special", specialString)
                        .build()
        );

        byte[] aadBytes = encrypted.associatedData();
        String aadJson = new String(aadBytes, StandardCharsets.UTF_8);

        // Check that it contains escaped characters.
        // We want to check for literal \n, \t, \" in the JSON string.
        // In Java source, \\ represents a single backslash.
        assertTrue(aadJson.contains("\\n"), "Should contain escaped newline");
        assertTrue(aadJson.contains("\\t"), "Should contain escaped tab");
        assertTrue(aadJson.contains("\\\""), "Should contain escaped quote");

        // Check that it does NOT contain actual newline characters
        assertEquals(-1, aadJson.indexOf('\n'), "Should not contain raw newline");
        assertEquals(-1, aadJson.indexOf('\t'), "Should not contain raw tab");
    }

    /**
     * This test ensures that Unicode characters in the AAD JSON are properly escaped.
     *
     * @throws Exception if any error occurs during encryption
     */
    @Test
    public void testSealedPayloadSerializationUnicode() throws Exception {
        SealedPayload sealer = SealedPayload.fromConfig();

        // "Mosé" -> 'é' is a unicode character (é = U+00E9)
        String unicodeString = "Mosé";

        EncryptedValue encrypted = sealer.encryptString("data",
                AADSpecification.builder().put("getName", unicodeString).build());

        String aadJson = new String(encrypted.associatedData(), StandardCharsets.UTF_8);

        // ensure that the Unicode character is escaped in the JSON
        assertTrue(aadJson.contains("\\u00e9"), "Should contain unicode escape for e-acute");
    }

    /**
     * Tests encrypt/decrypt round-trip for all encryption schemes.
     */
    @ParameterizedTest
    @EnumSource(EncryptionScheme.class)
    public void testEncryptDecryptRoundTrip(EncryptionScheme scheme) throws Exception {
        SealedPayload sealer = new SealedPayload(TEST_KEY, scheme);
        String plaintext = "Hello, Confidential Storm!";

        EncryptedValue encrypted = sealer.encryptString(plaintext,
                AADSpecification.builder().put("key", "value").build());
        String decrypted = sealer.decryptToString(encrypted);

        assertEquals(plaintext, decrypted, "Round-trip should preserve plaintext for " + scheme);
    }

    /**
     * Tests that NONE mode stores plaintext directly in the ciphertext field.
     */
    @Test
    public void testNoneModeStoresPlaintext() throws Exception {
        SealedPayload sealer = new SealedPayload(TEST_KEY, EncryptionScheme.NONE);
        String plaintext = "visible-data";

        EncryptedValue encrypted = sealer.encryptString(plaintext, AADSpecification.empty());

        // In NONE mode, ciphertext IS the plaintext
        String stored = new String(encrypted.ciphertext(), StandardCharsets.UTF_8);
        assertEquals(plaintext, stored, "NONE mode should store plaintext in ciphertext field");

        // Nonce should be all zeros
        byte[] expectedNonce = new byte[12];
        assertArrayEquals(expectedNonce, encrypted.nonce(), "NONE mode should use zero nonce");
    }

    /**
     * Tests that AAD is preserved in all encryption modes.
     */
    @ParameterizedTest
    @EnumSource(EncryptionScheme.class)
    public void testAADPreserved(EncryptionScheme scheme) throws Exception {
        SealedPayload sealer = new SealedPayload(TEST_KEY, scheme);
        AADSpecification aadSpec = AADSpecification.builder()
                .put("userId", "user-42")
                .put("streamId", "stream-1")
                .build();

        EncryptedValue encrypted = sealer.encryptString("payload", aadSpec);
        byte[] aadBytes = encrypted.associatedData();
        String aadJson = new String(aadBytes, StandardCharsets.UTF_8);

        assertTrue(aadJson.contains("user-42"), "AAD should contain userId for " + scheme);
        assertTrue(aadJson.contains("stream-1"), "AAD should contain streamId for " + scheme);
    }

    /**
     * Tests that ChaCha20-Poly1305 and AES-256-GCM produce ciphertext different from plaintext.
     */
    @Test
    public void testEncryptedSchemesProduceDifferentCiphertext() throws Exception {
        String plaintext = "sensitive-data";
        byte[] plaintextBytes = plaintext.getBytes(StandardCharsets.UTF_8);

        for (EncryptionScheme scheme : new EncryptionScheme[]{
                EncryptionScheme.CHACHA20_POLY1305, EncryptionScheme.AES_256_GCM}) {
            SealedPayload sealer = new SealedPayload(TEST_KEY, scheme);
            EncryptedValue encrypted = sealer.encryptString(plaintext, AADSpecification.empty());

            // Ciphertext should differ from plaintext (includes auth tag)
            assertFalse(Arrays.equals(plaintextBytes, encrypted.ciphertext()),
                    scheme + " ciphertext should differ from plaintext");
            // AEAD ciphers append an auth tag, so ciphertext is longer
            assertTrue(encrypted.ciphertext().length > plaintextBytes.length,
                    scheme + " ciphertext should be longer than plaintext (auth tag)");
        }
    }

    /**
     * Tests that encrypting with one scheme and decrypting with another fails.
     */
    @Test
    public void testCrossSchemeDecryptionFails() throws Exception {
        SealedPayload chacha = new SealedPayload(TEST_KEY, EncryptionScheme.CHACHA20_POLY1305);
        SealedPayload aesGcm = new SealedPayload(TEST_KEY, EncryptionScheme.AES_256_GCM);

        EncryptedValue encrypted = chacha.encryptString("secret", AADSpecification.empty());

        assertThrows(Exception.class, () -> aesGcm.decrypt(encrypted),
                "Decrypting ChaCha20 ciphertext with AES-GCM should fail");
    }

    /**
     * Tests byte array encrypt/decrypt round-trip.
     */
    @ParameterizedTest
    @EnumSource(EncryptionScheme.class)
    public void testByteArrayRoundTrip(EncryptionScheme scheme) throws Exception {
        SealedPayload sealer = new SealedPayload(TEST_KEY, scheme);
        byte[] data = new byte[]{0x00, 0x01, 0x02, (byte) 0xFF, (byte) 0xFE};

        EncryptedValue encrypted = sealer.encrypt(data, AADSpecification.empty());
        byte[] decrypted = sealer.decrypt(encrypted);

        assertArrayEquals(data, decrypted, "Byte array round-trip should work for " + scheme);
    }
}
