package ch.usi.inf.confidentialstorm.enclave.crypto;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public final class SealedPayload {
    // FIXME: This is just for development.
    private static final String STREAM_KEY_HEX =
            "a46bf317953bf1a8f71439f74f30cd889ec0aa318f8b6431789fb10d1053d932";
    private static final byte[] STREAM_KEY = HexFormat.of().parseHex(STREAM_KEY_HEX);
    private static final SecretKey ENCRYPTION_KEY = new SecretKeySpec(STREAM_KEY, "ChaCha20");
    private static final SecretKey MAC_KEY = new SecretKeySpec(STREAM_KEY, "HmacSHA256");
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final byte[] EMPTY_AAD = new byte[0];

    private SealedPayload() {
    }

    public static String decryptToString(EncryptedValue sealed) {
        byte[] plaintext = decrypt(sealed);
        return new String(plaintext, StandardCharsets.UTF_8);
    }

    public static byte[] decrypt(EncryptedValue sealed) {
        Objects.requireNonNull(sealed, "Encrypted payload cannot be null");
        Cipher cipher = initCipher(Cipher.DECRYPT_MODE, sealed.nonce(), sealed.associatedData());
        return doFinal(cipher, sealed.ciphertext());
    }

    public static EncryptedValue encryptString(String plaintext, Map<String, Object> aadFields) {
        byte[] data = plaintext.getBytes(StandardCharsets.UTF_8);
        return encrypt(data, aadFields);
    }

    public static EncryptedValue encrypt(byte[] plaintext, Map<String, Object> aadFields) {
        Objects.requireNonNull(plaintext, "Plaintext cannot be null");
        byte[] aad = encodeAad(aadFields);
        byte[] nonce = new byte[12];
        RANDOM.nextBytes(nonce);
        Cipher cipher = initCipher(Cipher.ENCRYPT_MODE, nonce, aad);
        byte[] ciphertext = doFinal(cipher, plaintext);
        return new EncryptedValue(aad, nonce, ciphertext);
    }

    public static String deriveRoutingKey(String value) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(MAC_KEY);
            byte[] digest = mac.doFinal(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Unable to derive routing key", e);
        }
    }

    private static Cipher initCipher(int mode, byte[] nonce, byte[] aad) {
        try {
            Cipher cipher = Cipher.getInstance("ChaCha20-Poly1305");
            IvParameterSpec ivSpec = new IvParameterSpec(nonce);
            cipher.init(mode, ENCRYPTION_KEY, ivSpec);
            if (aad.length > 0) {
                cipher.updateAAD(aad);
            }
            return cipher;
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Unable to initialize cipher", e);
        }
    }

    private static byte[] doFinal(Cipher cipher, byte[] input) {
        try {
            return cipher.doFinal(input);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Unable to process sealed payload", e);
        }
    }

    private static byte[] encodeAad(Map<String, Object> fields) {
        if (fields == null || fields.isEmpty()) {
            return EMPTY_AAD;
        }
        Map<String, Object> sorted = new TreeMap<>(fields);
        StringBuilder builder = new StringBuilder();
        builder.append('{');
        boolean first = true;
        for (Map.Entry<String, Object> entry : sorted.entrySet()) {
            if (!first) {
                builder.append(',');
            }
            first = false;
            builder.append('"').append(escape(entry.getKey())).append('"').append(':');
            builder.append(renderValue(entry.getValue()));
        }
        builder.append('}');
        return builder.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static String renderValue(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }
        return "\"" + escape(value.toString()) + "\"";
    }

    private static String escape(String value) {
        StringBuilder escaped = new StringBuilder(value.length());
        for (char c : value.toCharArray()) {
            if (c == '\\' || c == '"') {
                escaped.append('\\');
            }
            escaped.append(c);
        }
        return escaped.toString();
    }
}
