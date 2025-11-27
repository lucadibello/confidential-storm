package ch.usi.inf.confidentialstorm.enclave.crypto;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.crypto.exception.AADEncodingException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.CipherInitializationException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.RoutingKeyDerivationException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.SealedPayloadProcessingException;
import ch.usi.inf.confidentialstorm.enclave.EnclaveConfig;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.AADSpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.DecodedAAD;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLoggerFactory;

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

/**
 * Utility class for sealing and unsealing payloads within the enclave.
 * Uses ChaCha20-Poly1305 for encryption and HMAC-SHA256 for routing key derivation.
 */
public final class SealedPayload {

    private final EnclaveLogger log;
    private final SecretKey encryptionKey;
    private final SecretKey macKey;
    private final SecureRandom random;
    private final byte[] emptyAad = new byte[0];

    private SealedPayload(byte[] streamKey) {
        this.log = EnclaveLoggerFactory.getLogger(SealedPayload.class);
        this.log.info("Initializing SealedPayload");
        this.encryptionKey = new SecretKeySpec(streamKey, "ChaCha20");
        this.macKey = new SecretKeySpec(streamKey, "HmacSHA256");
        this.random = new SecureRandom();
        this.log.info("SealedPayload initialized successfully");
    }

    public static SealedPayload fromConfig() {
        return new SealedPayload(HexFormat.of().parseHex(EnclaveConfig.STREAM_KEY_HEX));
    }

    public byte[] decrypt(EncryptedValue sealed) throws SealedPayloadProcessingException, CipherInitializationException {
        Objects.requireNonNull(sealed, "Encrypted payload cannot be null");
        Cipher cipher = initCipher(Cipher.DECRYPT_MODE, sealed.nonce(), sealed.associatedData());
        return doFinal(cipher, sealed.ciphertext());
    }

    public String decryptToString(EncryptedValue sealed) throws SealedPayloadProcessingException, CipherInitializationException {
        byte[] plaintext = decrypt(sealed);
        return new String(plaintext, StandardCharsets.UTF_8);
    }

    public EncryptedValue encryptString(String plaintext, AADSpecification aadSpec) throws SealedPayloadProcessingException, AADEncodingException, CipherInitializationException {
        byte[] data = plaintext.getBytes(StandardCharsets.UTF_8);
        return encrypt(data, aadSpec);
    }

    public EncryptedValue encrypt(byte[] plaintext, Map<String, Object> aadFields) throws SealedPayloadProcessingException, AADEncodingException, CipherInitializationException {
        return encrypt(plaintext, AADSpecification.builder().putAll(aadFields).build());
    }

    public EncryptedValue encrypt(byte[] plaintext, AADSpecification aadSpec) throws AADEncodingException, CipherInitializationException, SealedPayloadProcessingException {
        Objects.requireNonNull(plaintext, "Plaintext cannot be null");
        byte[] nonce = new byte[12];
        random.nextBytes(nonce);
        byte[] aad = encodeAad(aadSpec);
        Cipher cipher = initCipher(Cipher.ENCRYPT_MODE, nonce, aad);
        byte[] ciphertext = doFinal(cipher, plaintext);
        return new EncryptedValue(aad, nonce, ciphertext);
    }

    public String deriveRoutingKey(String value) throws RoutingKeyDerivationException {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(macKey);
            byte[] digest = mac.doFinal(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (GeneralSecurityException e) {
            throw new RoutingKeyDerivationException("Unable to derive routing key", e);
        }
    }

    public void verifyRoute(EncryptedValue sealed,
                            TopologySpecification.Component expectedSourceComponent,
                            TopologySpecification.Component expectedDestinationComponent) {
        Objects.requireNonNull(expectedDestinationComponent, "Expected destination cannot be null");

        log.debug("Decoding AAD for route verification");
        DecodedAAD aad = DecodedAAD.fromBytes(sealed.associatedData());
        log.debug("Decoded AAD: {} using nonce: {}", aad, HexFormat.of().formatHex(sealed.nonce()));
        log.debug("Expected source: {}", expectedSourceComponent);
        log.debug("Expected destination: {}", expectedDestinationComponent);

        if (expectedSourceComponent != null) {
            aad.requireSource(expectedSourceComponent);
        }
        aad.requireDestination(expectedDestinationComponent);
    }

    private Cipher initCipher(int mode, byte[] nonce, byte[] aad) throws CipherInitializationException {
        try {
            Cipher cipher = Cipher.getInstance("ChaCha20-Poly1305");
            IvParameterSpec ivSpec = new IvParameterSpec(nonce);
            cipher.init(mode, encryptionKey, ivSpec);
            if (aad.length > 0) {
                cipher.updateAAD(aad);
            }
            return cipher;
        } catch (GeneralSecurityException e) {
            throw new CipherInitializationException("Unable to initialize cipher", e);
        }
    }

    private byte[] doFinal(Cipher cipher, byte[] input) throws SealedPayloadProcessingException {
        try {
            return cipher.doFinal(input);
        } catch (GeneralSecurityException e) {
            throw new SealedPayloadProcessingException("Unable to process sealed payload", e);
        }
    }

    private byte[] encodeAad(AADSpecification aad) throws AADEncodingException {
        if (aad == null || aad.isEmpty()) {
            return emptyAad;
        }
        Map<String, Object> sorted = new TreeMap<>(aad.attributes());
        aad.sourceComponent().ifPresent(component ->
                sorted.put("source", component.toString()));
        aad.destinationComponent().ifPresent(component ->
                sorted.put("destination", component.toString()));
        if (sorted.isEmpty()) {
            return emptyAad;
        }
        return serializeAad(sorted);
    }

    private byte[] serializeAad(Map<String, Object> fields) throws AADEncodingException {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append('{');
            boolean first = true;
            for (Map.Entry<String, Object> entry : fields.entrySet()) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                sb.append('"').append(escapeJson(entry.getKey())).append("\":");
                Object value = entry.getValue();
                if (value instanceof Number || value instanceof Boolean) {
                    sb.append(value.toString());
                } else {
                    sb.append('"').append(escapeJson(String.valueOf(value))).append('"');
                }
            }
            sb.append('}');
            return sb.toString().getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new AADEncodingException("Unable to encode AAD", e);
        }
    }

    private String escapeJson(String input) {
        StringBuilder sb = new StringBuilder(input.length());
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (c == '"' || c == '\\') {
                sb.append('\\');
            }
            sb.append(c);
        }
        return sb.toString();
    }
}
