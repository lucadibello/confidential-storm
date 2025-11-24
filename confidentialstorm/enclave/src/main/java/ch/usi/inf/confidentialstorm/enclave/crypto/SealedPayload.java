package ch.usi.inf.confidentialstorm.enclave.crypto;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.crypto.model.aad.AADSpecification;
import ch.usi.inf.confidentialstorm.common.crypto.model.aad.DecodedAAD;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.common.crypto.exception.AADEncodingException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.CipherInitializationException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.RoutingKeyDerivationException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.SealedPayloadProcessingException;
import ch.usi.inf.confidentialstorm.enclave.EnclaveConfig;
import ch.usi.inf.confidentialstorm.enclave.util.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.EnclaveLoggerFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.*;

/**
 * Utility class for sealing and unsealing payloads within the enclave.
 * Uses ChaCha20-Poly1305 for encryption and HMAC-SHA256 for routing key derivation.
 */
public final class SealedPayload {
    private static final byte[] STREAM_KEY = HexFormat.of().parseHex(EnclaveConfig.STREAM_KEY_HEX);
    private static final SecretKey ENCRYPTION_KEY = new SecretKeySpec(STREAM_KEY, "ChaCha20");
    private static final SecretKey MAC_KEY = new SecretKeySpec(STREAM_KEY, "HmacSHA256");
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final byte[] EMPTY_AAD = new byte[0];

    private static final ObjectMapper AAD_MAPPER = JsonMapper.builder()
            .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
            .build();

    private static final EnclaveLogger LOG = EnclaveLoggerFactory.getLogger(SealedPayload.class);

    private SealedPayload() {
    }

    public static byte[] decrypt(EncryptedValue sealed) {
        Objects.requireNonNull(sealed, "Encrypted payload cannot be null");
        Cipher cipher = initCipher(Cipher.DECRYPT_MODE, sealed.nonce(), sealed.associatedData());
        return doFinal(cipher, sealed.ciphertext());
    }

    public static String decryptToString(EncryptedValue sealed) {
        byte[] plaintext = decrypt(sealed);
        return new String(plaintext, StandardCharsets.UTF_8);
    }

    public static EncryptedValue encryptString(String plaintext, AADSpecification aadSpec) {
        byte[] data = plaintext.getBytes(StandardCharsets.UTF_8);
        return encrypt(data, aadSpec);
    }

    public static EncryptedValue encrypt(byte[] plaintext, Map<String, Object> aadFields) {
        return encrypt(plaintext, AADSpecification.builder().putAll(aadFields).build());
    }

    public static EncryptedValue encrypt(byte[] plaintext, AADSpecification aadSpec) {
        Objects.requireNonNull(plaintext, "Plaintext cannot be null");
        byte[] nonce = new byte[12];
        RANDOM.nextBytes(nonce);
        byte[] aad = encodeAad(aadSpec);
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
            throw new RoutingKeyDerivationException("Unable to derive routing key", e);
        }
    }

    public static void verify(EncryptedValue sealed,
                              TopologySpecification.Component expectedSourceComponent,
                              TopologySpecification.Component expectedDestinationComponent,
                              int expectedSequenceNumber) {
        // NOTE: the source component can be null when the payload is created outside the enclave
        Objects.requireNonNull(expectedDestinationComponent, "Expected destination cannot be null");

        // get decoded aad from sealed value
        DecodedAAD aad = DecodedAAD.fromBytes(sealed.associatedData());

        // ensure that the source and destination match
        LOG.debug("Decoded AAD: {}", aad, " using nonce: {}", HexFormat.of().formatHex(sealed.nonce()));
        LOG.debug("Expected source: {}", expectedSourceComponent);
        LOG.debug("Expected destination: {}", expectedDestinationComponent);

        // source can be null if not expected
        if (expectedSourceComponent != null)
            aad.requireSource(expectedSourceComponent);
        // destination must match
        aad.requireDestination(expectedDestinationComponent);
        // ensure that the sequence number matches
        aad.requireSequenceNumber(expectedSequenceNumber);
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
            throw new CipherInitializationException("Unable to initialize cipher", e);
        }
    }

    private static byte[] doFinal(Cipher cipher, byte[] input) {
        try {
            return cipher.doFinal(input);
        } catch (GeneralSecurityException e) {
            throw new SealedPayloadProcessingException("Unable to process sealed payload", e);
        }
    }

    private static byte[] encodeAad(AADSpecification aad) {
        if (aad == null || aad.isEmpty()) {
            return EMPTY_AAD;
        }
        Map<String, Object> sorted = new TreeMap<>(aad.attributes());
        aad.sourceComponent().ifPresent(component ->
                sorted.put("source", component.toString()));
        aad.destinationComponent().ifPresent(component ->
                sorted.put("destination", component.toString()));
        if (sorted.isEmpty()) {
            return EMPTY_AAD;
        }
        try {
            return AAD_MAPPER.writeValueAsBytes(sorted);
        } catch (JsonProcessingException e) {
            throw new AADEncodingException("Unable to encode AAD", e);
        }
    }
}
