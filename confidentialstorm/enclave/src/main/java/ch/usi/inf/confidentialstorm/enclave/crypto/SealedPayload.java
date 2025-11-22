package ch.usi.inf.confidentialstorm.enclave.crypto;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.crypto.model.aad.AADSpecification;
import ch.usi.inf.confidentialstorm.common.crypto.model.aad.DecodedAAD;
import ch.usi.inf.confidentialstorm.common.crypto.util.AADUtils;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
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

public final class SealedPayload {
    // FIXME: This is just for development.
    private static final String STREAM_KEY_HEX =
            "a46bf317953bf1a8f71439f74f30cd889ec0aa318f8b6431789fb10d1053d932";
    private static final byte[] STREAM_KEY = HexFormat.of().parseHex(STREAM_KEY_HEX);
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
        byte[] aad = encodeAad(aadSpec, nonce);
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

    public static void verifyRoute(EncryptedValue sealed,
                                   String expectedSourceComponentName,
                                   String expectedDestinationComponentName) {
        // NOTE: the source component can be null when the payload is created outside the enclave
        Objects.requireNonNull(expectedDestinationComponentName, "Expected destination cannot be null");

        // get decoded aad from sealed value
        DecodedAAD aad = DecodedAAD.fromBytes(sealed.associatedData());

        // ensure that the source and destination match
        LOG.debug("Decoded AAD: {}", aad);

        // source can be null if not expected
        if (expectedSourceComponentName != null)
            aad.requireSource(expectedSourceComponentName, sealed.nonce());
        // destination must match
        aad.requireDestination(expectedDestinationComponentName, sealed.nonce());
    }

    public static void verifyRoute(EncryptedValue sealed, Class<?> expectedSourceComponent, Class<?> expectedDestinationComponent) {
        verifyRoute(sealed, expectedSourceComponent.getName(), expectedDestinationComponent.getName());
    }
    public static void verifyRoute(EncryptedValue sealed, String expectedSourceComponentName, Class<?> expectedDestinationComponent) {
        verifyRoute(sealed, expectedSourceComponentName, expectedDestinationComponent.getName());
    }
    public static void verifyRoute(EncryptedValue sealed, Class<?> expectedSourceComponent, String expectedDestinationComponentName) {
        verifyRoute(sealed, expectedSourceComponent.getName(), expectedDestinationComponentName);
    }
    public static void verifyRoute(EncryptedValue sealed,
                                   TopologySpecification.Component expectedSourceComponent,
                                   TopologySpecification.Component expectedDestinationComponent) {
        verifyRoute(sealed, expectedSourceComponent.toString(), expectedDestinationComponent.toString());
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

    private static byte[] encodeAad(AADSpecification aad, byte[] nonce) {
        if (aad == null || aad.isEmpty()) {
            return EMPTY_AAD;
        }
        Map<String, Object> sorted = new TreeMap<>(aad.attributes());
        aad.sourceComponent().ifPresent(component ->
                sorted.put("source", AADUtils.privatizeComponentName(component.toString(), nonce)));
        aad.destinationComponent().ifPresent(component ->
                sorted.put("destination", AADUtils.privatizeComponentName(component.toString(), nonce)));
        if (sorted.isEmpty()) {
            return EMPTY_AAD;
        }
        try {
            return AAD_MAPPER.writeValueAsBytes(sorted);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Unable to encode AAD", e);
        }
    }
}
