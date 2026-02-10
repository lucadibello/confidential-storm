package ch.usi.inf.confidentialstorm.enclave.crypto;

import ch.usi.inf.confidentialstorm.common.crypto.exception.AADEncodingException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.CipherInitializationException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.SealedPayloadProcessingException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.EnclaveConfig;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.AADSpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.DecodedAAD;
import ch.usi.inf.confidentialstorm.enclave.util.EnclaveJsonUtil;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLoggerFactory;

import javax.crypto.Cipher;
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
    private final SecretKey encryptionKey;
    private final SecureRandom random = new SecureRandom();;
    private final byte[] emptyAad = new byte[0];

    private SealedPayload(byte[] streamKey) {
        this.encryptionKey = new SecretKeySpec(streamKey, "ChaCha20");
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

    public EncryptedValue encrypt(byte[] plaintext, AADSpecification aadSpec) throws AADEncodingException, CipherInitializationException, SealedPayloadProcessingException {
        Objects.requireNonNull(plaintext, "Plaintext cannot be null");
        byte[] nonce = new byte[12];
        random.nextBytes(nonce);
        byte[] aad = encodeAad(aadSpec);
        Cipher cipher = initCipher(Cipher.ENCRYPT_MODE, nonce, aad);
        byte[] ciphertext = doFinal(cipher, plaintext);
        return new EncryptedValue(aad, nonce, ciphertext);
    }

    public boolean isRouteValid(EncryptedValue sealed,
                            TopologySpecification.Component expectedSourceComponent,
                            TopologySpecification.Component expectedDestinationComponent) {
        Objects.requireNonNull(expectedDestinationComponent, "Expected destination cannot be null");

        // decode the AAD to check the source and destination components
        DecodedAAD aad = DecodedAAD.fromBytes(sealed.associatedData());

        // ensure the AAD matches the expected source and destination components
        return aad.matchesSource(expectedSourceComponent)
                && aad.matchesDestination(expectedDestinationComponent);
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
            return EnclaveJsonUtil.serialize(fields);
        } catch (Exception e) {
            throw new AADEncodingException("Unable to encode AAD", e);
        }
    }
}
