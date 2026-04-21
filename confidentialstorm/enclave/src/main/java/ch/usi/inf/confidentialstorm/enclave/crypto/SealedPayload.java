package ch.usi.inf.confidentialstorm.enclave.crypto;

import ch.usi.inf.confidentialstorm.common.crypto.exception.AADEncodingException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.CipherInitializationException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.SealedPayloadProcessingException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.EnclaveConfig;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.AADSpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.DecodedAAD;
import ch.usi.inf.confidentialstorm.common.util.EnclaveJsonUtil;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.security.spec.AlgorithmParameterSpec;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Utility class for sealing and unsealing payloads within the enclave.
 * Supports multiple AEAD encryption schemes via {@link EncryptionScheme}.
 */
public final class SealedPayload {
    private static final EnclaveLogger logger = EnclaveLoggerFactory.getLogger(SealedPayload.class);
    private static final int GCM_TAG_LENGTH_BITS = 128;

    private final SecretKey encryptionKey;
    private final EncryptionScheme scheme;
    private final SecureRandom random = new SecureRandom();
    private final byte[] emptyAad = new byte[0];

    /**
     * Creates a new SealedPayload with the given stream key and encryption scheme.
     *
     * @param streamKey the raw key bytes (256-bit / 32 bytes)
     * @param scheme    the encryption scheme to use
     */
    SealedPayload(byte[] streamKey, EncryptionScheme scheme) {
        this.scheme = Objects.requireNonNull(scheme, "Encryption scheme cannot be null");
        if (scheme.isEncryptionEnabled()) {
            this.encryptionKey = new SecretKeySpec(streamKey, scheme.getKeyAlgorithm());
        } else {
            this.encryptionKey = null;
            logger.warn("SealedPayload initialized with NONE encryption scheme -- " +
                    "no confidentiality or integrity protection is provided");
        }
    }

    /**
     * Creates a new SealedPayload instance using the key and scheme from the enclave configuration.
     *
     * @return the SealedPayload instance
     */
    public static SealedPayload fromConfig() {
        return new SealedPayload(
                HexFormat.of().parseHex(EnclaveConfig.STREAM_KEY_HEX),
                EnclaveConfig.ENCRYPTION_SCHEME
        );
    }

    /**
     * Decrypts a sealed payload.
     *
     * @param sealed the sealed payload to decrypt
     * @return the decrypted bytes
     * @throws SealedPayloadProcessingException if decryption fails
     * @throws CipherInitializationException    if cipher initialization fails
     */
    public byte[] decrypt(EncryptedValue sealed) throws SealedPayloadProcessingException, CipherInitializationException {
        Objects.requireNonNull(sealed, "Encrypted payload cannot be null");
        if (!scheme.isEncryptionEnabled()) {
            return sealed.ciphertext();
        }
        Cipher cipher = initCipher(Cipher.DECRYPT_MODE, sealed.nonce(), sealed.associatedData());
        return doFinal(cipher, sealed.ciphertext());
    }

    /**
     * Decrypts a sealed payload and returns the result as a UTF-8 string.
     *
     * @param sealed the sealed payload to decrypt
     * @return the decrypted string
     * @throws SealedPayloadProcessingException if decryption fails
     * @throws CipherInitializationException    if cipher initialization fails
     */
    public String decryptToString(EncryptedValue sealed) throws SealedPayloadProcessingException, CipherInitializationException {
        byte[] plaintext = decrypt(sealed);
        return new String(plaintext, StandardCharsets.UTF_8);
    }

    /**
     * Encrypts a string into a sealed payload.
     *
     * @param plaintext the string to encrypt
     * @param aadSpec   the AAD specification to include
     * @return the sealed payload
     * @throws SealedPayloadProcessingException if encryption fails
     * @throws AADEncodingException             if AAD encoding fails
     * @throws CipherInitializationException    if cipher initialization fails
     */
    public EncryptedValue encryptString(String plaintext, AADSpecification aadSpec) throws SealedPayloadProcessingException, AADEncodingException, CipherInitializationException {
        byte[] data = plaintext.getBytes(StandardCharsets.UTF_8);
        return encrypt(data, aadSpec);
    }

    /**
     * Encrypts a byte array into a sealed payload.
     *
     * @param plaintext the bytes to encrypt
     * @param aadSpec   the AAD specification to include
     * @return the sealed payload
     * @throws AADEncodingException             if AAD encoding fails
     * @throws CipherInitializationException    if cipher initialization fails
     * @throws SealedPayloadProcessingException if encryption fails
     */
    public EncryptedValue encrypt(byte[] plaintext, AADSpecification aadSpec) throws AADEncodingException, CipherInitializationException, SealedPayloadProcessingException {
        Objects.requireNonNull(plaintext, "Plaintext cannot be null");
        byte[] aad = encodeAad(aadSpec);

        if (!scheme.isEncryptionEnabled()) {
            byte[] zeroNonce = new byte[scheme.getNonceSize()];
            return new EncryptedValue(aad, zeroNonce, plaintext);
        }

        byte[] nonce = new byte[scheme.getNonceSize()];
        random.nextBytes(nonce);
        Cipher cipher = initCipher(Cipher.ENCRYPT_MODE, nonce, aad);
        byte[] ciphertext = doFinal(cipher, plaintext);
        return new EncryptedValue(aad, nonce, ciphertext);
    }

    /**
     * Verifies if the routing information in the sealed payload's AAD matches the expected source and destination.
     *
     * @param sealed                       the sealed payload
     * @param expectedSourceComponent      the expected source component
     * @param expectedDestinationComponent the expected destination component
     * @return true if routing is valid, false otherwise
     */
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
            Cipher cipher = Cipher.getInstance(scheme.getCipherAlgorithm());
            AlgorithmParameterSpec paramSpec = switch (scheme) {
                case AES_256_GCM -> new GCMParameterSpec(GCM_TAG_LENGTH_BITS, nonce);
                case CHACHA20_POLY1305 -> new IvParameterSpec(nonce);
                case NONE -> throw new IllegalStateException("Cannot init cipher for NONE scheme");
            };
            cipher.init(mode, encryptionKey, paramSpec);
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
            throw new SealedPayloadProcessingException("Unable to process sealed payload with scheme " + scheme, e);
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
