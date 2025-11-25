package ch.usi.inf.confidentialstorm.enclave.service.spouts;

import ch.usi.inf.confidentialstorm.common.api.SpoutMapperService;
import ch.usi.inf.confidentialstorm.common.crypto.exception.AADEncodingException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.CipherInitializationException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.SealedPayloadProcessingException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.enclave.EnclaveConfig;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.AADSpecification;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.SealedPayload;
import ch.usi.inf.confidentialstorm.enclave.util.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.EnclaveLoggerFactory;
import com.google.auto.service.AutoService;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@AutoService(SpoutMapperService.class)
public class SpoutMapperServiceImpl implements SpoutMapperService {
    private final EnclaveLogger LOG = EnclaveLoggerFactory.getLogger(SpoutMapperServiceImpl.class);
    private final AtomicLong sequenceCounter = new AtomicLong(0);
    private final String producerId = UUID.randomUUID().toString();
    private final SealedPayload sealedPayload;

    public SpoutMapperServiceImpl() {
        this(SealedPayload.fromConfig());
    }

    SpoutMapperServiceImpl(SealedPayload sealedPayload) {
        this.sealedPayload = Objects.requireNonNull(sealedPayload, "sealedPayload cannot be null");
    }

    @Override
    public EncryptedValue testRoute(TopologySpecification.Component component, EncryptedValue entry) throws SealedPayloadProcessingException, CipherInitializationException {
        // do something here
        Random rnd = new Random();
        String plaintext = "Hello from SpoutMapperServiceImpl!";
        byte[] data = plaintext.getBytes(StandardCharsets.UTF_8);
        byte[] nonce = new byte[12];
        rnd.nextBytes(nonce);
        byte[] aad = "TestAAD".getBytes(StandardCharsets.UTF_8);

        // now, try to infer AAD from jackson-serialized entry
        AADSpecification spec = AADSpecification.builder().put("testing", 1).build();

        // try to derypt with ChaCha20-Poly1305
        // byte[] body = sealedPayload.decrypt(entry);

        try {
            Cipher cipher = Cipher.getInstance("ChaCha20-Poly1305");
            IvParameterSpec ivSpec = new IvParameterSpec(nonce);
            final byte[] key = HexFormat.of().parseHex(EnclaveConfig.STREAM_KEY_HEX);
            final SecretKey ENCRYPTION_KEY = new SecretKeySpec(key, "ChaCha20");
            cipher.init(Cipher.ENCRYPT_MODE, ENCRYPTION_KEY, ivSpec);
            if (aad.length > 0) {
                cipher.updateAAD(aad);
            }
            // encryption works
            byte[] ciphertext = cipher.doFinal(data);
            LOG.info("Encryption successful, ciphertext: " + HexFormat.of().formatHex(ciphertext));
        } catch (Exception e) {
            throw new SealedPayloadProcessingException("Failed to initialize cipher", e);
        }

        // test encryption/decryption on random data
        return new EncryptedValue(entry.associatedData(), entry.nonce(), entry.ciphertext()); // this worked perfectly.
    }

    @Override
    public EncryptedValue setupRoute(TopologySpecification.Component component, EncryptedValue entry) throws SealedPayloadProcessingException, CipherInitializationException, AADEncodingException {
        Objects.requireNonNull(component, "component cannot be null");
        Objects.requireNonNull(entry, "Encrypted entry cannot be null");

        LOG.info("Setting up route for component: " + component.name());
        // we want to verify that the entry is correctly sealed
        sealedPayload.verifyRoute(entry,
                TopologySpecification.Component.DATASET,
                TopologySpecification.Component.MAPPER
        );

        // get string body
        byte[] body = sealedPayload.decrypt(entry);

        TopologySpecification.Component downstreamComponent = TopologySpecification.requireSingleDownstream(component);

        long sequence = sequenceCounter.getAndIncrement();
        // create new AAD with correct route names
        AADSpecification aad = AADSpecification.builder()
                .sourceComponent(component)
                .destinationComponent(downstreamComponent)
                .put("producer_id", producerId)
                .put("seq", sequence)
                .build();

        // seal again with new AAD routing information + return sealed entry
        return sealedPayload.encrypt(body, aad);
    }
}
