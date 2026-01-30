package ch.usi.inf.examples.confidential_word_count.enclave.service.spouts;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.SealedPayload;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.AADSpecification;
import ch.usi.inf.confidentialstorm.enclave.exception.EnclaveExceptionContext;
import ch.usi.inf.examples.confidential_word_count.common.api.spout.SpoutRouterService;
import ch.usi.inf.examples.confidential_word_count.common.topology.ComponentConstants;
import com.google.auto.service.AutoService;

import java.util.Objects;
import java.util.UUID;


/**
 * The SpoutRouterServiceProvider is responsible to re-encrypt the data coming from the spout with the
 * correct AAD routing information so that it can be correctly verified and decrypted by the downstream component.
 * <p>
 * This implementation allows to ensure routing integrity and confidentiality of the data as it moves through
 * the topology.
 * <p>
 * NOTE: If we disable routing verification, this is not needed.
 * (refer to @link{ch.usi.inf.examples.confidential_word_count.enclave.WordCountEnclaveConfigProvider#isRouteValidationEnabled()}
 */
@AutoService(SpoutRouterService.class)
public final class SpoutRouterServiceProvider implements SpoutRouterService {

    /**
     * Exception context for handling exceptions within the enclave.
     */
    private final EnclaveExceptionContext exceptionCtx = EnclaveExceptionContext.getInstance();

    /**
     * Sealed payload utility for encrypting and decrypting data with AAD.
     */
    private final SealedPayload sealedPayload;

    /**
     * Source component where the data originates from.
     * <p>
     * As this is a spout router, the source component is always the spout itself.
     */
    private final TopologySpecification.Component sourceComponent = ComponentConstants.RANDOM_JOKE_SPOUT;

    /**
     * UUID to uniquely identify this producer instance, used for packet replay protection.
     */
    private final String producerId = UUID.randomUUID().toString();

    /**
     * Sequence counter to uniquely identify each message from this producer instance, used for packet replay protection.
     */
    private long sequenceCounter = 0L;

    /**
     * Default constructor initializing the SpoutRouterServiceProvider with a SealedPayload
     * created from the enclave configuration.
     */
    @SuppressWarnings("unused")
    public SpoutRouterServiceProvider() {
        this(SealedPayload.fromConfig());
    }

    /**
     * Constructor initializing the SpoutRouterServiceProvider with the provided SealedPayload.
     *
     * @param sealedPayload The SealedPayload instance used for encryption and decryption.
     */
    SpoutRouterServiceProvider(SealedPayload sealedPayload) {
        this.sealedPayload = Objects.requireNonNull(sealedPayload, "sealedPayload cannot be null");
    }

    @Override
    public EncryptedValue setupRoute(EncryptedValue entry) throws EnclaveServiceException {
        try {
            Objects.requireNonNull(entry, "Encrypted entry cannot be null");

            // we want to verify that the entry is correctly sealed
            sealedPayload.verifyRoute(entry,
                    ComponentConstants.DATASET,
                    ComponentConstants.MAPPER
            );

            // get string body
            byte[] body = sealedPayload.decrypt(entry);
            long sequence = sequenceCounter++;

            // inspect graph to find correct downstream component
            TopologySpecification.Component destinationComponent = TopologySpecification.requireSingleDownstream(
                    ComponentConstants.RANDOM_JOKE_SPOUT
            );

            // create new AAD with correct route names
            AADSpecification aad = AADSpecification.builder()
                    .sourceComponent(sourceComponent)
                    .destinationComponent(destinationComponent)
                    .put("producer_id", producerId)
                    .put("seq", sequence)
                    .build();

            // seal again with new AAD routing information + return sealed entry
            return sealedPayload.encrypt(body, aad);
        } catch (Throwable t) {
            exceptionCtx.handleException(t);
            return null;
        }
    }
}
