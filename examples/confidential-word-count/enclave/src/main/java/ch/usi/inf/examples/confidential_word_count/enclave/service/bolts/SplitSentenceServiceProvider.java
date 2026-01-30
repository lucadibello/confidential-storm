package ch.usi.inf.examples.confidential_word_count.enclave.service.bolts;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.AADSpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.AADSpecificationBuilder;
import ch.usi.inf.confidentialstorm.enclave.service.bolts.ConfidentialBoltService;
import ch.usi.inf.confidentialstorm.enclave.util.EnclaveJsonUtil;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLoggerFactory;
import ch.usi.inf.examples.confidential_word_count.common.api.split.SplitSentenceService;
import ch.usi.inf.examples.confidential_word_count.common.api.split.model.SplitSentenceRequest;
import ch.usi.inf.examples.confidential_word_count.common.api.split.model.SplitSentenceResponse;
import ch.usi.inf.examples.confidential_word_count.common.topology.ComponentConstants;
import com.google.auto.service.AutoService;

import java.util.*;
import java.util.stream.Collectors;

@AutoService(SplitSentenceService.class)
public final class SplitSentenceServiceProvider extends ConfidentialBoltService<SplitSentenceRequest> implements SplitSentenceService {
    private final EnclaveLogger log = EnclaveLoggerFactory.getLogger(SplitSentenceServiceProvider.class);
    private final String producerId = UUID.randomUUID().toString();

    // for development purposes, we define the expected JSON fields here to validate the input
    private final Set<String> expectedJsonFields = new HashSet<>(List.of("body", "category", "id", "rating", "user_id"));

    private long sequenceCounter = 0;

    @Override
    public SplitSentenceResponse split(SplitSentenceRequest request) throws EnclaveServiceException {
        try {
            // decrypt the payload
            String jokeJsonPayload = sealedPayload.decryptToString(request.jokeEntry());

            // extract sentence and user_id from the json body
            Map<String, Object> jsonMap = EnclaveJsonUtil.parseJson(jokeJsonPayload);

            // ensure that all expected fields are present
            if (!jsonMap.keySet().containsAll(expectedJsonFields)) {
                log.warn("JSON payload is missing expected fields. Payload: {}", jokeJsonPayload);
                throw new RuntimeException("Invalid JSON payload structure.");
            }

            // extract body from payload
            Object bodyObj = jsonMap.get("body");
            String jokeText = (bodyObj instanceof String) ? (String) bodyObj : "";
            if (jokeText.isEmpty()) {
                log.warn("Could not extract 'body' from JSON payload: {}", jokeJsonPayload);
            }

            // Extract user_id from payload
            Object userId = jsonMap.get("user_id");
            if (userId == null) {
                throw new RuntimeException("Missing user_id in JSON payload. Did you use the correct data format?");
            }

            // compute sensitive operation
            //noinspection SimplifyStreamApiCallChains
            List<String> plainWords = Arrays.stream(jokeText.split("\\W+"))
                    .map(word -> word.toLowerCase(Locale.ROOT).trim())
                    .filter(word -> !word.isEmpty())
                    .collect(Collectors.toList());

            // NOTE: We need to encrypt each word separately as it will be handled alone
            // by the next services in the pipeline.
            List<EncryptedValue> encryptedWords = new ArrayList<>(plainWords.size());
            for (String plainWord : plainWords) {
                // append sequence number to AAD for protecting against replays
                long sequence = sequenceCounter++;

                // create default AAD specification for output words
                AADSpecificationBuilder aadBuilder = AADSpecification.builder()
                        .sourceComponent(ComponentConstants.SENTENCE_SPLIT)
                        .destinationComponent(ComponentConstants.WORD_COUNT)
                        .put("producer_id", producerId)
                        .put("seq", sequence);

                // Prepare payload JSON: { "word": "...", "user_id": "..." }
                Map<String, Object> payloadMap = new HashMap<>();
                payloadMap.put("word", plainWord);
                payloadMap.put("user_id", userId);

                // encode payload as JSON
                byte[] jsonPayloadBytes = EnclaveJsonUtil.serialize(payloadMap);

                // encrypt the word payload with its AAD
                EncryptedValue payload = sealedPayload.encrypt(jsonPayloadBytes, aadBuilder.build());

                // store encrypted word
                encryptedWords.add(payload);
            }

            // return response to bolt
            return new SplitSentenceResponse(encryptedWords);
        } catch (Throwable t) {
            super.exceptionCtx.handleException(t);
            return null;
        }
    }

    @Override
    public TopologySpecification.Component expectedSourceComponent() {
        return ComponentConstants.RANDOM_JOKE_SPOUT;
    }

    @Override
    public TopologySpecification.Component expectedDestinationComponent() {
        return ComponentConstants.SENTENCE_SPLIT;
    }

    @Override
    public Collection<EncryptedValue> valuesToVerify(SplitSentenceRequest request) {
        return List.of(request.jokeEntry());
    }
}
