package ch.usi.inf.examples.confidential_word_count.enclave.service.bolts;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.service.bolts.ConfidentialBoltService;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLoggerFactory;
import ch.usi.inf.examples.confidential_word_count.common.api.split.SplitSentenceService;
import ch.usi.inf.examples.confidential_word_count.common.api.split.model.SealedWord;
import ch.usi.inf.examples.confidential_word_count.common.api.split.model.SplitSentenceRequest;
import ch.usi.inf.examples.confidential_word_count.common.api.split.model.SplitSentenceResponse;
import ch.usi.inf.examples.confidential_word_count.common.topology.ComponentConstants;
import com.google.auto.service.AutoService;

import java.util.*;
import java.util.stream.Collectors;

@AutoService(SplitSentenceService.class)
public final class SplitSentenceServiceProvider extends ConfidentialBoltService<SplitSentenceRequest> implements SplitSentenceService {

    /**
     * The logger instance for this class.
     */
    private final EnclaveLogger log = EnclaveLoggerFactory.getLogger(SplitSentenceServiceProvider.class);

    /**
     * Expected fields in the decrypted payload (body, category, id, rating - no user_id, it's separate).
     */
    private final Set<String> expectedPayloadFields = new HashSet<>(List.of("body", "category", "id", "rating"));

    @Override
    public SplitSentenceResponse split(SplitSentenceRequest request) throws EnclaveServiceException {
        try {
            // Decrypt the payload (body, category, id, rating)
            Map<String, Object> payloadMap = decryptToMap(request.payload());

            // ensure that all expected fields are present
            if (!payloadMap.keySet().containsAll(expectedPayloadFields)) {
                log.warn("JSON payload is missing expected fields.");
                throw new RuntimeException("Invalid JSON payload structure.");
            }

            // extract body from payload
            String jokeText = (String) payloadMap.get("body");
            if (jokeText == null) {
                log.warn("Could not extract 'body' from JSON payload");
                throw new RuntimeException("Missing 'body' field in payload");
            }

            // The userId is already encrypted separately - we just need to re-encrypt it
            // with the correct AAD for the next hop
            int seq = nextSequenceNumber();
            EncryptedValue reEncryptedUserId = encrypt(decryptToBytes(request.userId()), seq);

            // Split the joke text into words
            //noinspection SimplifyStreamApiCallChains
            List<String> plainWords = Arrays.stream(jokeText.split("\\W+"))
                    .map(word -> word.toLowerCase(Locale.ROOT).trim())
                    .filter(word -> !word.isEmpty())
                    .collect(Collectors.toList());

            // Create list of SealedWord (word, userId) pairs
            // Each word is encrypted separately, userId is the same for all words from this joke
            List<SealedWord> sealedWords = new ArrayList<>(plainWords.size());
            for (String plainWord : plainWords) {
                // Encrypt just the word (no user_id embedded)
                EncryptedValue encryptedWord = encrypt(plainWord, seq);
                // Pair with the re-encrypted userId
                sealedWords.add(new SealedWord(encryptedWord, reEncryptedUserId));
            }

            // Return response with tuple format: List<(word, userId)>
            return new SplitSentenceResponse(sealedWords);
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
        return ComponentConstants.WORD_COUNT;
    }

    @Override
    public TopologySpecification.Component currentComponent() {
        return ComponentConstants.SENTENCE_SPLIT;
    }
}
