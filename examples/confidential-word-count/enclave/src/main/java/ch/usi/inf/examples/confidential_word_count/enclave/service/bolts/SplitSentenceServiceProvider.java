package ch.usi.inf.examples.confidential_word_count.enclave.service.bolts;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.Hash;
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
public final class SplitSentenceServiceProvider
        extends ConfidentialBoltService<SplitSentenceRequest>
        implements SplitSentenceService {

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
            // verify the request before processing
            verify(request);

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

            // Decrypt the userId for re-encryption with per-word AAD
            String userIdStr = decryptToString(request.userId());

            // Split the joke text into words
            //noinspection SimplifyStreamApiCallChains
            List<String> plainWords = Arrays.stream(jokeText.split("\\W+"))
                    .map(word -> word.toLowerCase(Locale.ROOT).trim())
                    .filter(word -> !word.isEmpty())
                    .collect(Collectors.toList());

            // Create list of SealedWord (word, userId) pairs
            // Each word is encrypted separately with a unique sequence number per tuple,
            // since each SealedWord becomes its own tuple downstream
            List<SealedWord> sealedWords = new ArrayList<>(plainWords.size());
            for (String plainWord : plainWords) {
                int seq = nextSequenceNumber();
                // Encrypt just the word (no user_id embedded)
                EncryptedValue encryptedWord = encrypt(plainWord, seq);
                // Encrypt count (1)
                EncryptedValue encryptedCount = encrypt(1L, seq);
                // Re-encrypt userId with this tuple's sequence number
                EncryptedValue reEncryptedUserId = encrypt(userIdStr, seq);

                // Generate routing info that links user to word (needed to route to correct user contribution boundary bolt)
                String routingInfo = "user:" + userIdStr + "|word:" + plainWord;
                byte[] routingKey = Hash.computeHash(routingInfo.getBytes());

                // Pair with the re-encrypted userId
                sealedWords.add(new SealedWord(encryptedWord, encryptedCount, reEncryptedUserId, routingKey));
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
        return ComponentConstants.SPOUT_RANDOM_JOKE;
    }

    @Override
    public TopologySpecification.Component expectedDestinationComponent() {
        return ComponentConstants.BOLT_USER_CONTRIBUTION_BOUNDING;
    }

    @Override
    public TopologySpecification.Component currentComponent() {
        return ComponentConstants.BOLT_SENTENCE_SPLIT;
    }
}
