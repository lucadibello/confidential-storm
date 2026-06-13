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
     * Expected fields in the decrypted payload (excluding user_id).
     */
    private final Set<String> expectedPayloadFields = new HashSet<>(List.of("body", "category", "id", "rating"));

    @Override
    public SplitSentenceResponse split(SplitSentenceRequest request) throws EnclaveServiceException {
        try {
            // Verify request
            verify(request);

            // Decrypt joke payload
            Map<String, Object> payloadMap = decryptToMap(request.payload());

            // Verify expected fields
            if (!payloadMap.keySet().containsAll(expectedPayloadFields)) {
                log.warn("JSON payload is missing expected fields.");
                throw new RuntimeException("Invalid JSON payload structure.");
            }

            // Extract body text
            String jokeText = (String) payloadMap.get("body");
            if (jokeText == null) {
                log.warn("Could not extract 'body' from JSON payload");
                throw new RuntimeException("Missing 'body' field in payload");
            }

            // Decrypt user ID for per-word re-encryption
            String userIdStr = decryptToString(request.userId());

            // Split text into words
            //noinspection SimplifyStreamApiCallChains
            List<String> plainWords = Arrays.stream(jokeText.split("\\W+"))
                    .map(word -> word.toLowerCase(Locale.ROOT).trim())
                    .filter(word -> !word.isEmpty())
                    .collect(Collectors.toList());

            // Encrypt each word separately with a unique sequence number as each becomes a downstream tuple
            List<SealedWord> sealedWords = new ArrayList<>(plainWords.size());
            for (String plainWord : plainWords) {
                int seq = nextSequenceNumber();
                // Encrypt word
                EncryptedValue encryptedWord = encrypt(plainWord, seq);
                // Encrypt count
                EncryptedValue encryptedCount = encrypt(1L, seq);
                // Re-encrypt user ID
                EncryptedValue reEncryptedUserId = encrypt(userIdStr, seq);

                // Generate routing key based on user ID
                String routingInfo = "user:" + userIdStr;
                byte[] routingKey = Hash.computeHash(routingInfo.getBytes());

                // Create sealed word with re-encrypted user ID
                sealedWords.add(new SealedWord(encryptedWord, encryptedCount, reEncryptedUserId, routingKey));
            }

            // Return sealed words
            return new SplitSentenceResponse(sealedWords);
        } catch (Throwable t) {
            super.exceptionCtx.handleException(t);
            return null;
        }
    }

    @Override
    public TopologySpecification.Component currentComponent() {
        return ComponentConstants.BOLT_SENTENCE_SPLIT;
    }
}
