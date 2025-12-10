package ch.usi.inf.examples.confidential_word_count.enclave.service.bolts.wordcount;

import ch.usi.inf.confidentialstorm.common.crypto.exception.AADEncodingException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.CipherInitializationException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.SealedPayloadProcessingException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.AADSpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.AADSpecificationBuilder;
import ch.usi.inf.confidentialstorm.enclave.util.EnclaveJsonUtil;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLoggerFactory;
import ch.usi.inf.examples.confidential_word_count.common.api.WordCountService;
import ch.usi.inf.examples.confidential_word_count.common.api.model.*;
import ch.usi.inf.examples.confidential_word_count.common.topology.ComponentConstants;
import com.google.auto.service.AutoService;

import java.util.*;

@AutoService(WordCountService.class)
public final class WordCountServiceImpl extends WordCountVerifier {
    private final EnclaveLogger log = EnclaveLoggerFactory.getLogger(WordCountService.class);
    private final String producerId = UUID.randomUUID().toString();
    private long sequenceCounter = 0;

    // buffer: Word -> (UserId -> Count)
    private final Map<String, Map<String, Long>> buffer = new HashMap<>();

    // for development purposes, we define the expected JSON fields here to validate the input
    private final Set<String> expectedJsonFields = new HashSet<>(List.of("word", "user_id"));

    @Override
    public WordCountAckResponse countImpl(WordCountRequest request) throws SealedPayloadProcessingException, CipherInitializationException {
        // Decrypt the payload
        String jsonPayload = sealedPayload.decryptToString(request.word());
        Map<String, Object> jsonMap = EnclaveJsonUtil.parseJson(jsonPayload);

        // Validate expected fields
        if (!jsonMap.keySet().containsAll(expectedJsonFields)) {
            log.warn("Invalid payload structure: {}", jsonPayload);
            throw new RuntimeException("Invalid payload structure");
        }
        
        // Extract word from payload
        String word = (String) jsonMap.get("word");
        if (word == null) {
            log.warn("Missing 'word' field in payload: {}", jsonPayload);
            throw new RuntimeException("Missing 'word' field in payload");
        }

        // Extract user_id from payload
        String userId = String.valueOf(jsonMap.get("user_id")); // long to string
        if (userId == null) {
            log.warn("Missing 'user_id' field in payload: {}", jsonPayload);
            throw new RuntimeException("Missing 'user_id' field in payload");
        }

        // Update buffer (per user)
        buffer.computeIfAbsent(word, k -> new HashMap<>())
              .merge(userId, 1L, Long::sum);

        // Return acknowledgment to indicate successful buffering
        return new WordCountAckResponse();
    }

    @Override
    public WordCountFlushResponse flushImpl(WordCountFlushRequest request) throws SealedPayloadProcessingException, CipherInitializationException, AADEncodingException {
        // Prepare list to hold WordCountResponses
        List<WordCountResponse> responses = new ArrayList<>();

        // for each entry in the buffer, create a WordCountResponse
        for (Map.Entry<String, Map<String, Long>> wordEntry : buffer.entrySet()) {
            String word = wordEntry.getKey();
            Map<String, Long> userCounts = wordEntry.getValue();

            for (Map.Entry<String, Long> userEntry : userCounts.entrySet()) {
                String userId = userEntry.getKey();
                Long count = userEntry.getValue();

                // Create AAD
                long sequence = sequenceCounter++;
                AADSpecificationBuilder entryAadBuilder = AADSpecification.builder()
                        .sourceComponent(ComponentConstants.WORD_COUNT)
                        .destinationComponent(ComponentConstants.HISTOGRAM_GLOBAL)
                        .put("producer_id", producerId)
                        .put("seq", sequence);

                // Prepare payload JSON for the word: { "word": "...", "user_id": "..." }
                Map<String, Object> wordPayloadMap = new HashMap<>();
                wordPayloadMap.put("word", word);
                wordPayloadMap.put("user_id", userId);

                // encode payload as JSON
                byte[] jsonPayloadBytes = EnclaveJsonUtil.serialize(wordPayloadMap);

                // build aad specification
                AADSpecification aad = entryAadBuilder.build();

                // Seal the word (with user_id in payload) and the count
                EncryptedValue sealedWord = sealedPayload.encrypt(jsonPayloadBytes, aad);
                EncryptedValue sealedCount = sealedPayload.encryptString(Long.toString(count), aad);

                // Create WordCountResponse and add to responses
                responses.add(new WordCountResponse(sealedWord, sealedCount));
            }
        }

        // clear the buffer after flushing
        buffer.clear();

        // Return the flush response with all WordCountResponses
        return new WordCountFlushResponse(responses);
    }
}