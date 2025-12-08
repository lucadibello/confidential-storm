package ch.usi.inf.examples.confidential_word_count.enclave.service.bolts.wordcount;

import ch.usi.inf.confidentialstorm.common.crypto.exception.AADEncodingException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.CipherInitializationException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.SealedPayloadProcessingException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.AADSpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.AADSpecificationBuilder;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.DecodedAAD;
import ch.usi.inf.examples.confidential_word_count.common.api.WordCountService;
import ch.usi.inf.examples.confidential_word_count.common.api.model.*;
import ch.usi.inf.examples.confidential_word_count.common.topology.ComponentConstants;
import com.google.auto.service.AutoService;

import java.util.*;

@AutoService(WordCountService.class)
public final class WordCountServiceImpl extends WordCountVerifier {
    private final String producerId = UUID.randomUUID().toString();
    // buffer: Word -> (UserId -> Count)
    private final Map<String, Map<String, Long>> buffer = new HashMap<>();
    private long sequenceCounter = 0L;

    @Override
    public WordCountAckResponse countImpl(WordCountRequest request) throws SealedPayloadProcessingException, CipherInitializationException {
        // Decrypt the word from the request
        String word = sealedPayload.decryptToString(request.word());

        // Extract user_id from input AAD
        DecodedAAD inputAad = DecodedAAD.fromBytes(request.word().associatedData());
        Object userIdObj = inputAad.attributes().get("user_id");
        String userId = userIdObj != null ? userIdObj.toString() : "unknown_user";

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
                
                // Propagate user_id if known
                if (!"unknown_user".equals(userId)) {
                    entryAadBuilder.put("user_id", userId);
                }
                
                AADSpecification aad = entryAadBuilder.build();

                // Seal the word and the count
                EncryptedValue sealedWord = sealedPayload.encryptString(word, aad);
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