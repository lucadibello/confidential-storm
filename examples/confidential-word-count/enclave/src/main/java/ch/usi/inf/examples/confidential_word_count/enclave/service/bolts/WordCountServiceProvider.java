package ch.usi.inf.examples.confidential_word_count.enclave.service.bolts;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.Hash;
import ch.usi.inf.confidentialstorm.enclave.service.bolts.ConfidentialBoltService;
import ch.usi.inf.examples.confidential_word_count.common.api.count.WordCountService;
import ch.usi.inf.examples.confidential_word_count.common.api.count.model.*;
import ch.usi.inf.examples.confidential_word_count.common.topology.ComponentConstants;
import com.google.auto.service.AutoService;

import java.util.*;

@AutoService(WordCountService.class)
public final class WordCountServiceProvider
        extends ConfidentialBoltService<WordCountBaseRequest>
        implements WordCountService {

    // buffer: Word -> (UserId -> Count)
    private final Map<String, Map<String, Long>> buffer = new HashMap<>();

    @Override
    public WordCountAckResponse count(WordCountRequest request) throws EnclaveServiceException {
        try {
            // verify request before processing
            verify(request);

            // Decrypt the word
            String word = Objects.requireNonNull(decryptToString(request.word()), "Missing word in payload");
            // Decrypt the userId
            String userId = Objects.requireNonNull(decryptToString(request.userId()), "Missing userId in payload");

            // Update buffer (per user)
            buffer.computeIfAbsent(word, k -> new HashMap<>())
                    .merge(userId, 1L, Long::sum);

            // Return acknowledgment to indicate successful buffering
            return new WordCountAckResponse();
        } catch (Throwable t) {
            super.exceptionCtx.handleException(t);
            return null; // signal error
        }
    }

    @Override
    public WordCountFlushResponse flush(WordCountFlushRequest request) throws EnclaveServiceException {
        // NOTE: no verification needed as the request has no payload
        try {
            // verify request
            verify(request);

            // Prepare list to hold WordCountResponses
            List<WordCountResponse> responses = new ArrayList<>();

            // for each entry in the buffer, create a WordCountResponse
            for (Map.Entry<String, Map<String, Long>> wordEntry : buffer.entrySet()) {
                String word = wordEntry.getKey();
                Map<String, Long> userCounts = wordEntry.getValue();

                for (Map.Entry<String, Long> userEntry : userCounts.entrySet()) {
                    String userId = userEntry.getKey();
                    Long count = userEntry.getValue();

                    int seq = nextSequenceNumber();

                    // Encrypt word (just the word string)
                    EncryptedValue sealedWord = encrypt(word, seq);

                    // Encrypt count
                    EncryptedValue sealedCount = encrypt(count, seq);

                    // Encrypt userId
                    EncryptedValue sealedUserId = encrypt(userId, seq);

                    // Generate routing info that links user to word (needed to route to correct user contribution boundary bolt)
                    String routingInfo = "user:" + userId + "|word:" + word;
                    byte[] routingKey = Hash.computeHash(routingInfo.getBytes());

                    // Create WordCountResponse: (word, count, userId, routingKey)
                    responses.add(new WordCountResponse(sealedWord, sealedCount, sealedUserId, routingKey));
                }
            }

            // clear the buffer after flushing
            buffer.clear();

            // Return the flush response with all WordCountResponses
            return new WordCountFlushResponse(responses);
        } catch (Throwable t) {
            super.exceptionCtx.handleException(t);
            return null; // signal error
        }
    }

    @Override
    public TopologySpecification.Component expectedSourceComponent() {
        return ComponentConstants.SENTENCE_SPLIT;
    }

    @Override
    public TopologySpecification.Component expectedDestinationComponent() {
        return ComponentConstants.USER_CONTRIBUTION_BOUNDING;
    }

    @Override
    public TopologySpecification.Component currentComponent() {
        return ComponentConstants.WORD_COUNT;
    }
}