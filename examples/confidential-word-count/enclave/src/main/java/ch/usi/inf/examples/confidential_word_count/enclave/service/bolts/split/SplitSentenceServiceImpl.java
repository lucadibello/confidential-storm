package ch.usi.inf.examples.confidential_word_count.enclave.service.bolts.split;

import ch.usi.inf.confidentialstorm.common.crypto.exception.AADEncodingException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.CipherInitializationException;
import ch.usi.inf.confidentialstorm.common.crypto.exception.SealedPayloadProcessingException;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.AADSpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.AADSpecificationBuilder;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.DecodedAAD;
import ch.usi.inf.confidentialstorm.enclave.util.EnclaveJsonUtil;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLoggerFactory;
import ch.usi.inf.examples.confidential_word_count.common.api.SplitSentenceService;
import ch.usi.inf.examples.confidential_word_count.common.api.model.SplitSentenceRequest;
import ch.usi.inf.examples.confidential_word_count.common.api.model.SplitSentenceResponse;
import ch.usi.inf.examples.confidential_word_count.common.config.DPConfig;
import com.google.auto.service.AutoService;

import java.util.*;
import java.util.stream.Collectors;

@AutoService(SplitSentenceService.class)
public final class SplitSentenceServiceImpl extends SplitSentenceVerifier {
    private final EnclaveLogger LOG = EnclaveLoggerFactory.getLogger(SplitSentenceServiceImpl.class);
    private long sequenceCounter = 0;
    private final String producerId = UUID.randomUUID().toString();

    @Override
    public SplitSentenceResponse splitImpl(SplitSentenceRequest request) throws SealedPayloadProcessingException, CipherInitializationException,  AADEncodingException {
        LOG.info("SplitSentenceServiceImpl: validated request received.");

        // decrypt the payload
        String jsonPayload = sealedPayload.decryptToString(request.body());

        // extract user_id from input AAD
        DecodedAAD inputAad = DecodedAAD.fromBytes(request.body().associatedData());
        Object userId = inputAad.attributes().get("user_id");
        if (userId == null && DPConfig.ENABLE_USER_LEVEL_PRIVACY) {
            LOG.warn("No user_id found in AAD for split request");
        }

        // extract sentence from the json body
        Map<String, Object> jsonMap = EnclaveJsonUtil.parseJson(jsonPayload);
        Object bodyObj = jsonMap.get("body");
        String jokeText = (bodyObj instanceof String) ? (String) bodyObj : "";
        if (jokeText.isEmpty()) {
            LOG.warn("Could not extract 'body' from JSON payload: {}", jsonPayload);
        }

        // compute sensitive operation
        //noinspection SimplifyStreamApiCallChains
        List<String> plainWords = Arrays.stream(jokeText.split("\\W+"))
                .map(word -> word.toLowerCase(Locale.ROOT).trim())
                .filter(word -> !word.isEmpty())
                .collect(Collectors.toList());

        // create default AAD specification for output words
        AADSpecificationBuilder aadBuilder = AADSpecification.builder()
                // NOTE: specify source and destination components for verification purposes
                .sourceComponent(TopologySpecification.Component.SENTENCE_SPLIT)
                .destinationComponent(TopologySpecification.Component.USER_CONTRIBUTION_BOUNDING)
                .put("producer_id", producerId);

        // NOTE: We need to encrypt each word separately as it will be handled alone
        // by the next services in the pipeline.
        List<EncryptedValue> encryptedWords = new ArrayList<>(plainWords.size());
        for (String plainWord : plainWords) {
            // append sequence number to AAD for protecting against replays
            long sequence = sequenceCounter++;
            aadBuilder.put("seq", sequence);

            // append user_id to AAD if user-level privacy is enabled
            if (DPConfig.ENABLE_USER_LEVEL_PRIVACY && userId != null) {
                aadBuilder.put("user_id", userId);
            }

            // encrypt the word with its AAD
            EncryptedValue payload = sealedPayload.encryptString(plainWord, aadBuilder.build());

            // store encrypted word
            encryptedWords.add(payload);
        }

        // return response to bolt
        return new SplitSentenceResponse(encryptedWords);
    }
}
