package ch.usi.inf.confidentialstorm.common.api;

import ch.usi.inf.confidentialstorm.common.api.model.SplitSentenceRequest;
import ch.usi.inf.confidentialstorm.common.api.model.SplitSentenceResponse;
import org.apache.teaclave.javasdk.common.annotations.EnclaveService;

@EnclaveService
public interface SplitSentenceService {
    SplitSentenceResponse split(SplitSentenceRequest request);
}
