package ch.usi.inf.confidentialstorm.common.api;

import ch.usi.inf.confidentialstorm.common.api.model.WordCountRequest;
import ch.usi.inf.confidentialstorm.common.api.model.WordCountResponse;
import org.apache.teaclave.javasdk.common.annotations.EnclaveService;

@EnclaveService
public interface WordCountService {
    WordCountResponse count(WordCountRequest request);
}
