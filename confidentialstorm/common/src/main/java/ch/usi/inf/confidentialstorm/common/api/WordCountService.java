package ch.usi.inf.confidentialstorm.common.api;

import ch.usi.inf.confidentialstorm.common.model.WordCountRequest;
import ch.usi.inf.confidentialstorm.common.model.WordCountResponse;
import org.apache.teaclave.javasdk.common.annotations.EnclaveService;

@EnclaveService
public interface WordCountService {
    WordCountResponse count(WordCountRequest request);
}
