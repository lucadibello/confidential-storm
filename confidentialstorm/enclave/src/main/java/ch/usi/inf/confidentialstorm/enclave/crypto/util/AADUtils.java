package ch.usi.inf.confidentialstorm.enclave.crypto.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.Map;

public class AADUtils {


    public static Map<String, Object> parseAadJson(byte[] aadBytes) {
        // create mappers
        final ObjectMapper AAD_MAPPER = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<Map<String, Object>>() {};

        // parse AAD
        try {
            Map<String, Object> parsed = AAD_MAPPER.readValue(aadBytes, MAP_TYPE);
            if (parsed == null) {
                return new LinkedHashMap<>();
            }
            return new LinkedHashMap<>(parsed);
        } catch (Exception e) {
            // Be permissive inside the enclave: return an empty map on malformed AAD to avoid crashing serialization.
            return new LinkedHashMap<>();
        }
    }
}
