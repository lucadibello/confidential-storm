package ch.usi.inf.confidentialstorm.common.crypto.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class AADUtils {

    private static final ObjectMapper AAD_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    public static Map<String, Object> parseAadJson(byte[] aadBytes) {
        try {
            Map<String, Object> parsed = AAD_MAPPER.readValue(aadBytes, MAP_TYPE);
            if (parsed == null) {
                return new LinkedHashMap<>();
            }
            return new LinkedHashMap<>(parsed);
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid AAD encoding", e);
        }
    }
}
