package ch.usi.inf.confidentialstorm.host.util;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public final class JokeReader {

    private final ObjectMapper mapper;
    private final ObjectMapper aadMapper;

    public JokeReader() {
        this.mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.aadMapper = JsonMapper.builder()
                .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
                .build();
    }

    public List<EncryptedValue> readAll(String jsonResourceName) throws IOException {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream resourceStream = classloader.getResourceAsStream(jsonResourceName);
        if (resourceStream == null) {
            throw new FileNotFoundException("Resource not found: " + jsonResourceName);
        }
        try (BufferedReader br = new BufferedReader(new InputStreamReader(resourceStream, StandardCharsets.UTF_8))) {
            JsonNode root = mapper.readTree(br);
            if (!root.isArray()) {
                throw new IllegalArgumentException("Expected JSON array for jokes dataset");
            }
            List<EncryptedValue> encryptedJokes = new ArrayList<>();
            for (JsonNode entry : root) {
                JsonNode headerNode = entry.get("header");
                if (headerNode == null || !headerNode.isObject()) {
                    continue;
                }
                Map<String, Object> header = mapper.convertValue(headerNode, new TypeReference<>() {
                });
                byte[] aad = buildAadBytes(header);
                byte[] nonce = decodeBase64(entry.get("nonce"));
                byte[] ciphertext = decodeBase64(entry.get("ciphertext"));

                // wrap fields inside EncryptedValue object
                EncryptedValue joke = new EncryptedValue(aad, nonce, ciphertext);
                encryptedJokes.add(joke);
            }
            return encryptedJokes;
        }
    }

    private byte[] decodeBase64(JsonNode node) {
        if (node == null || !node.isTextual()) {
            throw new IllegalArgumentException("Expected base64 string");
        }
        return Base64.getDecoder().decode(node.asText());
    }

    private byte[] buildAadBytes(Map<String, Object> header) {
        try {
            return aadMapper.writeValueAsBytes(header);
        } catch (IOException e) {
            throw new RuntimeException("Cannot serialize AAD", e);
        }
    }

    // Tiny demo
    public static void main(String[] args) throws Exception {
        JokeReader reader = new JokeReader();
        List<EncryptedValue> jokes = reader.readAll("jokes.enc.json");
        System.out.println("Loaded " + jokes.size() + " jokes");
        if (!jokes.isEmpty()) {
            System.out.println(jokes.get(0));
        }
    }
}
