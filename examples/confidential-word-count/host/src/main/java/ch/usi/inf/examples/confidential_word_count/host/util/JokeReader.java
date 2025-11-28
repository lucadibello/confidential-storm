package ch.usi.inf.examples.confidential_word_count.host.util;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public final class JokeReader {

    private static final Logger LOG = LoggerFactory.getLogger(JokeReader.class);
    private final ObjectMapper mapper;

    public JokeReader() {
        this.mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
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
                if (headerNode == null) {
                    continue;
                }
                byte[] aad = buildAadBytes(headerNode);
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

    private byte[] buildAadBytes(JsonNode headerNode) {
        String headerJson = headerNode.asText();
        return headerJson.getBytes(StandardCharsets.UTF_8);
    }
}
