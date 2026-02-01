package ch.usi.inf.examples.confidential_word_count.host.util;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.examples.confidential_word_count.common.api.spout.model.SealedJokeEntry;
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
        List<SealedJokeEntry> jokes = reader.readAll("jokes.enc.json");
        System.out.println("Loaded " + jokes.size() + " jokes");
        if (!jokes.isEmpty()) {
            System.out.println(jokes.get(0));
        }
    }

    /**
     * Reads all encrypted joke entries from a JSON resource.
     * Each entry contains separately encrypted userId and payload fields.
     *
     * @param jsonResourceName the name of the JSON resource file
     * @return list of SealedJokeEntry objects with separate encrypted userId and payload
     * @throws IOException if the resource cannot be read
     */
    public List<SealedJokeEntry> readAll(String jsonResourceName) throws IOException {
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
            List<SealedJokeEntry> encryptedJokes = new ArrayList<>();
            for (JsonNode entry : root) {
                // Parse separately encrypted userId and payload
                JsonNode userIdNode = entry.get("userId");
                JsonNode payloadNode = entry.get("payload");

                if (userIdNode == null || payloadNode == null) {
                    LOG.warn("Entry missing 'userId' or 'payload' field, skipping");
                    continue;
                }

                EncryptedValue encryptedUserId = parseEncryptedValue(userIdNode);
                EncryptedValue encryptedPayload = parseEncryptedValue(payloadNode);

                // Format: (payload, userId)
                encryptedJokes.add(new SealedJokeEntry(encryptedPayload, encryptedUserId));
            }
            return encryptedJokes;
        }
    }

    /**
     * Parses an EncryptedValue from a JSON node containing header, nonce, and ciphertext.
     */
    private EncryptedValue parseEncryptedValue(JsonNode node) {
        JsonNode headerNode = node.get("header");
        if (headerNode == null) {
            throw new IllegalArgumentException("Missing 'header' field in encrypted value");
        }
        byte[] aad = buildAadBytes(headerNode);
        byte[] nonce = decodeBase64(node.get("nonce"));
        byte[] ciphertext = decodeBase64(node.get("ciphertext"));
        return new EncryptedValue(aad, nonce, ciphertext);
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
