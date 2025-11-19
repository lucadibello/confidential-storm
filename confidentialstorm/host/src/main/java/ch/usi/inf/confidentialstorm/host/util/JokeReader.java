package ch.usi.inf.confidentialstorm.host.util;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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
import java.util.Objects;
import java.util.TreeMap;

public final class JokeReader {

    public static final class Joke {
        public final long sequence;
        public final int id;
        public final String category;
        public final String rating;
        public final EncryptedValue body;

        public Joke(long sequence, int id, String category, String rating, EncryptedValue body) {
            this.sequence = sequence;
            this.id = id;
            this.category = category;
            this.rating = rating;
            this.body = body;
        }

        @Override
        public String toString() {
            return "Joke{id=%d, seq=%d, category='%s', rating='%s'}"
                    .formatted(id, sequence, category, rating);
        }
    }

    private final ObjectMapper mapper;

    public JokeReader() {
        this.mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public List<Joke> readAll(String jsonResourceName) throws IOException {
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
            List<Joke> jokes = new ArrayList<>();
            for (JsonNode entry : root) {
                JsonNode headerNode = entry.get("header");
                if (headerNode == null || !headerNode.isObject()) {
                    continue;
                }
                Map<String, Object> header = mapper.convertValue(headerNode, new TypeReference<>() {});
                byte[] aad = buildAadBytes(header);
                byte[] nonce = decodeBase64(entry.get("nonce"));
                byte[] ciphertext = decodeBase64(entry.get("ciphertext"));
                EncryptedValue body = new EncryptedValue(aad, nonce, ciphertext);

                long seq = toLong(header.get("seq"));
                int id = (int) toLong(header.get("id"));
                String category = Objects.toString(header.getOrDefault("category", "unknown"));
                String rating = Objects.toString(header.getOrDefault("rating", "unrated"));
                jokes.add(new Joke(seq, id, category, rating, body));
            }
            return jokes;
        }
    }

    private byte[] decodeBase64(JsonNode node) {
        if (node == null || !node.isTextual()) {
            throw new IllegalArgumentException("Expected base64 string");
        }
        return Base64.getDecoder().decode(node.asText());
    }

    private byte[] buildAadBytes(Map<String, Object> header) {
        Map<String, Object> sorted = new TreeMap<>(header);
        StringBuilder builder = new StringBuilder();
        builder.append('{');
        boolean first = true;
        for (Map.Entry<String, Object> entry : sorted.entrySet()) {
            if (!first) {
                builder.append(", ");
            }
            first = false;
            builder.append('"')
                    .append(escapeJson(entry.getKey()))
                    .append('"')
                    .append(": ")
                    .append(renderJsonValue(entry.getValue()));
        }
        builder.append('}');
        return builder.toString().getBytes(StandardCharsets.UTF_8);
    }

    private String renderJsonValue(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }
        return "\"" + escapeJson(value.toString()) + "\"";
    }

    private String escapeJson(String value) {
        StringBuilder escaped = new StringBuilder(value.length());
        for (char c : value.toCharArray()) {
            if (c == '"' || c == '\\') {
                escaped.append('\\');
            }
            escaped.append(c);
        }
        return escaped.toString();
    }

    private long toLong(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(Objects.toString(value));
    }

    // Tiny demo
    public static void main(String[] args) throws Exception {
        JokeReader reader = new JokeReader();
        List<Joke> jokes = reader.readAll("jokes.enc.json");
        System.out.println("Loaded " + jokes.size() + " jokes");
        if (!jokes.isEmpty()) {
            System.out.println(jokes.get(0));
        }
    }
}
