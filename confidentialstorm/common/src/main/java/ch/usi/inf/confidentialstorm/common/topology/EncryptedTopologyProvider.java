package ch.usi.inf.confidentialstorm.common.topology;

import ch.usi.inf.confidentialstorm.common.util.EnclaveJsonUtil;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Loads and decrypts the topology graph from the encrypted resource file bundled in the JAR.
 * <p>
 * The resource {@code /topology.graph.enc} is generated at build time by
 * {@code TopologyGraphGenerator} and baked into the enclave native image via GraalVM.
 * It is encrypted with AES-256-GCM to provide integrity protection: any tampering by
 * the untrusted host will cause authentication tag verification to fail, preventing the
 * enclave from loading a forged topology.
 * <p>
 * At construction time this class builds both a <em>forward</em> index
 * (source -> downstream neighbours) and a <em>reverse</em> index (destination -> upstream
 * neighbours) so that both {@link #getDownstream} and {@link #getUpstream} can be served
 * in O(1).
 * <p>
 * <b>Key management note:</b> The encryption key is currently hard-coded for demonstration
 * purposes. In production, it must be provisioned via Intel SGX remote attestation or
 * SGX sealed storage so that only the genuine enclave binary can decrypt it.
 */
public class EncryptedTopologyProvider implements TopologyProvider {

    // FIXME: In a real SGX setting, this key must be provisioned via remote attestation or sealed storage.
    // NOTE: For now, we use a hardcoded key. "TrustMeI'mATopologyKey1234567890" (32 bytes)
    private static final String HARDCODED_KEY_BASE64 = "VHJ1c3RNZUknbUFUb3BvbG9neUtleTEyMzQ1Njc4OTA=";
    private static final String RESOURCE_NAME = "/topology.graph.enc";

    /**
     * Forward adjacency list: source component name -> list of downstream component names.
     */
    private final Map<String, List<String>> forwardGraph;

    /**
     * Reverse adjacency list: destination component name -> list of upstream component names.
     */
    private final Map<String, List<String>> reverseGraph;

    /**
     * Constructs an {@code EncryptedTopologyProvider} by loading and decrypting the topology
     * resource from the classpath.
     *
     * @throws RuntimeException if the resource is absent, the file is malformed, or AES-GCM
     *                          authentication tag verification fails (indicating tampering)
     */
    public EncryptedTopologyProvider() {
        this.forwardGraph = loadAndDecrypt();
        this.reverseGraph = buildReverseIndex(forwardGraph);
    }

    /**
     * Loads and decrypts the topology graph from the encrypted resource bundled in the JAR.
     *
     * @return the forward adjacency list mapping each source component name to its ordered list
     *         of downstream component names
     * @throws RuntimeException if the resource is absent, the file is malformed, or decryption
     *                          (including AES-GCM authentication tag verification) fails
     */
    @SuppressWarnings("unchecked")
    private Map<String, List<String>> loadAndDecrypt() {
        try (InputStream is = getClass().getResourceAsStream(RESOURCE_NAME)) {
            if (is == null) {
                // Fail-closed: missing topology file means route validation cannot be performed.
                // Do NOT silently disable security checks -- abort enclave startup instead.
                throw new IllegalStateException(
                        RESOURCE_NAME + " not found in classpath. " +
                        "Run TopologyGraphGenerator during the build to produce this file " +
                        "and place it in the enclave's src/main/resources directory.");
            }

            byte[] allBytes = is.readAllBytes();

            // Wire format: [12-byte IV][AES-256-GCM ciphertext + 16-byte auth tag]
            if (allBytes.length < 12) {
                throw new IllegalArgumentException(RESOURCE_NAME + " is too short to contain a valid IV");
            }

            byte[] iv = new byte[12];
            System.arraycopy(allBytes, 0, iv, 0, 12);

            int cipherTextLen = allBytes.length - 12;
            byte[] cipherText = new byte[cipherTextLen];
            System.arraycopy(allBytes, 12, cipherText, 0, cipherTextLen);

            // Decrypt and verify integrity via GCM authentication tag
            byte[] keyBytes = Base64.getDecoder().decode(HARDCODED_KEY_BASE64);
            SecretKeySpec keySpec = new SecretKeySpec(keyBytes, "AES");
            GCMParameterSpec gcmSpec = new GCMParameterSpec(128, iv);

            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, keySpec, gcmSpec);

            byte[] plainText = cipher.doFinal(cipherText);
            String json = new String(plainText, StandardCharsets.UTF_8);

            // Parse JSON: { "adjacencyList": { "src": ["dst1", "dst2"] } }
            Map<String, Object> root = EnclaveJsonUtil.parseJson(json);

            Object adjObj = root.get("adjacencyList");
            if (!(adjObj instanceof Map)) {
                throw new IllegalStateException(
                        "Decrypted topology JSON is missing or has an invalid 'adjacencyList' key. " +
                        "Re-run TopologyGraphGenerator to regenerate " + RESOURCE_NAME);
            }

            Map<String, Object> rawMap = (Map<String, Object>) adjObj;
            Map<String, List<String>> result = new HashMap<>();

            for (Map.Entry<String, Object> entry : rawMap.entrySet()) {
                Object val = entry.getValue();
                if (val instanceof List<?> rawList) {
                    List<String> strList = new ArrayList<>();
                    for (Object item : rawList) {
                        if (item != null) {
                            strList.add(item.toString());
                        }
                    }
                    result.put(entry.getKey().toLowerCase(Locale.ROOT), Collections.unmodifiableList(strList));
                }
            }

            return Collections.unmodifiableMap(result);

        } catch (Exception e) {
            throw new RuntimeException("Failed to load or decrypt topology configuration. Integrity check failed.", e);
        }
    }

    /**
     * Builds a reverse adjacency list from the given forward adjacency list.
     * For every edge {@code src -> dst} in the forward graph, an edge {@code dst -> src}
     * is added to the reverse graph.
     *
     * @param forward the forward adjacency list to invert
     * @return an unmodifiable reverse adjacency list
     */
    private static Map<String, List<String>> buildReverseIndex(Map<String, List<String>> forward) {
        Map<String, List<String>> reverse = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : forward.entrySet()) {
            String src = entry.getKey();
            for (String dst : entry.getValue()) {
                reverse.computeIfAbsent(dst.toLowerCase(Locale.ROOT), k -> new ArrayList<>()).add(src);
            }
        }
        // Make all lists unmodifiable
        Map<String, List<String>> result = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : reverse.entrySet()) {
            result.put(entry.getKey(), Collections.unmodifiableList(entry.getValue()));
        }
        return Collections.unmodifiableMap(result);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Looks up the component by name (case-insensitive) in the reverse adjacency list.
     * Returns an empty list if no upstream neighbours are registered.
     */
    @Override
    public List<TopologySpecification.Component> getUpstream(TopologySpecification.Component component) {
        return lookupNeighbours(reverseGraph, component);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Looks up the component by name (case-insensitive) in the forward adjacency list.
     * Returns an empty list if the component has no registered downstream neighbours.
     */
    @Override
    public List<TopologySpecification.Component> getDownstream(TopologySpecification.Component component) {
        return lookupNeighbours(forwardGraph, component);
    }

    /**
     * Looks up the neighbours of a component in the given adjacency map
     * using case-insensitive matching (keys are normalized to lowercase at load time).
     *
     * @param adjacency the adjacency map to search
     * @param component the component to look up
     * @return the list of neighbour components, or an empty list if none are found
     */
    private List<TopologySpecification.Component> lookupNeighbours(
            Map<String, List<String>> adjacency,
            TopologySpecification.Component component) {

        if (adjacency.isEmpty()) return Collections.emptyList();

        String key = component.getName().toLowerCase(Locale.ROOT);
        List<String> neighbours = adjacency.get(key);

        if (neighbours == null) return Collections.emptyList();

        return neighbours.stream()
                .map(TopologySpecification.Component::of)
                .collect(Collectors.toList());
    }
}
