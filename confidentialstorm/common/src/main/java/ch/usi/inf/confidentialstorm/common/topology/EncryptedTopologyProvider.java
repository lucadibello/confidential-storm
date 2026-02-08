package ch.usi.inf.confidentialstorm.common.topology;

import ch.usi.inf.confidentialstorm.common.util.EnclaveJsonUtil;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.ArrayList;

public class EncryptedTopologyProvider implements TopologyProvider {
    
    // FIXME: In a real SGX setting, this key would be provisioned via remote attestation or sealed storage.
    // NOTE: For now , we use a hardcoded key. "TrustMeI'mATopologyKey1234567890" (32 bytes)
    private static final String HARDCODED_KEY_BASE64 = "VHJ1c3RNZUknbUFUb3BvbG9neUtleTEyMzQ1Njc4OTA="; 
    private static final String RESOURCE_NAME = "/topology.graph.enc";
    
    private final Map<String, List<String>> graph;

    public EncryptedTopologyProvider() {
        this.graph = loadAndDecrypt();
    }

    @SuppressWarnings("unchecked")
    private Map<String, List<String>> loadAndDecrypt() {
        try (InputStream is = getClass().getResourceAsStream(RESOURCE_NAME)) {
            if (is == null) {
                // Warning: Security fallback.
                // In production, this should likely throw an exception to prevent unverified execution.
                System.err.println("WARN: " + RESOURCE_NAME + " not found. Topology verification disabled (returning empty graph).");
                return Collections.emptyMap();
            }

            byte[] allBytes = is.readAllBytes();
            
            // Extract IV (12 bytes)
            if (allBytes.length < 12) {
                throw new IllegalArgumentException("File too short to contain IV");
            }
            byte[] iv = new byte[12];
            System.arraycopy(allBytes, 0, iv, 0, 12);
            
            // Extract Ciphertext
            int cipherTextLen = allBytes.length - 12;
            byte[] cipherText = new byte[cipherTextLen];
            System.arraycopy(allBytes, 12, cipherText, 0, cipherTextLen);
            
            // Decrypt
            byte[] keyBytes = Base64.getDecoder().decode(HARDCODED_KEY_BASE64);
            SecretKeySpec keySpec = new SecretKeySpec(keyBytes, "AES");
            GCMParameterSpec gcmSpec = new GCMParameterSpec(128, iv);
            
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, keySpec, gcmSpec);
            
            byte[] plainText = cipher.doFinal(cipherText);
            String json = new String(plainText, StandardCharsets.UTF_8);
            
            // Use EnclaveJsonUtil (minimal parser)
            Map<String, Object> root = EnclaveJsonUtil.parseJson(json);
            
            // The JSON structure matches TopologyGraph: { "adjacencyList": { "src": ["dst1", "dst2"] } }
            if (root.containsKey("adjacencyList")) {
                Object adjObj = root.get("adjacencyList");
                if (adjObj instanceof Map) {
                    Map<String, Object> rawMap = (Map<String, Object>) adjObj;
                    Map<String, List<String>> result = new java.util.HashMap<>();
                    
                    for (Map.Entry<String, Object> entry : rawMap.entrySet()) {
                        Object val = entry.getValue();
                        if (val instanceof List) {
                            List<?> rawList = (List<?>) val;
                            List<String> strList = new ArrayList<>();
                            for (Object item : rawList) {
                                if (item != null) {
                                    strList.add(item.toString());
                                }
                            }
                            result.put(entry.getKey(), strList);
                        }
                    }
                    return result;
                }
            }
            
            return Collections.emptyMap();
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to load or decrypt topology configuration. Integrity check failed.", e);
        }
    }

    @Override
    public List<TopologySpecification.Component> getDownstream(TopologySpecification.Component component) {
        if (graph == null || graph.isEmpty()) return Collections.emptyList();
        
        String key = component.getName();
        List<String> downstreams = graph.get(key);
        
        if (downstreams == null) {
            downstreams = graph.get(key.toLowerCase(Locale.ROOT));
        }

        if (downstreams == null) return Collections.emptyList();
        
        return downstreams.stream()
                .map(TopologySpecification.Component::of)
                .collect(Collectors.toList());
    }
}
