package ch.usi.inf.confidentialstorm.host.tools;

import ch.usi.inf.confidentialstorm.common.annotation.ConfidentialTopologyBuilder;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.FileOutputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.*;

/**
 * Generic tool to export the topology graph from any topology class annotated with @ConfidentialTopologyBuilder.
 */
public class TopologyGraphGenerator {
    
    // "TrustMeI'mATopologyKey1234567890" (32 bytes) 
    private static final String HARDCODED_KEY_BASE64 = "VHJ1c3RNZUknbUFUb3BvbG9neUtleTEyMzQ1Njc4OTA=";

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: TopologyGraphGenerator <FullyQualifiedTopologyClass> <OutputFile>");
            System.exit(1);
        }
        String className = args[0];
        String outputFile = args[1];

        // 1. Load Class and Find Builder Method
        Class<?> topologyClass = Class.forName(className);
        Method builderMethod = null;
        for (Method m : topologyClass.getDeclaredMethods()) {
            if (m.isAnnotationPresent(ConfidentialTopologyBuilder.class)) {
                if (builderMethod != null) {
                    throw new IllegalStateException("Multiple methods annotated with @ConfidentialTopologyBuilder found in " + className);
                }
                builderMethod = m;
            }
        }
        
        if (builderMethod == null) {
             throw new IllegalStateException("No static method annotated with @ConfidentialTopologyBuilder found in " + className);
        }
        
        System.out.println("Found builder method: " + builderMethod.getName());

        // 2. Invoke Method to get TopologyBuilder
        TopologyBuilder builder = (TopologyBuilder) builderMethod.invoke(null);
        StormTopology topology = builder.createTopology();

        // 3. Extract Graph (Source -> [Destinations])
        Map<String, List<String>> adjacencyList = new HashMap<>();

        Map<String, Bolt> bolts = topology.get_bolts();
        for (Map.Entry<String, Bolt> entry : bolts.entrySet()) {
            String destBoltId = entry.getKey();
            Bolt bolt = entry.getValue();
            
            Map<GlobalStreamId, Grouping> inputs = bolt.get_common().get_inputs();
            for (GlobalStreamId streamId : inputs.keySet()) {
                String sourceComponentId = streamId.get_componentId();
                adjacencyList.computeIfAbsent(sourceComponentId, k -> new ArrayList<>()).add(destBoltId);
            }
        }
        
        // Ensure lists are sorted for determinism
        for (List<String> dests : adjacencyList.values()) {
            Collections.sort(dests);
        }

        // 4. Serialize to JSON (Manual serialization)
        StringBuilder json = new StringBuilder();
        json.append("{\"adjacencyList\":{");
        boolean firstKey = true;
        
        List<String> sources = new ArrayList<>(adjacencyList.keySet());
        Collections.sort(sources);
        
        for (String src : sources) {
            if (!firstKey) json.append(",");
            firstKey = false;
            json.append("\"" + src + "\":[");
            
            List<String> dests = adjacencyList.get(src);
            boolean firstVal = true;
            for (String dst : dests) {
                if (!firstVal) json.append(",");
                firstVal = false;
                json.append("\"" + dst + "\"");
            }
            json.append("]");
        }
        json.append("}}");
        
        System.out.println("Generated Graph JSON: " + json);

        // 5. Encrypt
        byte[] plainText = json.toString().getBytes(StandardCharsets.UTF_8);
        byte[] keyBytes = Base64.getDecoder().decode(HARDCODED_KEY_BASE64);
        
        // Generate IV
        byte[] iv = new byte[12];
        new SecureRandom().nextBytes(iv);
        
        SecretKeySpec keySpec = new SecretKeySpec(keyBytes, "AES");
        GCMParameterSpec gcmSpec = new GCMParameterSpec(128, iv);
        
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, gcmSpec);
        
        byte[] cipherText = cipher.doFinal(plainText);
        
        // 6. Write to file: IV + Ciphertext
        try (FileOutputStream fos = new FileOutputStream(outputFile)) {
            fos.write(iv);
            fos.write(cipherText);
        }
        
        System.out.println("Encrypted graph exported to: " + outputFile);
    }
}
