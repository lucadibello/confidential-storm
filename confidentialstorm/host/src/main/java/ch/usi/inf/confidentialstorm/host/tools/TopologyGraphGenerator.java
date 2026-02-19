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
 * Build-time tool that extracts the Storm topology graph from a class annotated with
 * {@link ConfidentialTopologyBuilder}, serialises it as JSON, and writes an AES-256-GCM
 * encrypted representation to a file.
 * <p>
 * The encrypted file ({@code topology.graph.enc}) must be placed in the enclave module's
 * {@code src/main/resources} directory so that GraalVM bakes it into the native image.
 * At runtime, {@code EncryptedTopologyProvider} loads and decrypts it to answer topology
 * routing queries without relying on any external file system access.
 * <p>
 * <b>Wire format:</b> {@code [12-byte random IV][AES-256-GCM ciphertext + 16-byte auth tag]}
 * <p>
 * <b>JSON format:</b>
 * <pre>{@code {"adjacencyList":{"source-component":["dest1","dest2"]}}}</pre>
 * <p>
 * <b>Key management note:</b> The encryption key is currently hard-coded for demonstration
 * purposes. In production, the same key must be available to both this generator (build time)
 * and the enclave (runtime via remote attestation / SGX sealed storage).
 *
 * <p>Invoked by {@code exec-maven-plugin} during the host module's {@code process-classes} phase.
 */
public class TopologyGraphGenerator {

    // FIXME: In a real SGX setting, this key must be provisioned via remote attestation or sealed storage.
    // NOTE: For now, we use a hardcoded key. "TrustMeI'mATopologyKey1234567890" (32 bytes)
    private static final String HARDCODED_KEY_BASE64 = "VHJ1c3RNZUknbUFUb3BvbG9neUtleTEyMzQ1Njc4OTA=";

    /**
     * Entry point for the topology graph generator.
     * <p>
     * Expects exactly two arguments:
     * <ol>
     *   <li>The fully-qualified name of the topology class that contains a static method
     *       annotated with {@link ConfidentialTopologyBuilder}.</li>
     *   <li>The output file path where the encrypted graph will be written.</li>
     * </ol>
     *
     * @param args command-line arguments: {@code <TopologyClass> <OutputFile>}
     * @throws Exception if the topology class cannot be loaded, no annotated builder method is found,
     *                   the topology cannot be constructed, or I/O or encryption fails
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: TopologyGraphGenerator <FullyQualifiedTopologyClass> <OutputFile>");
            System.exit(1);
        }
        String className = args[0];
        String outputFile = args[1];

        // 1. Load class and find the @ConfidentialTopologyBuilder-annotated static method
        Class<?> topologyClass = Class.forName(className);
        Method builderMethod = null;
        for (Method m : topologyClass.getDeclaredMethods()) {
            if (m.isAnnotationPresent(ConfidentialTopologyBuilder.class)) {
                if (builderMethod != null) {
                    throw new IllegalStateException(
                            "Multiple methods annotated with @ConfidentialTopologyBuilder found in " + className);
                }
                builderMethod = m;
            }
        }

        if (builderMethod == null) {
            throw new IllegalStateException(
                    "No static method annotated with @ConfidentialTopologyBuilder found in " + className);
        }

        System.out.println("Found builder method: " + builderMethod.getName());

        // 2. Invoke the method to obtain the TopologyBuilder and build the StormTopology
        TopologyBuilder builder = (TopologyBuilder) builderMethod.invoke(null);
        StormTopology topology = builder.createTopology();

        // 3. Extract the directed adjacency list (source component -> list of destination components)
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

        // Sort destination lists for deterministic output across builds
        for (List<String> dests : adjacencyList.values()) {
            Collections.sort(dests);
        }

        // 4. Serialize to JSON using safe escaping to prevent injection via component names
        String json = buildJson(adjacencyList);
        System.out.println("Generated graph JSON: " + json);

        // 5. Encrypt with AES-256-GCM (random IV per run for semantic security)
        byte[] plainText = json.getBytes(StandardCharsets.UTF_8);
        byte[] keyBytes = Base64.getDecoder().decode(HARDCODED_KEY_BASE64);

        byte[] iv = new byte[12];
        new SecureRandom().nextBytes(iv);

        SecretKeySpec keySpec = new SecretKeySpec(keyBytes, "AES");
        GCMParameterSpec gcmSpec = new GCMParameterSpec(128, iv);

        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, gcmSpec);

        byte[] cipherText = cipher.doFinal(plainText);

        // 6. Write wire format: IV (12 bytes) || ciphertext (with embedded 16-byte GCM tag)
        try (FileOutputStream fos = new FileOutputStream(outputFile)) {
            fos.write(iv);
            fos.write(cipherText);
        }

        System.out.println("Encrypted graph exported to: " + outputFile);

        // Shut down Log4j2 explicitly so its background scheduler thread does not linger
        // after main() returns and cause exec-maven-plugin to print thread-interruption warnings.
        // Log4j2 is pulled in transitively by Storm (provided scope) and starts a daemon thread
        // even when no logging calls are made.
        shutdownLog4j2();
    }

    /**
     * Shuts down the Log4j2 logging system via reflection to avoid a hard compile-time dependency
     * on {@code log4j-core}, which is only present on the runtime classpath via Storm's transitive
     * dependencies.
     * <p>
     * Stops the current {@code LoggerContext}, which terminates Log4j2's background scheduler
     * thread ({@code Log4j2-TF-1-Scheduled-*}) that would otherwise linger after {@code main()}
     * returns and cause exec-maven-plugin thread-interruption warnings.
     */
    private static void shutdownLog4j2() {
        try {
            // Obtain the current LoggerContext via LogManager (log4j-api, always on classpath)
            Class<?> logManagerClass = Class.forName("org.apache.logging.log4j.LogManager");
            java.lang.reflect.Method getContext = logManagerClass.getMethod("getContext", boolean.class);
            Object context = getContext.invoke(null, false);

            // Call LoggerContext.stop(timeout, unit) to cleanly drain and terminate background threads
            java.lang.reflect.Method stop = context.getClass()
                    .getMethod("stop", long.class, java.util.concurrent.TimeUnit.class);
            stop.invoke(context, 5L, java.util.concurrent.TimeUnit.SECONDS);
        } catch (Exception ignored) {
            // Log4j2 not present or already shut down — nothing to do
        }
    }

    /**
     * Serialises the adjacency list to a JSON string, escaping all string values to prevent
     * injection in case component names contain backslashes or double-quote characters.
     *
     * @param adjacencyList the topology adjacency list to serialise
     * @return the JSON representation of the graph
     */
    private static String buildJson(Map<String, List<String>> adjacencyList) {
        List<String> sources = new ArrayList<>(adjacencyList.keySet());
        Collections.sort(sources);

        StringBuilder json = new StringBuilder("{\"adjacencyList\":{");
        boolean firstKey = true;
        for (String src : sources) {
            if (!firstKey) json.append(",");
            firstKey = false;
            json.append("\"").append(jsonEscape(src)).append("\":[");

            List<String> dests = adjacencyList.get(src);
            boolean firstVal = true;
            for (String dst : dests) {
                if (!firstVal) json.append(",");
                firstVal = false;
                json.append("\"").append(jsonEscape(dst)).append("\"");
            }
            json.append("]");
        }
        json.append("}}");
        return json.toString();
    }

    /**
     * Escapes a string value for safe inclusion inside a JSON double-quoted string.
     * Handles backslash, double-quote, and the standard JSON control characters.
     *
     * @param value the raw string to escape
     * @return the escaped string, safe for embedding between {@code "} delimiters
     */
    private static String jsonEscape(String value) {
        StringBuilder sb = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"'  -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.toString();
    }
}
