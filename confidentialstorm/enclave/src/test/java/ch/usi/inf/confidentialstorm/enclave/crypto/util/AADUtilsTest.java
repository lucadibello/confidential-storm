package ch.usi.inf.confidentialstorm.enclave.crypto.util;

import ch.usi.inf.confidentialstorm.enclave.crypto.aad.AADSpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.DecodedAAD;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link AADUtils} parsing and integration with decoded AAD metadata.
 */
class AADUtilsTest {

    /**
     * Verifies that valid JSON AAD bytes are parsed into expected primitive and nested values.
     */
    @Test
    void parseAadJson_validJson_parsesValues() {
        String aadJson = "{\"seq\":42,\"producer_id\":\"task-1\",\"epoch\":5,\"nested\":{\"a\":1}}";

        Map<String, Object> parsed = AADUtils.parseAadJson(aadJson.getBytes(StandardCharsets.UTF_8));

        assertEquals(42L, parsed.get("seq"));
        assertEquals("task-1", parsed.get("producer_id"));
        assertEquals(5L, parsed.get("epoch"));
        assertTrue(parsed.get("nested") instanceof Map);
    }

    /**
     * Verifies that malformed JSON does not throw and instead yields an empty map.
     */
    @Test
    void parseAadJson_malformedJson_returnsEmptyMap() {
        byte[] malformed = "not-json".getBytes(StandardCharsets.UTF_8);

        Map<String, Object> parsed = AADUtils.parseAadJson(malformed);

        assertTrue(parsed.isEmpty());
    }

    /**
     * Verifies that decoding preserves sequence, producer id, and epoch when the values are present.
     */
    @Test
    void decodedAad_fromBytes_extractsKnownMetadata() {
        String aadJson = "{\"source\":\"spoutA\",\"destination\":\"boltB\",\"producer_id\":\"p-7\",\"seq\":99,\"epoch\":3,\"custom\":\"v\"}";

        DecodedAAD decodedAAD = DecodedAAD.fromBytes(aadJson.getBytes(StandardCharsets.UTF_8));

        assertEquals(99L, decodedAAD.sequenceNumber().orElseThrow());
        assertEquals("p-7", decodedAAD.producerId().orElseThrow());
        assertEquals(3, decodedAAD.epoch().orElseThrow());
    }

    /**
     * Verifies that missing numeric metadata fields are represented as absent optionals.
     */
    @Test
    void decodedAad_fromBytes_missingNumericFields_areAbsent() {
        AADSpecification aadSpecification = AADSpecification.builder()
                .put("custom", "value")
                .build();
        byte[] aadBytes = ch.usi.inf.confidentialstorm.common.util.EnclaveJsonUtil.serialize(aadSpecification.attributes());

        DecodedAAD decodedAAD = DecodedAAD.fromBytes(aadBytes);

        assertFalse(decodedAAD.sequenceNumber().isPresent());
        assertFalse(decodedAAD.epoch().isPresent());
    }
}
