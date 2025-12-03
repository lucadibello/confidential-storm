package ch.usi.inf.confidentialstorm.enclave.crypto;

import ch.usi.inf.confidentialstorm.enclave.crypto.aad.AADSpecification;
import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SealedPayloadSerializationTest {

    /**
     * This test ensures that special characters in the AAD JSON are properly escaped.
     * @throws Exception if any error occurs during encryption
     */
    @Test
    public void testSealedPayloadSerializationEscaping() throws Exception {
        SealedPayload sealer = SealedPayload.fromConfig();
        
        String specialString = "Line1\nLine2\tTabbed\"Quoted\"";
        
        EncryptedValue encrypted = sealer.encryptString("example-data-for-testing",
            AADSpecification.builder()
                .put("special", specialString)
                .build()
        );
        
        byte[] aadBytes = encrypted.associatedData();
        String aadJson = new String(aadBytes, StandardCharsets.UTF_8);
        
        // Check that it contains escaped characters.
        // We want to check for literal \n, \t, \" in the JSON string.
        // In Java source, \\ represents a single backslash.
        assertTrue(aadJson.contains("\\n"), "Should contain escaped newline");
        assertTrue(aadJson.contains("\\t"), "Should contain escaped tab");
        assertTrue(aadJson.contains("\\\""), "Should contain escaped quote");
        
        // Check that it does NOT contain actual newline characters
        assertEquals(-1, aadJson.indexOf('\n'), "Should not contain raw newline");
        assertEquals(-1, aadJson.indexOf('\t'), "Should not contain raw tab");
    }

    /**
     * This test ensures that Unicode characters in the AAD JSON are properly escaped.
     * @throws Exception if any error occurs during encryption
     */
    @Test
    public void testSealedPayloadSerializationUnicode() throws Exception {
        SealedPayload sealer = SealedPayload.fromConfig();
        
        // "Mosé" -> 'é' is a unicode character (é = U+00E9)
        String unicodeString = "Mosé";
        
        EncryptedValue encrypted = sealer.encryptString("data", 
             AADSpecification.builder().put("name", unicodeString).build());
             
        String aadJson = new String(encrypted.associatedData(), StandardCharsets.UTF_8);

        // ensure that the Unicode character is escaped in the JSON
        assertTrue(aadJson.contains("\\u00e9"), "Should contain unicode escape for e-acute");
    }
}
