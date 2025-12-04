package ch.usi.inf.confidentialstorm.enclave.crypto;

import ch.usi.inf.confidentialstorm.enclave.crypto.util.AADUtils;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class JsonUtilsTest {

    /**
     * Test to ensure that the AADUtils can correctly parse JSON strings as produced by Python's json.dumps with ensure_ascii=True.
     */
    @Test
    public void testAADUtilsParsePythonStyleJson() {
        // Python: json.dumps({"key": "val\u00fce"}, ensure_ascii=True) -> {"key": "val\u00fce"}
        // NOTE: \u00fc is ü
        String pythonJson = "{\"key\": \"val\\u00fce\", \"newline\": \"line1\\nline2\"}";
        byte[] aadBytes = pythonJson.getBytes(StandardCharsets.UTF_8);

        Map<String, Object> result = AADUtils.parseAadJson(aadBytes);

        assertEquals("val\u00fce", result.get("key"));
        assertEquals("line1\nline2", result.get("newline"));
    }

    /**
     * Test to ensure that the AADUtils can correctly parse various JSON strings.
     */
    @Test
    public void testAADUtilsParseSimple() {
        String json = "{\"a\": 1, \"b\": \"hello\"}";
        Map<String, Object> result = AADUtils.parseAadJson(json.getBytes(StandardCharsets.UTF_8));
        assertEquals(1L, result.get("a"));
        assertEquals("hello", result.get("b"));
    }

    /**
     * Test to ensure that the AADUtils can correctly parse spaces in JSON strings.
     */
    @Test
    public void testAADUtilsParseWithSpaces() {
        String json = "{ \"a\" : 123 , \"b\" : \"test\" }";
        Map<String, Object> result = AADUtils.parseAadJson(json.getBytes(StandardCharsets.UTF_8));
        assertEquals(123L, result.get("a"));
        assertEquals("test", result.get("b"));
    }

    /**
     * Test to ensure that the AADUtils can correctly parse various primitive JSON types: booleans, nulls, integers, and doubles.
     */
    @Test
    public void testAADUtilsParsePrimitives() {
        String json = "{\"boolTrue\": true, \"boolFalse\": false, \"nullVal\": null, \"intVal\": 42, \"doubleVal\": 3.14}";
        Map<String, Object> result = AADUtils.parseAadJson(json.getBytes(StandardCharsets.UTF_8));

        assertEquals(Boolean.TRUE, result.get("boolTrue"));
        assertEquals(Boolean.FALSE, result.get("boolFalse"));
        assertNull(result.get("nullVal"));
        assertEquals(42L, result.get("intVal"));
        assertEquals(3.14, result.get("doubleVal"));
    }

    /**
     * Test to ensure that the AADUtils can correctly parse escaped characters in JSON strings.
     */
    @Test
    public void testAADUtilsParseEscapes() {
        // JSON: {"str": "\t\n\\\""} -> in Java string: "{\"str\": \"\\t\\n\\\\\\\"\"}"
        String json = "{\"str\": \"\\t\\n\\\\\\\"\"}";
        Map<String, Object> result = AADUtils.parseAadJson(json.getBytes(StandardCharsets.UTF_8));

        String expected = "\t\n\\\"";
        assertEquals(expected, result.get("str"));
    }

    /**
     * Test to ensure that the AADUtils can correctly parse Unicode characters in JSON strings.
     */
    @Test
    public void testAADUtilsParseUnicode() {
        // JSON: {"unicode": "\u00e9"} -> é
        String json = "{\"unicode\": \"\\u00e9\"}";
        Map<String, Object> result = AADUtils.parseAadJson(json.getBytes(StandardCharsets.UTF_8));

        assertEquals("é", result.get("unicode"));
    }
}