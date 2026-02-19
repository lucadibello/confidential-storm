package ch.usi.inf.confidentialstorm.enclave.crypto.util;

import ch.usi.inf.confidentialstorm.common.util.EnclaveJsonUtil;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Utility class for AAD processing.
 */
public class AADUtils {

    /**
     * Parses the AAD JSON bytes into a map.
     *
     * @param aadBytes the AAD bytes
     * @return the map of AAD fields
     */
    public static Map<String, Object> parseAadJson(byte[] aadBytes) {
        try {
            return EnclaveJsonUtil.parseJson(new String(aadBytes, java.nio.charset.StandardCharsets.UTF_8));
        } catch (Exception e) {
            return new LinkedHashMap<>();
        }
    }
}
