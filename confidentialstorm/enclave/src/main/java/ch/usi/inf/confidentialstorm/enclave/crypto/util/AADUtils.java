package ch.usi.inf.confidentialstorm.enclave.crypto.util;

import ch.usi.inf.confidentialstorm.enclave.util.EnclaveJsonUtil;

import java.util.LinkedHashMap;
import java.util.Map;

public class AADUtils {

    public static Map<String, Object> parseAadJson(byte[] aadBytes) {
        try {
            return EnclaveJsonUtil.parseJson(new String(aadBytes, java.nio.charset.StandardCharsets.UTF_8));
        } catch (Exception e) {
            return new LinkedHashMap<>();
        }
    }
}
