package ch.usi.inf.confidentialstorm.enclave.crypto.util;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public class AADUtils {


    public static Map<String, Object> parseAadJson(byte[] aadBytes) {
        try {
            return parseSimpleObject(new String(aadBytes, java.nio.charset.StandardCharsets.UTF_8));
        } catch (Exception e) {
            return new LinkedHashMap<>();
        }
    }

    private static Map<String, Object> parseSimpleObject(String json) {
        Map<String, Object> result = new LinkedHashMap<>();
        String trimmed = Optional.ofNullable(json).orElse("").trim();
        if (trimmed.isEmpty() || "{}".equals(trimmed)) {
            return result;
        }

        if (trimmed.charAt(0) == '{' && trimmed.charAt(trimmed.length() - 1) == '}') {
            trimmed = trimmed.substring(1, trimmed.length() - 1);
        }

        int i = 0;
        int len = trimmed.length();
        while (i < len) {
            i = skipWhitespace(trimmed, i);
            if (i >= len) {
                break;
            }
            if (trimmed.charAt(i) == ',') {
                i++;
                continue;
            }
            if (trimmed.charAt(i) != '"') {
                return result;
            }
            int keyEnd = findStringEnd(trimmed, i + 1);
            if (keyEnd < 0) {
                return result;
            }
            String key = unescape(trimmed.substring(i + 1, keyEnd));
            i = skipWhitespace(trimmed, keyEnd + 1);
            if (i >= len || trimmed.charAt(i) != ':') {
                return result;
            }
            i = skipWhitespace(trimmed, i + 1);
            if (i >= len) {
                return result;
            }
            char valueStart = trimmed.charAt(i);
            Object value;
            if (valueStart == '"') {
                int valueEnd = findStringEnd(trimmed, i + 1);
                if (valueEnd < 0) {
                    return result;
                }
                value = unescape(trimmed.substring(i + 1, valueEnd));
                i = valueEnd + 1;
            } else {
                int valueEnd = i;
                while (valueEnd < len && trimmed.charAt(valueEnd) != ',') {
                    valueEnd++;
                }
                String raw = trimmed.substring(i, valueEnd).trim();
                value = parseScalar(raw);
                i = valueEnd;
            }
            result.put(key, value);
        }
        return result;
    }

    private static int skipWhitespace(String s, int idx) {
        int i = idx;
        while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
            i++;
        }
        return i;
    }

    private static int findStringEnd(String s, int start) {
        for (int i = start; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\') {
                i++; // skip escaped character
            } else if (c == '"') {
                return i;
            }
        }
        return -1;
    }

    private static String unescape(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        boolean escaping = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (escaping) {
                sb.append(c);
                escaping = false;
            } else if (c == '\\') {
                escaping = true;
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private static Object parseScalar(String raw) {
        if (raw == null || raw.isEmpty()) {
            return "";
        }
        // try long
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException ignored) {
        }
        // try double
        try {
            return Double.parseDouble(raw);
        } catch (NumberFormatException ignored) {
        }
        // fallback string
        return raw;
    }
}
