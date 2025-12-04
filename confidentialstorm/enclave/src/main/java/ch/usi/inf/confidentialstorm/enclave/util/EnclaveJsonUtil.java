package ch.usi.inf.confidentialstorm.enclave.util;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Minimal JSON parser for use within Enclaves (Jackson is not available due to extremely high TCB).
 */
public final class EnclaveJsonUtil {

    private EnclaveJsonUtil() {
    }

    public static Map<String, Object> parseJson(String json) {
        try {
            return parseSimpleObject(json);
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
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\') {
                i++;
                if (i >= s.length()) {
                    throw new IllegalArgumentException("Invalid JSON string: trailing backslash");
                }
                char next = s.charAt(i);
                switch (next) {
                    case '"':
                        sb.append('"');
                        break;
                    case '\\':
                        sb.append('\\');
                        break;
                    case '/':
                        sb.append('/');
                        break;
                    case 'b':
                        sb.append('\b');
                        break;
                    case 'f':
                        sb.append('\f');
                        break;
                    case 'n':
                        sb.append('\n');
                        break;
                    case 'r':
                        sb.append('\r');
                        break;
                    case 't':
                        sb.append('\t');
                        break;
                    case 'u':
                        if (i + 4 >= s.length()) {
                            throw new IllegalArgumentException("Invalid JSON string: incomplete unicode escape");
                        }
                        String hex = s.substring(i + 1, i + 5);
                        try {
                            sb.append((char) Integer.parseInt(hex, 16));
                        } catch (NumberFormatException e) {
                            throw new IllegalArgumentException("Invalid JSON string: invalid unicode escape " + hex);
                        }
                        i += 4;
                        break;
                    default:
                        sb.append(next);
                        break;
                }
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
        switch (raw) {
            case "null" -> {
                return null;
            }
            case "true" -> {
                return Boolean.TRUE;
            }
            case "false" -> {
                return Boolean.FALSE;
            }
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

    public static byte[] serialize(Map<String, Object> fields) {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        boolean first = true;
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            sb.append('"').append(escapeJson(entry.getKey())).append("\":");
            Object value = entry.getValue();
            if (value == null) {
                sb.append("null");
            } else if (value instanceof Number || value instanceof Boolean) {
                sb.append(value);
            } else {
                sb.append('"').append(escapeJson(String.valueOf(value))).append('"');
            }
        }
        sb.append('}');
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static String escapeJson(String input) {
        if (input == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder(input.length());
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            switch (c) {
                case '"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\b':
                    sb.append("\\b");
                    break;
                case '\f':
                    sb.append("\\f");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                default:
                    if (c < ' ' || c >= 0x7F) {
                        String hex = Integer.toHexString(c);
                        sb.append("\\u");
                        sb.append("0".repeat(4 - hex.length()));
                        sb.append(hex);
                    } else {
                        sb.append(c);
                    }
            }
        }
        return sb.toString();
    }
}
