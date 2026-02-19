package ch.usi.inf.confidentialstorm.common.util;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Minimal JSON parser for use within Enclaves (Jackson is not available due to extremely high TCB).
 * Supports nested Objects (Maps) and Arrays (Lists).
 */
public final class EnclaveJsonUtil {

    private EnclaveJsonUtil() {
    }

    public static Map<String, Object> parseJson(String json) {
        try {
            Object result = parseValue(new StringParser(json));
            if (result instanceof Map) {
                //noinspection unchecked
                return (Map<String, Object>) result;
            }
            return new LinkedHashMap<>();
        } catch (Exception e) {
            return new LinkedHashMap<>();
        }
    }

    private static class StringParser {
        final String src;
        int idx;
        final int len;

        StringParser(String src) {
            this.src = Optional.ofNullable(src).orElse("").trim();
            this.idx = 0;
            this.len = this.src.length();
        }

        void skipWhitespace() {
            while (idx < len && Character.isWhitespace(src.charAt(idx))) {
                idx++;
            }
        }

        char peek() {
            skipWhitespace();
            if (idx >= len) return 0;
            return src.charAt(idx);
        }

        char next() {
            skipWhitespace();
            if (idx >= len) return 0;
            return src.charAt(idx++);
        }

        boolean consume(char c) {
            skipWhitespace();
            if (idx < len && src.charAt(idx) == c) {
                idx++;
                return true;
            }
            return false;
        }
    }

    private static Object parseValue(StringParser p) {
        char c = p.peek();
        if (c == '{') {
            return parseObject(p);
        } else if (c == '[') {
            return parseArray(p);
        } else if (c == '"') {
            return parseString(p);
        } else {
            return parseScalar(p);
        }
    }

    private static Map<String, Object> parseObject(StringParser p) {
        Map<String, Object> map = new LinkedHashMap<>();
        p.consume('{');
        while (p.peek() != '}' && p.peek() != 0) {
            String key = parseString(p);
            p.consume(':');
            Object value = parseValue(p);
            map.put(key, value);
            if (!p.consume(',')) {
                break;
            }
        }
        p.consume('}');
        return map;
    }

    private static List<Object> parseArray(StringParser p) {
        List<Object> list = new ArrayList<>();
        p.consume('[');
        while (p.peek() != ']' && p.peek() != 0) {
            list.add(parseValue(p));
            if (!p.consume(',')) {
                break;
            }
        }
        p.consume(']');
        return list;
    }

    private static String parseString(StringParser p) {
        if (!p.consume('"')) return "";
        int start = p.idx;
        StringBuilder sb = new StringBuilder();
        while (p.idx < p.len) {
            char c = p.src.charAt(p.idx++);
            if (c == '"') {
                return sb.toString();
            } else if (c == '\\') {
                if (p.idx >= p.len) break;
                char next = p.src.charAt(p.idx++);
                switch (next) {
                    case '"': sb.append('"'); break;
                    case '\\': sb.append('\\'); break;
                    case '/': sb.append('/'); break;
                    case 'b': sb.append('\b'); break;
                    case 'f': sb.append('\f'); break;
                    case 'n': sb.append('\n'); break;
                    case 'r': sb.append('\r'); break;
                    case 't': sb.append('\t'); break;
                    case 'u':
                        if (p.idx + 4 > p.len) break;
                        String hex = p.src.substring(p.idx, p.idx + 4);
                        p.idx += 4;
                        try {
                            sb.append((char) Integer.parseInt(hex, 16));
                        } catch (NumberFormatException e) { /* ignore */ }
                        break;
                    default: sb.append(next); break;
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private static Object parseScalar(StringParser p) {
        int start = p.idx;
        while (p.idx < p.len) {
            char c = p.src.charAt(p.idx);
            if (c == ',' || c == '}' || c == ']' || Character.isWhitespace(c)) {
                break;
            }
            p.idx++;
        }
        String raw = p.src.substring(start, p.idx);
        if ("null".equals(raw)) return null;
        if ("true".equals(raw)) return Boolean.TRUE;
        if ("false".equals(raw)) return Boolean.FALSE;
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException e) {
            try {
                return Double.parseDouble(raw);
            } catch (NumberFormatException e2) {
                return raw;
            }
        }
    }

    public static byte[] serialize(double value) {
        String strValue = Double.toString(value);
        return strValue.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] serialize(long value) {
        String strValue = Long.toString(value);
        return strValue.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] serialize(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
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
            serializeValue(sb, value);
        }
        sb.append('}');
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static void serializeValue(StringBuilder sb, Object value) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof Number || value instanceof Boolean) {
            sb.append(value);
        } else if (value instanceof Map) {
            sb.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                if (!first) sb.append(',');
                first = false;
                sb.append('"').append(escapeJson(String.valueOf(entry.getKey()))).append("\":");
                serializeValue(sb, entry.getValue());
            }
            sb.append('}');
        } else if (value instanceof List) {
            sb.append('[');
            boolean first = true;
            for (Object item : (List<?>) value) {
                if (!first) sb.append(',');
                first = false;
                serializeValue(sb, item);
            }
            sb.append(']');
        } else {
            sb.append('"').append(escapeJson(String.valueOf(value))).append('"');
        }
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
