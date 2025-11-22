package ch.usi.inf.confidentialstorm.enclave.util;

public final class EnclaveLoggerFactory {
    private EnclaveLoggerFactory() { }

    public static EnclaveLogger getLogger(Class<?> clazz) {
        return new EnclaveLoggerImpl(clazz);
    }

    private static class EnclaveLoggerImpl implements EnclaveLogger {
        private final Class<?> clazz;

        private enum Level {
            INFO,
            WARN,
            ERROR,
            DEBUG
        }

        EnclaveLoggerImpl(Class<?> clazz) {
            this.clazz = clazz;
        }

        private void log(Level level, String message) {
            String log = String.format("[%s] %s: %s", level.name(), clazz.getSimpleName(), message);
            if (level == Level.ERROR) {
                System.err.println(log);
            } else {
                System.out.println(log);
            }
        }

        private void log(Level level, String format, Object... args) {
            log(level, formatMessage(format, args));
        }

        private String formatMessage(String format, Object... args) {
            if (format == null || args == null || args.length == 0) {
                return format;
            }
            StringBuilder builder = new StringBuilder();
            int argIndex = 0;
            int cursor = 0;
            int placeholder;
            while (cursor < format.length() && argIndex < args.length) {
                placeholder = format.indexOf("{}", cursor);
                if (placeholder < 0) {
                    break;
                }
                builder.append(format, cursor, placeholder);
                builder.append(args[argIndex++]);
                cursor = placeholder + 2;
            }
            if (cursor < format.length()) {
                builder.append(format.substring(cursor));
            }
            while (argIndex < args.length) {
                builder.append(" ").append(args[argIndex++]);
            }
            return builder.toString();
        }

        @Override
        public void info(String message) {
            log(Level.INFO, message);
        }

        @Override
        public void info(String message, Object... args) {
            log(Level.INFO, message, args);
        }

        @Override
        public void warn(String message) {
            log(Level.WARN, message);
        }

        @Override
        public void warn(String message, Object... args) {
            log(Level.WARN, message, args);
        }

        @Override
        public void error(String message) {
            log(Level.ERROR, message);
        }

        @Override
        public void error(String message, Throwable t) {
            log(Level.ERROR, message);
            if (t != null) {
                t.printStackTrace(System.err);
            }
        }

        @Override
        public void error(String message, Object... args) {
            log(Level.ERROR, message, args);
        }

        @Override
        public void debug(String message) {
            log(Level.DEBUG, message);
        }

        @Override
        public void debug(String message, Object... args) {
            log(Level.DEBUG, message, args);
        }
    }
}
