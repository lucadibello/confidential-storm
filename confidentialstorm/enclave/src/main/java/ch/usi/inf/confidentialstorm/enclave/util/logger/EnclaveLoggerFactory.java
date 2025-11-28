package ch.usi.inf.confidentialstorm.enclave.util.logger;

import ch.usi.inf.confidentialstorm.enclave.EnclaveConfig;

public final class EnclaveLoggerFactory {
    private EnclaveLoggerFactory() {
    }

    public static EnclaveLogger getLogger(Class<?> clazz) {
        return new EnclaveLoggerImpl(clazz);
    }

    private record EnclaveLoggerImpl(Class<?> clazz) implements EnclaveLogger {
        private static final LogLevel CONFIGURED_LEVEL = resolveLogLevel();

        private static LogLevel resolveLogLevel() {
            return EnclaveConfig.LOG_LEVEL;
        }

        private boolean isEnabled(LogLevel level) {
            return level.getPriority() >= CONFIGURED_LEVEL.getPriority();
        }

        private void log(LogLevel level, String message) {
            if (!isEnabled(level)) {
                return;
            }
            String log = String.format("[%s] %s: %s", level.name(), clazz.getSimpleName(), message);
            if (level == LogLevel.ERROR) {
                System.err.println(log);
            } else {
                System.out.println(log);
            }
        }

        private void log(LogLevel level, String format, Object... args) {
            if (!isEnabled(level)) {
                return;
            }
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
            log(LogLevel.INFO, message);
        }

        @Override
        public void info(String message, Object... args) {
            log(LogLevel.INFO, message, args);
        }

        @Override
        public void warn(String message) {
            log(LogLevel.WARN, message);
        }

        @Override
        public void warn(String message, Object... args) {
            log(LogLevel.WARN, message, args);
        }

        @Override
        public void error(String message) {
            log(LogLevel.ERROR, message);
        }

        @Override
        public void error(String message, Throwable t) {
            log(LogLevel.ERROR, message);
            if (t != null) {
                t.printStackTrace(System.err);
            }
        }

        @Override
        public void error(String message, Object... args) {
            log(LogLevel.ERROR, message, args);
        }

        @Override
        public void debug(String message) {
            log(LogLevel.DEBUG, message);
        }

        @Override
        public void debug(String message, Object... args) {
            log(LogLevel.DEBUG, message, args);
        }
    }
}
