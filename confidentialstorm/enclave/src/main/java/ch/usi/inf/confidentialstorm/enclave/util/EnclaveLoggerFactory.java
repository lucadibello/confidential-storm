package ch.usi.inf.confidentialstorm.enclave.util;

import ch.usi.inf.confidentialstorm.enclave.EnclaveConfig;
import ch.usi.inf.confidentialstorm.enclave.EnclaveConfig.LogLevel;

public final class EnclaveLoggerFactory {
    private EnclaveLoggerFactory() { }

    public static EnclaveLogger getLogger(Class<?> clazz) {
        return new EnclaveLoggerImpl(clazz);
    }

    private record EnclaveLoggerImpl(Class<?> clazz) implements EnclaveLogger {
            private enum Level {
                DEBUG(0),
                INFO(1),
                WARN(2),
                ERROR(3);

                private final int priority;

                Level(int priority) {
                    this.priority = priority;
                }
            }

            private static final Level CONFIGURED_LEVEL = resolveLogLevel();

        private static Level resolveLogLevel() {
                LogLevel configured = EnclaveConfig.LOG_LEVEL;
                if (configured == null) {
                    return Level.INFO;
                }
                return switch (configured) {
                    case DEBUG -> Level.DEBUG;
                    case INFO -> Level.INFO;
                    case WARN -> Level.WARN;
                    case ERROR -> Level.ERROR;
                };
            }

            private boolean isEnabled(Level level) {
                return level.priority >= CONFIGURED_LEVEL.priority;
            }

            private void log(Level level, String message) {
                if (!isEnabled(level)) {
                    return;
                }
                String log = String.format("[%s] %s: %s", level.name(), clazz.getSimpleName(), message);
                if (level == Level.ERROR) {
                    System.err.println(log);
                } else {
                    System.out.println(log);
                }
            }

            private void log(Level level, String format, Object... args) {
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
