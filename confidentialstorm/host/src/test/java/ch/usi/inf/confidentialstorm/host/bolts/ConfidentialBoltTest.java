package ch.usi.inf.confidentialstorm.host.bolts;

import ch.usi.inf.confidentialstorm.common.crypto.exception.EnclaveServiceException;
import ch.usi.inf.confidentialstorm.host.base.ConfidentialComponentState;
import ch.usi.inf.confidentialstorm.host.util.EnclaveManager;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.teaclave.javasdk.host.EnclaveType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ConfidentialBolt} execute-time behavior with injected state.
 */
class ConfidentialBoltTest {

    /**
     * Verifies that execute delegates to {@code processTuple} with the service resolved from component state.
     */
    @Test
    void execute_successfulTuple_delegatesToProcessTuple() throws Exception {
        OutputCollector collector = mock(OutputCollector.class);
        TestService service = mock(TestService.class);
        Tuple tuple = mock(Tuple.class);

        RecordingConfidentialBolt bolt = new RecordingConfidentialBolt();
        bolt.injectState(buildState(collector, service));

        bolt.execute(tuple);

        assertSame(tuple, bolt.lastTuple());
        assertSame(service, bolt.lastService());
        verifyNoInteractions(collector);
    }

    /**
     * Verifies that execute reports errors, fails the tuple, and wraps the original enclave exception.
     */
    @Test
    void execute_processTupleThrows_reportsErrorAndFailsTuple() throws Exception {
        OutputCollector collector = mock(OutputCollector.class);
        TestService service = mock(TestService.class);
        Tuple tuple = mock(Tuple.class);

        when(tuple.getSourceComponent()).thenReturn("source");
        when(tuple.getSourceStreamId()).thenReturn("stream");

        RecordingConfidentialBolt bolt = new RecordingConfidentialBolt();
        bolt.setFailure(new EnclaveServiceException("java.lang.IllegalStateException", "boom"));
        bolt.injectState(buildState(collector, service));

        RuntimeException thrown = assertThrows(RuntimeException.class, () -> bolt.execute(tuple));

        assertTrue(thrown.getCause() instanceof EnclaveServiceException);
        verify(collector).reportError(any(EnclaveServiceException.class));
        verify(collector).fail(tuple);
    }

    /**
     * Verifies that execute still throws when collector error reporting itself fails.
     */
    @Test
    void execute_errorReportingFails_stillThrowsWrappedException() throws Exception {
        OutputCollector collector = mock(OutputCollector.class);
        TestService service = mock(TestService.class);
        Tuple tuple = mock(Tuple.class);

        doThrow(new RuntimeException("collector-down"))
                .when(collector)
                .reportError(any(Throwable.class));

        RecordingConfidentialBolt bolt = new RecordingConfidentialBolt();
        bolt.setFailure(new EnclaveServiceException("java.lang.IllegalArgumentException", "invalid"));
        bolt.injectState(buildState(collector, service));

        RuntimeException thrown = assertThrows(RuntimeException.class, () -> bolt.execute(tuple));

        assertTrue(thrown.getCause() instanceof EnclaveServiceException);
        verify(collector).reportError(any(Throwable.class));
        verify(collector, never()).fail(tuple);
    }

    /**
     * Builds a component state with a deterministic in-memory enclave manager for unit tests.
     */
    private ConfidentialComponentState<OutputCollector, TestService> buildState(OutputCollector collector,
                                                                                TestService service) throws Exception {
        ConfidentialComponentState<OutputCollector, TestService> state =
                new ConfidentialComponentState<>(TestService.class, EnclaveType.TEE_SDK);
        state.setCollector(collector);
        state.setComponentId("test-bolt");
        state.setTaskId(7);

        setField(state, "enclaveManager", new InMemoryEnclaveManager(service));
        return state;
    }

    /**
     * Sets a private field using reflection for controlled test injection.
     */
    private void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    /**
     * Minimal enclave service contract used by this test.
     */
    private interface TestService {
        /**
         * Executes a noop call for test service shape.
         */
        String noop();
    }

    /**
     * Test-only enclave manager that always returns a predefined service instance.
     */
    private static final class InMemoryEnclaveManager extends EnclaveManager<TestService> {
        private final TestService service;

        /**
         * Creates a test enclave manager with fixed service binding.
         */
        private InMemoryEnclaveManager(TestService service) {
            super(TestService.class, EnclaveType.TEE_SDK);
            this.service = service;
        }

        /**
         * Returns the injected service instance without touching SGX lifecycle code.
         */
        @Override
        public TestService getService() {
            return service;
        }
    }

    /**
     * Confidential bolt implementation used to assert delegation and error handling behavior.
     */
    private static final class RecordingConfidentialBolt extends ConfidentialBolt<TestService> {
        private Tuple lastTuple;
        private TestService lastService;
        private Throwable failure;

        /**
         * Creates a recording bolt for the test service type.
         */
        private RecordingConfidentialBolt() {
            super(TestService.class);
        }

        /**
         * Injects prebuilt component state to bypass enclave initialization in unit tests.
         */
        private void injectState(ConfidentialComponentState<OutputCollector, TestService> injectedState) {
            this.state = injectedState;
        }

        /**
         * Configures a throwable to be raised during tuple processing.
         */
        private void setFailure(Throwable throwable) {
            this.failure = throwable;
        }

        /**
         * Returns the last tuple observed by {@code processTuple}.
         */
        private Tuple lastTuple() {
            return lastTuple;
        }

        /**
         * Returns the last service instance observed by {@code processTuple}.
         */
        private TestService lastService() {
            return lastService;
        }

        /**
         * Records the tuple and service, then optionally fails according to the configured throwable.
         */
        @Override
        protected void processTuple(Tuple input, TestService service) throws EnclaveServiceException {
            this.lastTuple = input;
            this.lastService = service;
            if (failure instanceof EnclaveServiceException enclaveServiceException) {
                throw enclaveServiceException;
            }
            if (failure instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (failure != null) {
                throw new RuntimeException(failure);
            }
        }

        /**
         * Declares no output fields because this unit-test bolt does not emit tuples.
         */
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            // no outputs for this test bolt
        }
    }
}
