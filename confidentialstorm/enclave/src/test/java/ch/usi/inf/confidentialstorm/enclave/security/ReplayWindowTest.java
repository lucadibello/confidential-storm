package ch.usi.inf.confidentialstorm.enclave.security;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ReplayWindow} acceptance and replay semantics.
 */
class ReplayWindowTest {

    /**
     * Verifies that the first non-negative sequence number is accepted.
     */
    @Test
    void accept_firstSequence_isAccepted() {
        ReplayWindow replayWindow = new ReplayWindow(8);

        assertTrue(replayWindow.accept(0));
    }

    /**
     * Verifies that negative sequence numbers are always rejected.
     */
    @Test
    void accept_negativeSequence_isRejected() {
        ReplayWindow replayWindow = new ReplayWindow(8);

        assertFalse(replayWindow.accept(-1));
    }

    /**
     * Verifies that an already accepted sequence number is rejected as a replay.
     */
    @Test
    void accept_duplicateSequence_isRejected() {
        ReplayWindow replayWindow = new ReplayWindow(8);

        assertTrue(replayWindow.accept(7));
        assertFalse(replayWindow.accept(7));
    }

    /**
     * Verifies that out-of-order sequence numbers are accepted if they are within the active window.
     */
    @Test
    void accept_outOfOrderSequenceWithinWindow_isAccepted() {
        ReplayWindow replayWindow = new ReplayWindow(8);

        assertTrue(replayWindow.accept(10));
        assertTrue(replayWindow.accept(8));
        assertTrue(replayWindow.accept(9));
        assertFalse(replayWindow.accept(8));
    }

    /**
     * Verifies that sequence numbers older than the sliding window lower bound are rejected.
     */
    @Test
    void accept_sequenceOutsideWindow_isRejected() {
        ReplayWindow replayWindow = new ReplayWindow(4);

        assertTrue(replayWindow.accept(0));
        assertTrue(replayWindow.accept(1));
        assertTrue(replayWindow.accept(2));
        assertTrue(replayWindow.accept(3));
        assertTrue(replayWindow.accept(7));

        assertFalse(replayWindow.accept(3));
    }

    /**
     * Verifies that the oldest sequence still inside the window is accepted once and then rejected on replay.
     */
    @Test
    void accept_lowerWindowBoundary_isAcceptedOnlyOnce() {
        ReplayWindow replayWindow = new ReplayWindow(4);

        assertTrue(replayWindow.accept(5));
        assertTrue(replayWindow.accept(2));
        assertFalse(replayWindow.accept(2));
    }

    /**
     * Verifies that a large forward jump clears previous history and old values remain rejected.
     */
    @Test
    void accept_largeForwardJump_clearsHistoryAndRejectsTooOldValues() {
        ReplayWindow replayWindow = new ReplayWindow(8);

        assertTrue(replayWindow.accept(5));
        assertTrue(replayWindow.accept(6));
        assertTrue(replayWindow.accept(30));

        assertFalse(replayWindow.accept(5));
    }
}
