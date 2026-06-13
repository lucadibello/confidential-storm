package ch.usi.inf.confidentialstorm.enclave.security;

import java.util.BitSet;

/**
 * Tracks seen sequence numbers for a single producer using a fixed-size sliding window.
 * Uses a {@link BitSet} for efficient tracking of sequence numbers within the window.
 */
public final class ReplayWindow {
    /**
     * Window size in number of sequence numbers tracked
     */
    private final int windowSize;

    /**
     * Highest sequence number seen so far (-1 if none)
     */
    private long maxSeen = -1;

    /**
     * BitSet tracking seen sequence numbers within the window [maxSeen - windowSize + 1, maxSeen]
     */
    private BitSet window;

    /**
     * Constructs a new ReplayWindow with the given size.
     *
     * @param windowSize the number of sequence numbers to track
     */
    public ReplayWindow(int windowSize) {
        this.windowSize = windowSize;
        this.window = new BitSet(windowSize);
    }

    /**
     * Check if the given sequence number is acceptable (not a replay and within the window).
     * The bitset window always keeps the maxSeen as the LSB (bit 0), with older sequence numbers at increasing offsets.
     * <p>
     * When the sequence number is greater than maxSeen, the window is shifted accordingly.
     *
     * @param sequence the sequence number to check
     * @return true if accepted, false otherwise
     */
    public boolean accept(long sequence) {
        if (sequence < 0) {
            return false; // invalid sequence number
        }

        // reject everything outside the window [maxSeen - windowSize + 1, maxSeen + 1]
        if (maxSeen >= 0 && sequence <= maxSeen - windowSize) {
            return false; // too old
        }

        // Update window if the sequence number exceeds maxSeen.
        // Sequence numbers grow monotonically; maxSeen is anchored at bit 0.
        if (sequence > maxSeen) {
            long shift = sequence - maxSeen; // shift the window to fit the new maxSeen

            // Clear window if shift exceeds windowSize (all current history is out of range).
            if (shift >= windowSize) {
                window.clear();
            }
            // Shift the bitset right by the shift amount.
            else if (maxSeen >= 0) {
                // create empty bitset
                BitSet shifted = new BitSet(windowSize);
                int shiftBy = (int) shift;
                // copy bits from old window to new shifted window relative to the shift
                for (int i = 0; i < windowSize - shiftBy; i++) {
                    if (window.get(i)) {
                        shifted.set(i + shiftBy);
                    }
                }
                // update window reference
                window = shifted;
            }
            // Initialize for the first sequence number seen.
            else {
                window.clear();
            }

            // Update maxSeen; bit 0 represents the newest sequence.
            maxSeen = sequence;
            window.set(0); // mark as seen

            return true; // notify that the value has been accepted
        }

        // Sequence number is within the window range.

        // find the offset from maxSeen (how far back it is) + ensure offset is within window size
        int offset = (int) (maxSeen - sequence);
        if (offset >= windowSize) {
            return false; // too old (should not happen due to previous checks actually)
        }

        // Replay check.
        if (window.get(offset)) {
            return false; // replay detected
        }

        // within window and unseen: mark as seen
        window.set(offset);
        return true;
    }
}
