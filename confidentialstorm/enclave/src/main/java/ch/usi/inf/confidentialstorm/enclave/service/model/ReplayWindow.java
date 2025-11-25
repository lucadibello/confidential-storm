package ch.usi.inf.confidentialstorm.enclave.service.model;

import java.util.BitSet;

/**
 * This replay window tracks seen sequence numbers for a single producer using a fixed-size sliding window (bitset for efficiency).
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

    public ReplayWindow(int windowSize) {
        this.windowSize = windowSize;
        this.window = new BitSet(windowSize);
    }

    /**
     * Check if the given sequence number is acceptable (not a replay and within the window). The bitset window always
     * keeps the maxSeen as the LSB (bit 0), with older sequence numbers at increasing offsets.
     * <p>
     * When the sequence number is greater than maxSeen, the window is shifted accordingly in order to accommodate the new maxSeen.
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

        // if sequence number is valid and greater than maxSeen, we need to update the window
        // NOTE: as the sequence numbers only grow, we want to have maxSeen always as the LSB of the window
        // [maxSeen - windowSize + 1, maxSeen]
        if (sequence > maxSeen) {
            long shift = sequence - maxSeen; // shift the window to fit the new maxSeen

            // if the shift is larger than the window size, we clear the window as everything is out of range
            // Example: windowSize = 128, maxSeen = 200, new sequence = 400 -> shift = 200 -> clear window
            if (shift >= windowSize) {
                window.clear();
            }
            // otherwise, we shift the bitset to the right by 'shift' positions
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
            // else, it's the first sequence number seen, so we just clear the window (we haven't seen anything yet)
            else {
                window.clear();
            }

            // the new sequence number is now the maxSeen
            // NOTE: bit 0 always tracks maxSeen (the newest sequence), older ones sit at increasing offsets
            maxSeen = sequence;
            window.set(0); // mark as seen

            return true; // notify that the value has been accepted
        }

        // otherwise, the sequence number is within the window range!

        // find the offset from maxSeen (how far back it is) + ensure offset is within window size
        int offset = (int) (maxSeen - sequence);
        if (offset >= windowSize) {
            return false; // too old (should not happen due to previous checks actually)
        }

        // if we have already seen this offset, it's a replay!
        if (window.get(offset)) {
            return false; // replay detected
        }

        // within window and unseen: mark as seen
        window.set(offset);
        return true;
    }
}
