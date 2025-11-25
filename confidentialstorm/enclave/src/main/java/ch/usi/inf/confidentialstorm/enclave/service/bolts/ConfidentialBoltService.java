package ch.usi.inf.confidentialstorm.enclave.service.bolts;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.enclave.crypto.aad.DecodedAAD;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.crypto.SealedPayload;
import ch.usi.inf.confidentialstorm.enclave.util.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.EnclaveLoggerFactory;

import java.util.BitSet;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public abstract class ConfidentialBoltService<T extends Record> {
    /**
     * Zero-depencency logger for the enclave services.
     */
    private final EnclaveLogger LOG = EnclaveLoggerFactory.getLogger(ConfidentialBoltService.class);

    /**
     * Size of the replay window for sequence number tracking (should be large enough to accommodate out-of-order messages).
     */
    private final int REPLAY_WINDOW_SIZE = 128;

    /**
     * Map of producer IDs to their corresponding replay windows for replay attack prevention.
     * NOTE: we use a map of replay windows as one bolt could ingest data streams from multiple different producers.
     */
    private final Map<String, ReplayWindow> replayWindows = new ConcurrentHashMap<>();
    protected final SealedPayload sealedPayload;

    protected ConfidentialBoltService() {
        this(SealedPayload.fromConfig());
    }

    protected ConfidentialBoltService(SealedPayload sealedPayload) {
        this.sealedPayload = Objects.requireNonNull(sealedPayload, "sealedPayload cannot be null");
    }

    /**
     * Get the expected source component for the sealed values in the request.
     * @return the expected source component
     */
    public abstract TopologySpecification.Component expectedSourceComponent();

    /**
     * Get the expected destination component for the sealed values in the request.
     * @return the expected destination component
     */
    public abstract TopologySpecification.Component expectedDestinationComponent();

    /**
     * Extract all sealed/encrypted values from the request that need to be verified.
     * @param request the request containing the sealed values
     * @return a collection of sealed/encrypted values to verify
     */
    public abstract Collection<EncryptedValue> valuesToVerify(T request);

    /**
     * Verify that all sealed values in the request are valid, come from the expected source to the expected destination,
     * and that their sequence numbers are within the replay window.
     * @param request the request containing the sealed values to verify
     * @throws SecurityException if any verification step fails
     */
    protected void verify(T request) throws SecurityException {
        // extract all critical values from the request
        Collection<EncryptedValue> values = valuesToVerify(request);

        TopologySpecification.Component destination = Objects.requireNonNull(expectedDestinationComponent(),
                "Expected destination component cannot be null");
        TopologySpecification.Component expectedSource = expectedSourceComponent();

        String producerId = null;
        Long sequence = null;

        // verify each value
        for (EncryptedValue sealedValue : values) {
            try {
                // NOTE: if the source is null, it means that the value was created outside of ConfidentialStorm
                // hence, verifyRoute would verify only the destination component
                LOG.info("Verifying sealed value: {} from {} to {}", sealedValue, expectedSource, destination);
                sealedPayload.verifyRoute(sealedValue, expectedSource, destination);

                // extract AAD and check producer/sequence consistency
                DecodedAAD aad = DecodedAAD.fromBytes(sealedValue.associatedData());
                String currentProducer = aad.producerId().orElseThrow(() ->
                        new SecurityException("AAD missing producer_id"));
                Long currentSeq = aad.sequenceNumber().orElseThrow(() ->
                        new SecurityException("AAD missing sequence number"));

                // NOTE: we assume all encrypted fields in the same request share the same producer and sequence number

                // get first available producer/sequence tuple and ensure all values have the same
                if (producerId == null && sequence == null) {
                    producerId = currentProducer;
                    sequence = currentSeq;
                } else if (!Objects.equals(producerId, currentProducer) || !Objects.equals(sequence, currentSeq)) {
                    throw new SecurityException("Mismatch between AAD producer/sequence across encrypted fields");
                }
            } catch (Exception e) {
                LOG.error("Sealed value verification failed for source {} destination {} value {}: {}",
                        expectedSource, destination, sealedValue, e.getMessage());
                LOG.error("Sealed value verification exception", e);
                throw new SecurityException("Sealed value verification failed", e);
            }
        }

        // ensure that we got valid producer/sequence info
        if (producerId == null || sequence == null) {
            throw new SecurityException("Missing producer/sequence information");
        }

        // now, check sequence number for replay attacks -> if the sequence number is outside of the replay window or
        // has already been seen, we reject the request

        // we create or get the existing replay window for this producer (1 replay window per producer)
        ReplayWindow window = replayWindows.computeIfAbsent(producerId, id -> new ReplayWindow(REPLAY_WINDOW_SIZE));
        if (!window.accept(sequence)) {
            throw new SecurityException("Replay or out-of-window sequence " + sequence + " for producer " + producerId);
        }
    }

    /**
     * This replay window tracks seen sequence numbers for a single producer using a fixed-size sliding window (bitset for efficiency).
     */
    private static final class ReplayWindow {
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

        private ReplayWindow(int windowSize) {
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
        synchronized boolean accept(long sequence) {
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
}
