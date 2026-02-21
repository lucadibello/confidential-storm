package ch.usi.inf.confidentialstorm.enclave.service.bolts.base.tunnel;

import ch.usi.inf.confidentialstorm.common.crypto.model.EncryptedValue;
import ch.usi.inf.confidentialstorm.common.tunnel.TunnelConstants;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Enclave-side codec for serializing/deserializing {@link EncryptedValue} field lists
 * into fixed-size slot byte arrays used by the cloaked tunnel batch format.
 * <p>
 * Slot layout:
 * <pre>
 * [1B marker: 0x01=real, 0x00=dummy]
 * [4B payload length (big-endian)]
 * [payload bytes: length-prefix framed List&lt;EncryptedValue&gt;]
 * [zero-padding to slotSize]
 * </pre>
 * <p>
 * Each {@link EncryptedValue} is framed as:
 * <pre>
 * [4B aad length][aad bytes][4B nonce length][nonce bytes][4B ciphertext length][ciphertext bytes]
 * </pre>
 */
public final class CloakedTunnelSlotCodec {

    private CloakedTunnelSlotCodec() {
    }

    /**
     * Serializes a list of encrypted tuple fields into a fixed-size real slot.
     *
     * @param fields   the encrypted fields of the tuple
     * @param slotSize the fixed slot size in bytes
     * @return a byte array of exactly {@code slotSize} bytes
     * @throws IllegalArgumentException if the serialized payload exceeds the slot capacity
     */
    public static byte[] encodeRealSlot(List<EncryptedValue> fields, int slotSize) {
        byte[] payload = serializeFields(fields);
        if (1 + 4 + payload.length > slotSize) {
            throw new IllegalArgumentException(
                    "Serialized tuple fields (" + (1 + 4 + payload.length) +
                            " bytes) exceed slot size (" + slotSize + " bytes)");
        }
        ByteBuffer buf = ByteBuffer.allocate(slotSize);
        buf.put(TunnelConstants.MARKER_REAL);
        buf.putInt(payload.length);
        buf.put(payload);
        // remaining bytes are zero-padded by default (ByteBuffer.allocate zeroes memory)
        return buf.array();
    }

    /**
     * Creates a dummy (padding) slot of the given size.
     *
     * @param slotSize the fixed slot size in bytes
     * @return a byte array of exactly {@code slotSize} bytes with marker=0x00
     */
    public static byte[] encodeDummySlot(int slotSize) {
        // ByteBuffer.allocate zeroes all bytes; marker byte is already 0x00
        return new byte[slotSize];
    }

    /**
     * Decodes a slot and returns the tuple fields if it is a real slot,
     * or {@code null} if it is a dummy slot.
     *
     * @param slot     the slot byte array
     * @param slotSize the expected slot size
     * @return the list of encrypted fields, or null for dummy slots
     */
    public static List<EncryptedValue> decodeSlot(byte[] slot, int slotSize) {
        if (slot.length != slotSize) {
            throw new IllegalArgumentException(
                    "Slot length (" + slot.length + ") does not match expected size (" + slotSize + ")");
        }
        ByteBuffer buf = ByteBuffer.wrap(slot);
        byte marker = buf.get();
        if (marker == TunnelConstants.MARKER_DUMMY) {
            return null;
        }
        if (marker != TunnelConstants.MARKER_REAL) {
            throw new IllegalArgumentException("Unknown slot marker: " + marker);
        }
        int payloadLength = buf.getInt();
        byte[] payload = new byte[payloadLength];
        buf.get(payload);
        return deserializeFields(payload);
    }

    /**
     * Serializes a list of EncryptedValues using length-prefix framing.
     */
    private static byte[] serializeFields(List<EncryptedValue> fields) {
        // Calculate total size
        int totalSize = 4; // field count
        for (EncryptedValue ev : fields) {
            totalSize += 4 + ev.associatedData().length
                    + 4 + ev.nonce().length
                    + 4 + ev.ciphertext().length;
        }
        ByteBuffer buf = ByteBuffer.allocate(totalSize);
        buf.putInt(fields.size());
        for (EncryptedValue ev : fields) {
            byte[] aad = ev.associatedData();
            byte[] nonce = ev.nonce();
            byte[] ct = ev.ciphertext();
            buf.putInt(aad.length);
            buf.put(aad);
            buf.putInt(nonce.length);
            buf.put(nonce);
            buf.putInt(ct.length);
            buf.put(ct);
        }
        return buf.array();
    }

    /**
     * Deserializes a length-prefix framed payload into a list of EncryptedValues.
     */
    private static List<EncryptedValue> deserializeFields(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        int fieldCount = buf.getInt();
        List<EncryptedValue> fields = new ArrayList<>(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            int aadLen = buf.getInt();
            byte[] aad = new byte[aadLen];
            buf.get(aad);
            int nonceLen = buf.getInt();
            byte[] nonce = new byte[nonceLen];
            buf.get(nonce);
            int ctLen = buf.getInt();
            byte[] ct = new byte[ctLen];
            buf.get(ct);
            fields.add(new EncryptedValue(aad, nonce, ct));
        }
        return fields;
    }
}
