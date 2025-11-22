package ch.usi.inf.confidentialstorm.enclave;

/**
 * Configuration parameters for the secure enclave.
 */
public final class EnclaveConfig {
    private EnclaveConfig() {
        // Prevent instantiation
    }

    /**
     * Hard-coded stream key in hexadecimal format to decrypt data within the enclave.
     *
     * NOTE: this is just for demonstration purposes. For production we may need to setup
     * a secure key provisioning mechanism (i.e., using Intel SGX Remote Attestation).
     */
    public static final String STREAM_KEY_HEX = "a46bf317953bf1a8f71439f74f30cd889ec0aa318f8b6431789fb10d1053d932";
}
