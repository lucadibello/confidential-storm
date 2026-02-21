package ch.usi.inf.confidentialstorm.common.api.tunnel;

import org.apache.teaclave.javasdk.common.annotations.EnclaveService;

/**
 * Enclave service interface for the sender side of a cloaked tunnel.
 * Registered separately from the receiver for service discovery via {@code @AutoService}.
 */
@EnclaveService
public interface CloakedTunnelSenderService extends CloakedTunnelService {
}
