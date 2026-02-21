package ch.usi.inf.confidentialstorm.common.api.tunnel;

import org.apache.teaclave.javasdk.common.annotations.EnclaveService;

/**
 * Enclave service interface for the receiver side of a cloaked tunnel.
 * Registered separately from the sender for service discovery via {@code @AutoService}.
 */
@EnclaveService
public interface CloakedTunnelReceiverService extends CloakedTunnelService {
}
