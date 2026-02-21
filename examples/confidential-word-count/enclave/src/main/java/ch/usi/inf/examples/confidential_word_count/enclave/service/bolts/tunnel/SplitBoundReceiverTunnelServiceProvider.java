package ch.usi.inf.examples.confidential_word_count.enclave.service.bolts.tunnel;

import ch.usi.inf.confidentialstorm.common.api.tunnel.CloakedTunnelReceiverService;
import ch.usi.inf.confidentialstorm.common.topology.TopologySpecification;
import ch.usi.inf.confidentialstorm.enclave.service.bolts.base.tunnel.AbstractCloakedTunnelServiceProvider;
import ch.usi.inf.examples.confidential_word_count.common.topology.ComponentConstants;
import com.google.auto.service.AutoService;

/**
 * Cloaked tunnel receiver service provider for the split→bounding tunnel.
 * Runs inside the receiver bolt's enclave instance.
 */
@AutoService(CloakedTunnelReceiverService.class)
public final class SplitBoundReceiverTunnelServiceProvider
        extends AbstractCloakedTunnelServiceProvider
        implements CloakedTunnelReceiverService {

    private static final int BATCH_SIZE = 32;
    private static final int SLOT_SIZE = 1500;

    @Override
    public TopologySpecification.Component currentComponent() {
        return ComponentConstants.TUNNEL_SPLIT_BOUND_RECEIVER;
    }

    @Override
    protected int getBatchSize() {
        return BATCH_SIZE;
    }

    @Override
    protected int getSlotSizeBytes() {
        return SLOT_SIZE;
    }
}
