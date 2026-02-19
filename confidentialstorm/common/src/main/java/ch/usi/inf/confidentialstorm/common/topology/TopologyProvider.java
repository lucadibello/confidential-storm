package ch.usi.inf.confidentialstorm.common.topology;

import java.util.List;

/**
 * Interface for providing topology-related information, such as downstream components.
 */
public interface TopologyProvider {
    /**
     * Gets the list of downstream components for a given component.
     *
     * @param component the source component
     * @return the list of downstream components
     */
    List<TopologySpecification.Component> getDownstream(TopologySpecification.Component component);
}
