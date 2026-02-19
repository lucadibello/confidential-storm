package ch.usi.inf.confidentialstorm.common.topology;

import java.util.List;

/**
 * Interface for providing topology routing information: upstream and downstream neighbours
 * for each component in the Storm topology graph.
 */
public interface TopologyProvider {

    /**
     * Returns all components that send tuples directly to {@code component}
     * (i.e., the immediate predecessors in the topology DAG).
     *
     * @param component the component whose upstream neighbours are requested
     * @return immutable list of upstream components; empty if none are registered
     */
    List<TopologySpecification.Component> getUpstream(TopologySpecification.Component component);

    /**
     * Returns all components that receive tuples directly from {@code component}
     * (i.e., the immediate successors in the topology DAG).
     *
     * @param component the component whose downstream neighbours are requested
     * @return immutable list of downstream components; empty if none are registered
     */
    List<TopologySpecification.Component> getDownstream(TopologySpecification.Component component);
}
