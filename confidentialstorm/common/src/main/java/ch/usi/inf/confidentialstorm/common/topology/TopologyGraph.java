package ch.usi.inf.confidentialstorm.common.topology;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Data-transfer object that represents the directed topology graph as an adjacency list.
 * <p>
 * Each key in the adjacency list is a source component name; the corresponding value is the
 * ordered list of downstream component names that receive tuples from that source.
 * <p>
 * This class is serialised to JSON by {@code TopologyGraphGenerator} at build time and
 * deserialised by {@code EncryptedTopologyProvider} at enclave startup.
 */
public class TopologyGraph implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Directed adjacency list: source component name → list of downstream component names.
     */
    private Map<String, List<String>> adjacencyList;

    /**
     * No-argument constructor required for JSON deserialisation.
     */
    public TopologyGraph() {
    }

    /**
     * Constructs a topology graph with the given adjacency list.
     *
     * @param adjacencyList directed adjacency list mapping each source component name
     *                      to its list of downstream component names
     */
    public TopologyGraph(Map<String, List<String>> adjacencyList) {
        this.adjacencyList = adjacencyList;
    }

    /**
     * Returns the directed adjacency list.
     *
     * @return the adjacency list, or {@code null} if not yet set
     */
    public Map<String, List<String>> getAdjacencyList() {
        return adjacencyList;
    }

    /**
     * Sets the directed adjacency list.
     *
     * @param adjacencyList directed adjacency list mapping each source component name
     *                      to its list of downstream component names
     */
    public void setAdjacencyList(Map<String, List<String>> adjacencyList) {
        this.adjacencyList = adjacencyList;
    }
}
