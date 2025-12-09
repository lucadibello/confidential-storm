package ch.usi.inf.confidentialstorm.enclave.dp;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.util.FastMath;

/**
 * A binary aggregation tree for differential privacy.
 * This class implements a binary tree structure used for computing differentially private
 * prefix sums. Each node in the tree is initialized with Gaussian noise, and values are
 * added along paths from leaves to the root to maintain differential privacy guarantees.
 *
 * <p>
 * Refer to Algorithm 4 in "Differentially Private Stream Processing at Scale"
 */
public final class BinaryAggregationTree {
    /**
     * Binary tree stored as a list, where each node contains a double value.
     */
    private final List<Double> tree;

    // tree parameters
    /**
     * The height of the binary tree computed as H = ceil(log2(T)).
     */
    private final int height;

    /**
     * The number of leaves in the binary tree, computed as L = 2^H.
     */
    private final int num_leaves;

    /**
     * Standard deviation of the Gaussian noise added to each node.
     */
    private final double sigma;

    /**
     * Constructs a binary aggregation tree for differential privacy.
     *
     * @param n     the number of data points in the data stream D = {x_1, ..., x_n} each x_i \in [0,L]
     * @param sigma the standard deviation of the Gaussian noise to be added to each node
     */
    public BinaryAggregationTree(int n, double sigma) {
        // precompute tree parameters
        height = (int) FastMath.ceil(FastMath.log(n) / FastMath.log(2)); // H = ceil(log2(n))
        num_leaves = (int) FastMath.pow(2, height); // L = 2^ceil(log2(n))

        // store sigma for Honaker variance computation
        this.sigma = sigma;

        // initialize tree with Gaussian noise
        this.tree = initializeTree(sigma);
    }

    /**
     * Computes the variance of the Honaker estimate for a specific leaf index (time step).
     * The variance depends on the number of levels involved in the prefix sum.
     * <p>
     * From the paper: Variance(node) = sigma^2 * (1 / (2 * (1 - 2^-kappa)))
     * where kappa is the height of the subtree rooted at node_i.
     * <p>
     * The total variance is the sum of variances of all nodes in the path for the prefix sum.
     * <p>
     * Refer to Appendix C subsection "Honaker estimation for variance reduction"
     *
     * @param i the zero-based index of the leaf
     * @return the variance of the prefix sum at step i
     */
    public double getHonakerVariance(int i) {
        // NOTE: same traversal logic of query(i) to identify which nodes are summed
        int indexBinary = i + 1;
        int nodeIndex = 0;
        double totalVariance = 0.0;

        // FIXME: remove useless comments as soon as this works as expected

        // Tree height H (root is level 0, leaves are level H)
        // NOTE: "level_0...level_kappa are levels of subtree rooted at node_i".

        // A node at depth 'j' (j=0 => root) covers 2^(height - j) leaves
        // kappa = height - j + 1. (Leaves have height 1).

        for (int j = 0; j <= height; j++) {
            int levelBit = (indexBinary >> (height - j)) & 1;

            if (levelBit == 1) {
                int kappa = height - j + 1;
                
                // Variance contribution for this node (using Honaker formula)
                // Formula: Variance(node_i) = sigma^2 / (2 * (1 - 2^-kappa))
                // NOTE: refer to Appendix C of the paper, equation 1
                double nodeVariance = (sigma * sigma) / (2.0 * (1.0 - FastMath.pow(2.0, -kappa)));
                totalVariance += nodeVariance;
            }

            if (j < height) {
                int pathBit = (i >> (height - 1 - j)) & 1;
                int leftChild = 2 * nodeIndex + 1;
                int rightChild = leftChild + 1;
                nodeIndex = (pathBit == 0) ? leftChild : rightChild;
            }
        }
        return totalVariance;
    }

    /**
     * Adds a value c to all the nodes on the path from leaf i to the root of the tree and computes the DP prefix sum S_i^priv.
     *
     * @param i the index of the leaf
     * @param c the value to add
     */
    public double addToTree(int i, double c) {
        // 1. Update tree on path leaf_i -> root
        add(i, c);
        // 2. Compute DP prefix sum
        return query(i);
    }

    /**
     * Helper method to add value c to all nodes on the path from leaf i to the root.
     *
     * @param i the zero-based index of the leaf
     * @param c the value to add to each node on the path
     */
    private void add(int i, double c) {
        // Compute the index of the leaf in the tree array
        int index = num_leaves - 1 + i;
        // Traverse from the leaf to the root, adding c to each node
        while (index > 0) {
            tree.set(index, tree.get(index) + c);
            index = (index - 1) / 2; // move up to the parent node
        }
        // Finally, update the root node
        tree.set(0, tree.get(0) + c);
    }

    /**
     * Helper method to compute the DP prefix sum S_i^priv for leaf i.
     * The computation uses the "Bottom-up Honaker variance reduction" technique described in Appendix C
     * of the paper. Instead of simply summing the nodes in the canonical decomposition, we compute
     * a weighted estimate for each canonical node using its subtree.
     *
     * @param i the zero-based index of the leaf
     * @return the differentially private prefix sum S_i^priv
     */
    public double query(int i) {
        // now compute DP prefix sum S_i^priv
        int indexBinary = i + 1; // convert from 0-based to 1-based index

        int nodeIndex = 0; // current node index, starting from the root
        double sPriv = 0f;

        // From 0...h (inclusive) -> h+1 levels in total
        for (int j = 0; j <= height; j++) {

            // get the bit at position 'j' from indexBinary (from most significant to least significant)
            int levelBit = (indexBinary >> (height - j)) & 1;

            // if the bit is 1, add the left sibling's contribution to sPriv
            if (levelBit == 1) {
                int leftSibling;
                if (nodeIndex == 0) {
                    leftSibling = 0; // root has no siblings
                } else if (nodeIndex % 2 == 0) {
                    leftSibling = nodeIndex - 1; // node_i is a right child
                } else {
                    leftSibling = nodeIndex; // node_i is a left child, so its left sibling is itself
                }
                
                // Calculate kappa (subtree height) for the sibling (NOTE: currently at level j)
                // - If j = height (leaf), kappa = 1 (only the leaf node)
                // - If j = 0 (root), kappa = height + 1 (entire tree)
                int kappa = height - j + 1;

                // Add the Honaker variance-reduced estimate for this node
                // (refer to appendix C subsection 1, "Honaker estimation for variance reduction")

                // Formula: sum_{j=0 to kappa-1} (c_j * Sum(level_j))
                sPriv += computeHonakerEstimate(leftSibling, kappa);
            }

            // for all levels except the last one (leaf level), move to the next node in the path
            if (j < height) {
                int pathBit = (i >> (height - 1 - j)) & 1;

                int leftChild = 2 * nodeIndex + 1;
                int rightChild = leftChild + 1;

                // move to the next node in the path
                nodeIndex = (pathBit == 0) ? leftChild : rightChild;
            }
        }

        // return total DP sum S_i^priv
        return sPriv;
    }

    /**
     * Computes the Honaker estimate for a given node.
     * The estimate is a weighted sum of the sums of levels in the subtree rooted at nodeIndex.
     * <p>
     * FORMULA:
     *  Sum_{j=0 to k-1} (c_j * Sum(level_j)) where:
     *  c_j = (1/2^j) / (Sum_{j=0 to k-1} (1/2^j))
     * <p>
     * Refer to Appendix C subsection "Honaker estimation for variance reduction" for details.
     *
     * @param nodeIndex The root of the subtree.
     * @param k     The height of the subtree (number of levels from nodeIndex to leaves).
     * @return The weighted estimate.
     */
    private double computeHonakerEstimate(int nodeIndex, int k) {
        double honaker_estimate = 0.0;

        // We need to traverse levels j in [0, ..., k-1] of the subtree and compute c_j * Sum(level_j)
        // - Level 0 contains just nodeIndex (subtree root)
        // - Level k contains 2^k nodes (a subtree of a complete binary tree is still complete)

        // keep track of nodes at the current level
        List<Integer> currentLevelNodes = new ArrayList<>();
        currentLevelNodes.add(nodeIndex);

        for (int j = 0; j < k; j++) {
            double sum_level_j = 0; // Sum(level_j) = sum of values at level j

            // keep track of nodes for the next level
            List<Integer> nextLevelNodes = new ArrayList<>();

            // for each node in the current level, add its value and prepare its children for the next level
            for (int idx : currentLevelNodes) {
                // check if idx is within bounds of the tree
                if (idx < tree.size()) {
                    sum_level_j += tree.get(idx);

                    // Prepare children for next level
                    if (j < k - 1) {
                        nextLevelNodes.add(2 * idx + 1);
                        nextLevelNodes.add(2 * idx + 2);
                    }
                }
            }

            // Calculate weight c_j
            // FORMULA:
            //  c_j = (1/2^j) / (Sum_{j=0 to k-1} (1/2^j)) -> Sum_{j=0 to k-1} (1/2^j) is a finite geometric series
            //  c_j = (1/2^j) / [(1 - (1/2)^k) / (1 - 1/2)]  -> simplification
            //  c_j = (1/2^j) / [ (1 - 2^-k) / (1/2) ] -> simplification
            //  c_j = (1/2^j) * (2 * (1 - 2^-k)) -> simplification
            //  c_j = 2^-j / (2 * (1 - 2^-k))
            double c_j = FastMath.pow(2.0, -j) / (2.0 * (1.0 - FastMath.pow(2.0, -k)));

            // accumulate weighted sum
            // FORMULA: estimate = Sum_{j=0 to k-1} (c_j * Sum(level_j)) =
            honaker_estimate += c_j * sum_level_j;

            // move to the next level and bring its children
            currentLevelNodes = nextLevelNodes;
        }

        // return the final Honaker estimate
        return honaker_estimate;
    }

    /**
     * Initializes a complete binary tree with (2 * NUM_LEAVES - 1) nodes, where each node is sampled
     * from a Gaussian distribution N(0, sigma^2).
     *
     * @param sigma the standard deviation of the Gaussian noise distribution
     * @return the initialized tree as a list of doubles with size (2 * NUM_LEAVES - 1)
     */
    private List<Double> initializeTree(double sigma) {
        // create a list to hold the tree nodes
        List<Double> tree = new ArrayList<>(2 * num_leaves - 1);

        // fill the tree with Gaussian noise
        SecureRandom rnd = new SecureRandom();
        for (int i = 0; i < 2 * num_leaves - 1; i++) {
            // NOTE: nextGaussian() samples from N(0,1)
            // If X ~ N(mu, 1) => Y = aX ~ N(a*mu, a^2)
            // Here: mu=0 and a=sigma => Y ~ N(0, sigma^2)

            // sample noise from N(0, sigma^2)
            double noise = rnd.nextGaussian() * sigma;
            tree.add(noise);
        }

        // return the initialized tree
        return tree;
    }

}
