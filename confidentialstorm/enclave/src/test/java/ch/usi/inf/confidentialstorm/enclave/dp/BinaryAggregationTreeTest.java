package ch.usi.inf.confidentialstorm.enclave.dp;

import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLogger;
import ch.usi.inf.confidentialstorm.enclave.util.logger.EnclaveLoggerFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

class BinaryAggregationTreeTest {

    private static final EnclaveLogger LOG = EnclaveLoggerFactory.getLogger(BinaryAggregationTreeTest.class);
    private final static double TOL = 1e-6;

    /**
     * Test to verify equivalence between BinaryAggregationTree and
     * the reference implementation (BinaryAggregationTreeOriginal) across different tree sizes.
     * <p>
     * We use sigma = 0 to ensure deterministic behavior for logic verification,
     * as comparing noisy trees would require sharing Random seeds between implementations.
     *
     * @param treeSize The size of the tree (number of leaves).
     */
    @ParameterizedTest
    @ValueSource(ints = {16, 128, 1024, 2048})
    void equivalenceTest(int treeSize) {
        LOG.info("Starting equivalence test for BinaryAggregationTree with size {}...", treeSize);

        double sigma = 0.0; // Must be 0 to compare logic deterministically
        BinaryAggregationTree myTree = new BinaryAggregationTree(treeSize, sigma);
        BinaryAggregationTreeOriginal theirTree = new BinaryAggregationTreeOriginal(treeSize, sigma);

        Random rand = new Random(42); // Fixed seed for reproducible tests

        // Test filling the tree up to its full capacity
        for (int i = 0; i < treeSize; i++) {
            double value = rand.nextDouble() * 1000 - 500; // Random value between -500 and 500

            double theirOutput = theirTree.addToTree(i, value);
            double mineOutput = myTree.addToTree(i, value);

            Assertions.assertEquals(theirOutput, mineOutput, TOL,
                    String.format("Mismatch at index %d for tree size %d", i, treeSize));
        }
        LOG.info("Equivalence test passed for tree size {}!", treeSize);
    }


    /**
     * Test specifically checking boundary conditions with a minimal tree size.
     */
    @Test
    void boundaryTest() {
        int treeSize = 2;
        double sigma = 0.0;
        BinaryAggregationTree myTree = new BinaryAggregationTree(treeSize, sigma);
        BinaryAggregationTreeOriginal theirTree = new BinaryAggregationTreeOriginal(treeSize, sigma);

        double[] inputs = {1.0, 2.0};
        for (int i = 0; i < treeSize; i++) {
            Assertions.assertEquals(theirTree.addToTree(i, inputs[i]), myTree.addToTree(i, inputs[i]), TOL);
        }
    }

    private static class BinaryAggregationTreeOriginal {
        private Double curPrivateSum = 0.00;
        private final ArrayList<Double> tree;
        private static int height = 0;

        public BinaryAggregationTreeOriginal(int n, double sigma) {
            tree = initializeTree(n, sigma);
        }

        public Double getTotalSum() {
            return curPrivateSum;
        }

        private ArrayList<Double> initializeTree(int n, double sigma) {
            height = (int) Math.ceil(Math.log(n) / Math.log(2));  // Height of the tree
            int numLeaves = (int) Math.pow(2, height);  // Number of leaf nodes
            ArrayList<Double> tree = new ArrayList<>();
            int treeSize = 2 * numLeaves - 1;
            Random rand = new Random();
            for (int j = 0; j < treeSize; j++) {
                Double noise = rand.nextGaussian() * sigma;
                tree.add(noise);
            }
            return tree;
        }

        public Double addToTree(int i, Double value) {
            int numLeaves = (tree.size() + 1) / 2;
            {
                int nodeIndex = numLeaves - 1 + i;

                while (nodeIndex > 0) {
                    Double curVal = tree.get(nodeIndex);
                    tree.set(nodeIndex, curVal + value);
                    nodeIndex = (nodeIndex - 1) / 2;
                }
                tree.set(nodeIndex, tree.get(nodeIndex) + value);
            }

            double sPriv = 0.00;
            String indexBinaryRepr = Integer.toBinaryString(i + 1); // numeration from 1 to n
            indexBinaryRepr = String.format("%" + (height + 1) + "s", indexBinaryRepr).replace(' ', '0');  // Pad with zeros
            String pathBinary = Integer.toBinaryString(i);
            pathBinary = String.format("%" + height + "s", pathBinary).replace(' ', '0');

            int nodeIndex = 0;
            for (int j = 0; j < height + 1; j++) {
                char vertexBit = indexBinaryRepr.charAt(j); // determines if we add the node to the privSum

                if (vertexBit == '1') {
                    int leftSibling = (nodeIndex % 2 == 0) ? nodeIndex - 1 : nodeIndex; // left sibling or itself

                    if (nodeIndex == 0) { // if current node is the root
                        leftSibling = nodeIndex;
                    }

                    sPriv = sPriv + tree.get(leftSibling);
                }
                if (j < height) {
                    char pathBit = pathBinary.charAt(j);
                    int leftChild = 2 * nodeIndex + 1;
                    int rightChild = 2 * nodeIndex + 2;

                    nodeIndex = (pathBit == '0') ? leftChild : rightChild;
                }

            }
            curPrivateSum = sPriv;
            return sPriv;
        }

        public List<Double> getTree() {
            return new ArrayList<>(tree);
        }
    }
}