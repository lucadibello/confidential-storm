package ch.usi.inf.confidentialstorm.enclave.dp;

import java.io.Serializable;
import java.util.*;

public class BinaryAggregationTreeOriginal implements Serializable {
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