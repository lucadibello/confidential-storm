package ch.usi.inf.examples.synthetic_baseline.dp;

import java.security.SecureRandom;

import org.apache.commons.math3.util.FastMath;

/**
 * A binary aggregation tree for differential privacy.
 * This class implements a binary tree structure used for computing differentially private
 * prefix sums. Each node in the tree is initialized with Gaussian noise, and values are
 * added along paths from leaves to the root to maintain differential privacy guarantees.
 *
 * <p>
 * Refer to Algorithm 4 in "Differentially Private Stream Processing at Scale" and Appendix C
 * for additional information on the Honaker variance reduction technique.
 * <p>
 * Copied from confidentialstorm/enclave — no SGX dependencies.
 */
public final class BinaryAggregationTree {
    private final double[] tree;
    private final int height;
    private final int num_leaves;
    private final double sigma;
    private final double[] varianceCache;
    private final int[] currentLevel;
    private final int[] nextLevel;

    public BinaryAggregationTree(int n, double sigma) {
        height = (int) FastMath.ceil(FastMath.log(n) / FastMath.log(2));
        num_leaves = 1 << height;
        this.sigma = sigma;
        this.tree = initializeTree(sigma);
        this.varianceCache = precomputeTotalVariances();
        this.currentLevel = new int[num_leaves];
        this.nextLevel = new int[num_leaves];
    }

    public double getHonakerVariance(int i) {
        return varianceCache[i];
    }

    public void addToTree(int i, double x_i) {
        add(i, x_i);
    }

    private void add(int i, double c) {
        int index = num_leaves - 1 + i;
        while (index > 0) {
            tree[index] += c;
            index = (index - 1) / 2;
        }
        tree[0] += c;
    }

    public double getTotalSum(int i) {
        int indexBinary = i + 1;
        int nodeIndex = 0;
        double sPriv = 0f;

        for (int j = 0; j <= height; j++) {
            int levelBit = (indexBinary >> (height - j)) & 1;

            if (levelBit == 1) {
                int leftSibling;
                if (nodeIndex == 0) {
                    leftSibling = 0;
                } else if (nodeIndex % 2 == 0) {
                    leftSibling = nodeIndex - 1;
                } else {
                    leftSibling = nodeIndex;
                }

                int kappa = height - j + 1;
                sPriv += computeHonakerEstimate(leftSibling, kappa);
            }

            if (j < height) {
                int pathBit = (i >> (height - 1 - j)) & 1;
                int leftChild = 2 * nodeIndex + 1;
                int rightChild = leftChild + 1;
                nodeIndex = (pathBit == 0) ? leftChild : rightChild;
            }
        }

        return sPriv;
    }

    private double computeHonakerEstimate(int nodeIndex, int k) {
        double honaker_estimate = 0.0;
        int currentSize = 0;
        int nextSize;

        currentLevel[currentSize++] = nodeIndex;

        for (int j = 0; j < k; j++) {
            double sum_level_j = 0;
            nextSize = 0;

            for (int ci = 0; ci < currentSize; ci++) {
                int idx = currentLevel[ci];
                if (idx < tree.length) {
                    sum_level_j += tree[idx];
                    if (j < k - 1) {
                        nextLevel[nextSize++] = 2 * idx + 1;
                        nextLevel[nextSize++] = 2 * idx + 2;
                    }
                }
            }

            double c_j = (1.0 / (1L << j)) / (2.0 * (1.0 - 1.0 / (1L << k)));
            honaker_estimate += c_j * sum_level_j;

            System.arraycopy(nextLevel, 0, currentLevel, 0, nextSize);
            currentSize = nextSize;
        }

        return honaker_estimate;
    }

    private double[] initializeTree(double sigma) {
        int size = 2 * num_leaves - 1;
        double[] tree = new double[size];
        SecureRandom rnd = new SecureRandom();
        for (int i = 0; i < size; i++) {
            tree[i] = rnd.nextGaussian() * sigma;
        }
        return tree;
    }

    private double[] precomputeTotalVariances() {
        double[] cache = new double[num_leaves];

        for (int i = 0; i < num_leaves; i++) {
            int indexBinary = i + 1;
            int nodeIndex = 0;
            double totalVariance = 0.0;

            for (int j = 0; j <= height; j++) {
                int levelBit = (indexBinary >> (height - j)) & 1;

                if (levelBit == 1) {
                    int kappa = height - j + 1;
                    double nodeVariance = (sigma * sigma) / (2.0 * (1.0 - 1.0 / (1L << kappa)));
                    totalVariance += nodeVariance;
                }

                if (j < height) {
                    int pathBit = (i >> (height - 1 - j)) & 1;
                    int leftChild = 2 * nodeIndex + 1;
                    int rightChild = leftChild + 1;
                    nodeIndex = (pathBit == 0) ? leftChild : rightChild;
                }
            }

            cache[i] = totalVariance;
        }

        return cache;
    }
}
