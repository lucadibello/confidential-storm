package ch.usi.inf.examples.synthetic_dp.host.util;

import org.apache.commons.math3.util.FastMath;

import java.util.Random;

/**
 * Zipf-Mandelbrot distribution implementation.
 * <p>
 * Sources:
 * - <a href="https://en.wikipedia.org/wiki/Zipf%E2%80%93Mandelbrot_law">Zipf-Mandelbrot law</a>
 * - "Random sampling of the Zipf-Mandelbrot distribution as a representation of vocabulary growth" paper
 * - <a href="https://en.wikipedia.org/wiki/Inverse_transform_sampling">Inverse Random Sampling</a>
 * - <a href="https://github.com/gkohri/discreteRNG">discreteRNG C++ implementation</a> - C++ implementation of Zipf-Mandelbrot distribution
 */
public class ZipfMandelbrotDistribution {

    private final int N;
    private final double q;
    private final double s;

    private final Random random;
    private final double H_N_q_s;

    // Cumulative probabilities for inverse transform sampling
    private double[] cumulativeProbabilities;

    /**
     * Constructor method.
     *
     * @param N Number of elements (N \in N^+, N >= 1)
     * @param q Offset parameter (q \in R^+)
     * @param s Exponent parameter (s \in R^+\{0})
     * @param random Random number generator
     */
    public ZipfMandelbrotDistribution(int N, double q, double s, Random random) {
        // Validate parameters
        if (N <= 0) throw new IllegalArgumentException("N must be positive");
        if (q < 0) throw new IllegalArgumentException("q must be >= 0");
        if (s <= 0) throw new IllegalArgumentException("s must be > 0");

        this.N = N;
        this.q = q;
        this.s = s;

        this.H_N_q_s = computeHarmonicNumber(N, q, s);

        this.random = random;
    }

    /**
     * Computes the generalized harmonic number H_{N,q,s} = sum_{i=1}^{N} (1 / (i + q)^s)
     */
    private double computeHarmonicNumber(int N, double q, double s) {
        double H_N_q_s = 0.0;
        for (int i = 1; i <= N; i++) {
            H_N_q_s += 1.0 / FastMath.pow(i + q, s);
        }
        return H_N_q_s;
    }

    /**
     * Computes the cumulative distribution function (CDF) for the Zipf-Mandelbrot distribution.
     */
    private double[] computeCumulativeProbabilities() {
        double[] cdf = new double[N];
        double H_k_q_s = 0.0; // Compute harmonic numbers incrementally

        for (int k = 1; k <= N; k++) {
            H_k_q_s += 1.0 / FastMath.pow(k + q, s);
            cdf[k - 1] = H_k_q_s / H_N_q_s;
        }

        // Ensure the last value is exactly 1.0 to handle floating point precision
        cdf[N - 1] = 1.0;

        return cdf;
    }

    /**
     * Samples a value from the distribution using inverse transform sampling.
     * <p>
     * Source: <a href="https://en.wikipedia.org/wiki/Inverse_transform_sampling">Inverse Random Sampling</a>
     */
    public int sample() {
        // Lazy initialization of cumulative probabilities
        if (cumulativeProbabilities == null) {
            cumulativeProbabilities = computeCumulativeProbabilities();
        }

        double u = random.nextDouble();

        // Binary search for inverse transform sampling
        int left = 0;
        int right = N - 1;
        while (left < right) {
            int mid = (left + right) / 2;
            if (cumulativeProbabilities[mid] < u) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        return left + 1;
    }

    /**
     * Computes the theoretical mean of the distribution.
     * <p>
     * From Wikipedia: Mean = (H_{N,q,s-1} / H_{N,q,s}) - q
     */
    public double getMean() {
        double H_N_q_s_minus_1 = computeHarmonicNumber(N, q, s - 1);
        return (H_N_q_s_minus_1 / H_N_q_s) - q;
    }
}
