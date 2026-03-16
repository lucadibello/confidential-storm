package ch.usi.inf.examples.synthetic_baseline.util;

import org.apache.commons.math3.util.FastMath;

import java.util.Random;

/**
 * Zipf-Mandelbrot distribution implementation.
 * (copied from synthetic-dp-histogram/host/util)
 */
public class ZipfMandelbrotDistribution {

    private final int N;
    private final double q;
    private final double s;

    private final Random random;
    private final double H_N_q_s;

    private double[] cumulativeProbabilities;

    public ZipfMandelbrotDistribution(int N, double q, double s, Random random) {
        if (N <= 0) throw new IllegalArgumentException("N must be positive");
        if (q < 0) throw new IllegalArgumentException("q must be >= 0");
        if (s <= 0) throw new IllegalArgumentException("s must be > 0");

        this.N = N;
        this.q = q;
        this.s = s;
        this.H_N_q_s = computeHarmonicNumber(N, q, s);
        this.random = random;
    }

    private double computeHarmonicNumber(int N, double q, double s) {
        double H_N_q_s = 0.0;
        for (int i = 1; i <= N; i++) {
            H_N_q_s += 1.0 / FastMath.pow(i + q, s);
        }
        return H_N_q_s;
    }

    private double[] computeCumulativeProbabilities() {
        double[] cdf = new double[N];
        double H_k_q_s = 0.0;

        for (int k = 1; k <= N; k++) {
            H_k_q_s += 1.0 / FastMath.pow(k + q, s);
            cdf[k - 1] = H_k_q_s / H_N_q_s;
        }

        cdf[N - 1] = 1.0;
        return cdf;
    }

    public int sample() {
        if (cumulativeProbabilities == null) {
            cumulativeProbabilities = computeCumulativeProbabilities();
        }

        double u = random.nextDouble();

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
}
