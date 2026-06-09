package ch.usi.inf.examples.microbatch_baseline.tools;

import ch.usi.inf.examples.microbatch_baseline.util.ZipfMandelbrotDistribution;
import org.apache.storm.Config;
import org.apache.storm.serialization.KryoValuesSerializer;
import org.apache.storm.tuple.Values;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;

/**
 * Standalone probe that estimates the on-wire serialized footprint of one
 * baseline data tuple by running {@link KryoValuesSerializer} (the same
 * serializer Storm uses on the wire) over a representative sample drawn from
 * the spout's Zipf-Mandelbrot key and user distributions.
 *
 * <p>Run from the module root. Storm is a {@code provided} dependency, so use
 * either the Storm CLI (preferred) or maven exec with the {@code test} scope
 * (which includes provided deps):
 * <pre>
 *   mvn -q -DskipTests package
 *   storm jar target/microbatch-benchmark-baseline-1.0-SNAPSHOT.jar \
 *       ch.usi.inf.examples.microbatch_baseline.tools.TupleSizeProbe
 *
 *   # or:
 *   mvn -q exec:java \
 *       -Dexec.mainClass=ch.usi.inf.examples.microbatch_baseline.tools.TupleSizeProbe \
 *       -Dexec.classpathScope=test
 * </pre>
 *
 * <p>Flags (all optional, system-property form: {@code -Dprobe.samples=...}):
 * <ul>
 *   <li>{@code probe.samples} — number of tuples to serialize (default 200_000)</li>
 *   <li>{@code probe.num.users} — user-id range (default 10_000_000)</li>
 *   <li>{@code probe.num.keys} — key range (default 1_000_000)</li>
 *   <li>{@code probe.seed} — RNG seed (default 42)</li>
 * </ul>
 *
 * <p>The result is the value you should pass as {@code --bytes-per-tuple} to
 * the micro-batch topology. Pass the <em>same</em> value to the confidential
 * variant so both pipelines see identical record counts per nominal batch
 * size — workload is matched on logical tuples, not on on-wire bytes.
 */
public final class TupleSizeProbe {

    private TupleSizeProbe() {}

    public static void main(String[] args) {
        int samples = Integer.getInteger("probe.samples", 200_000);
        int numUsers = Integer.getInteger("probe.num.users", 10_000_000);
        int numKeys = Integer.getInteger("probe.num.keys", 1_000_000);
        long seed = Long.getLong("probe.seed", 42L);

        System.out.printf("Probing baseline tuple size: samples=%d, numUsers=%d, numKeys=%d, seed=%d%n",
                samples, numUsers, numKeys, seed);

        Random rng = new Random(seed);
        ZipfMandelbrotDistribution keyDist = new ZipfMandelbrotDistribution(numKeys, 1000, 1.4, rng);

        Map<String, Object> conf = stormDefaultConfig();
        KryoValuesSerializer ser = new KryoValuesSerializer(conf);

        int[] sizes = new int[samples];

        long totalBytes = 0L;
        int min = Integer.MAX_VALUE, max = 0;

        for (int i = 0; i < samples; i++) {
            int userId = rng.nextInt(numUsers);
            String key = Integer.toString(keyDist.sample());
            String userIdStr = Integer.toString(userId);

            Values v = new Values(key, 1.0, userIdStr, userIdStr);
            byte[] bytes = ser.serialize(v);

            int len = bytes.length;
            sizes[i] = len;
            totalBytes += len;
            if (len < min) min = len;
            if (len > max) max = len;
        }

        double mean = (double) totalBytes / samples;

        Arrays.sort(sizes);
        int p50 = sizes[samples / 2];
        int p90 = sizes[(int) (samples * 0.90)];
        int p99 = sizes[(int) (samples * 0.99)];

        System.out.println("=========================================================");
        System.out.printf("samples         : %d%n", samples);
        System.out.printf("total bytes     : %d (%.3f MiB)%n", totalBytes, totalBytes / (1024.0 * 1024.0));
        System.out.printf("mean size       : %.2f bytes%n", mean);
        System.out.printf("p50 size        : %d bytes%n", p50);
        System.out.printf("p90 size        : %d bytes%n", p90);
        System.out.printf("p99 size        : %d bytes%n", p99);
        System.out.printf("min size        : %d bytes%n", min);
        System.out.printf("max size        : %d bytes%n", max);
        System.out.println();
        System.out.printf("Recommended --bytes-per-tuple : %d (rounded-up mean)%n",
                (long) Math.ceil(mean));
        System.out.println("=========================================================");
        System.out.println();
        System.out.println("Workload sizing:");
        for (double sizeGb : new double[] { 1.0, 2.0, 5.0 }) {
            long records = (long) Math.ceil(sizeGb * 1024.0 * 1024.0 * 1024.0 / mean);
            System.out.printf("  size=%.1f GB -> %d records%n", sizeGb, records);
        }
        System.out.println();
        System.out.println("Pass the SAME --bytes-per-tuple value to both the baseline and the");
        System.out.println("confidential micro-batch topologies so the record count per nominal");
        System.out.println("batch size matches across pipelines (logical workload equality).");
    }

    /**
     * Builds the same topology conf the micro-batch topologies install,
     * layered on top of Storm's packaged defaults so {@link KryoValuesSerializer}
     * finds all of the keys it expects (kryo factory, tuple serializer, etc.).
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> stormDefaultConfig() {
        Map<String, Object> defaults = org.apache.storm.utils.Utils.readDefaultConfig();
        Config conf = new Config();
        conf.registerSerialization(java.util.LinkedHashMap.class);
        conf.registerSerialization(java.util.Collections.emptyMap().getClass());
        defaults.putAll(conf);
        return defaults;
    }
}
