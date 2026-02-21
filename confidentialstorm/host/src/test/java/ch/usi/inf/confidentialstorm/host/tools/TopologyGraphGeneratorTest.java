package ch.usi.inf.confidentialstorm.host.tools;

import ch.usi.inf.confidentialstorm.common.annotation.ConfidentialTopologyBuilder;
import org.apache.storm.topology.TopologyBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TopologyGraphGeneratorTest {

    @Test
    public void testGenerator(@TempDir Path tempDir) throws Exception {
        File output = tempDir.resolve("topology.graph.enc").toFile();
        
        TopologyGraphGenerator.main(new String[]{
            MockTopology.class.getName(),
            output.getAbsolutePath()
        });

        assertTrue(output.exists());
        // Minimum: 12-byte IV + 16-byte GCM auth tag (even for zero-length plaintext)
        assertTrue(output.length() >= 28, "output too short to contain valid IV + GCM tag");
    }

    public static class MockTopology {
        @ConfidentialTopologyBuilder
        public static TopologyBuilder getBuilder() {
            return new TopologyBuilder();
        }
    }
}
