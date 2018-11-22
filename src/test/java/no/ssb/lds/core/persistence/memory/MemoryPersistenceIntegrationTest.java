package no.ssb.lds.core.persistence.memory;

import no.ssb.lds.api.persistence.buffered.DefaultBufferedPersistence;
import no.ssb.lds.core.persistence.test.BufferedPersistenceIntegration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.Map;
import java.util.Set;

public class MemoryPersistenceIntegrationTest extends BufferedPersistenceIntegration {

    public MemoryPersistenceIntegrationTest() {
        super("lds-provider-memory-integration-test");
    }

    @BeforeClass
    public void setup() {
        streaming = new MemoryInitializer().initialize(namespace,
                Map.of("persistence.mem.wait.min", "0",
                        "persistence.mem.wait.max", "0"),
                Set.of("Person", "Address", "FunkyLongAddress"));
        persistence = new DefaultBufferedPersistence(streaming);
    }

    @AfterClass
    public void teardown() {
        if (persistence != null) {
            persistence.close();
        }
    }
}
