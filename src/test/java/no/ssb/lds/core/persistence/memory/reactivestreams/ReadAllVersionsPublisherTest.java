package no.ssb.lds.core.persistence.memory.reactivestreams;

import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.json.JsonPersistence;
import no.ssb.lds.api.persistence.streaming.Fragment;
import no.ssb.lds.api.persistence.streaming.Persistence;
import no.ssb.lds.core.persistence.memory.MemoryInitializer;
import org.json.JSONObject;
import org.reactivestreams.tck.TestEnvironment;
import org.reactivestreams.tck.flow.FlowPublisherVerification;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Flow;

public class ReadAllVersionsPublisherTest extends FlowPublisherVerification<Fragment> {

    public static final long DEFAULT_TIMEOUT_MILLIS = 100;
    public static final long DEFAULT_NO_SIGNALS_TIMEOUT_MILLIS = 100;
    public static final long PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS = 500;

    final MemoryInitializer initializer;
    final JsonPersistence jsonPersistence;
    final Persistence persistence;

    Transaction tx;

    public ReadAllVersionsPublisherTest() {
        super(new TestEnvironment(DEFAULT_TIMEOUT_MILLIS, DEFAULT_NO_SIGNALS_TIMEOUT_MILLIS), PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS);
        initializer = new MemoryInitializer();
        initializer.initialize("ns",
                Map.of(
                        "persistence.mem.wait.min", "0",
                        "persistence.mem.wait.max", "0",
                        "persistence.fragment.capacity", "8192"
                ),
                Set.of("A")
        );
        jsonPersistence = initializer.getJsonPersistence();
        persistence = initializer.getPersistence();
    }

    @BeforeMethod
    public void beginTransaction() {
        tx = persistence.createTransaction(false);
    }

    @AfterMethod
    public void endTransaction() {
        tx.cancel();
    }

    JSONObject createJSON(long elements) {
        JSONObject jsonObject = new JSONObject();
        for (int i = 0; i < Math.min(elements, 1000); i++) {
            jsonObject.put("k" + i, "v" + i);
        }
        return jsonObject;
    }

    @Override
    public Flow.Publisher<Fragment> createFlowPublisher(long elements) {
        ZonedDateTime timestamp = ZonedDateTime.now(ZoneId.of("Etc/UTC"));
        JSONObject jsonObject = createJSON(elements);
        JsonDocument document = new JsonDocument(new DocumentKey("ns", "A", "1", timestamp), jsonObject);
        tx.cancel();
        tx = persistence.createTransaction(false);
        jsonPersistence.createOrOverwrite(tx, document, null);
        return persistence.readAllVersions(tx, "ns", "A", "1", null, 10000);
    }

    @Override
    public Flow.Publisher<Fragment> createFailedFlowPublisher() {
        return persistence.read(null, ZonedDateTime.now(ZoneId.of("Etc/UTC")), "ns", "A", "1");
    }

    @Override
    public long boundedDepthOfOnNextAndRequestRecursion() {
        return 1;
    }
}
