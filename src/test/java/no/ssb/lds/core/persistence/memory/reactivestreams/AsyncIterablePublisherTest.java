package no.ssb.lds.core.persistence.memory.reactivestreams;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.core.persistence.foundationdb.AsyncIterablePublisher;
import no.ssb.lds.core.persistence.memory.MemoryInitializer;
import no.ssb.lds.core.persistence.memory.MemoryRxPersistence;
import no.ssb.lds.core.persistence.memory.MemoryTransaction;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class AsyncIterablePublisherTest extends PublisherVerification<KeyValue> {

    public static final long DEFAULT_TIMEOUT_MILLIS = 100;
    public static final long DEFAULT_NO_SIGNALS_TIMEOUT_MILLIS = 100;
    public static final long PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS = 500;

    final MemoryInitializer initializer;
    final RxJsonPersistence rxJsonPersistence;
    final MemoryRxPersistence rxPersistence;

    MemoryTransaction tx;

    public AsyncIterablePublisherTest() {
        super(new TestEnvironment(DEFAULT_TIMEOUT_MILLIS, DEFAULT_NO_SIGNALS_TIMEOUT_MILLIS), PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS);
        initializer = new MemoryInitializer();
        initializer.initialize("ns",
                Map.of(
                        "persistence.mem.wait.min", "0",
                        "persistence.mem.wait.max", "0",
                        "persistence.fragment.capacity", "8192"
                ),
                Set.of("A"),
                null
        );
        rxJsonPersistence = initializer.getRxJsonPersistence();
        rxPersistence = initializer.getPersistence();
    }

    @BeforeMethod
    public void beginTransaction() {
        tx = (MemoryTransaction) rxPersistence.createTransaction(false);
    }

    @AfterMethod
    public void endTransaction() {
        tx.cancel();
    }

    @Override
    public Publisher<KeyValue> createPublisher(long elements) {
        rollbackExistingAndBeginNewTransaction();
        for (int i = 0; i < Math.min(elements, 1000); i++) {
            tx.set(ByteBuffer.allocate(4).putInt(i).array(), new byte[0], "AnyIndex");
        }
        return new AsyncIterablePublisher<>(tx.getRange(KeySelector.firstGreaterOrEqual(new byte[]{(byte) 0}), KeySelector.firstGreaterOrEqual(new byte[]{(byte) -1}), (int) elements, StreamingMode.ITERATOR, "AnyIndex"));
    }

    @Override
    public Publisher<KeyValue> createFailedPublisher() {
        return new AsyncIterablePublisher<>(new AsyncIterable<>() {
            @Override
            public AsyncIterator<KeyValue> iterator() {
                return null;
            }

            @Override
            public CompletableFuture<List<KeyValue>> asList() {
                return null;
            }
        });
    }

    private void rollbackExistingAndBeginNewTransaction() {
        tx.cancel();
        tx = (MemoryTransaction) rxPersistence.createTransaction(false);
    }

    @Override
    public long boundedDepthOfOnNextAndRequestRecursion() {
        return 1;
    }
}
