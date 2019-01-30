package no.ssb.lds.core.persistence.memory;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.apple.foundationdb.ReadTransaction.ROW_LIMIT_UNLIMITED;

public class MemoryAsyncIterator implements AsyncIterator<KeyValue> {

    final Executor executor;
    final NavigableMap<byte[], byte[]> map;
    final Iterator<Map.Entry<byte[], byte[]>> internalIterator;
    int produced = 0;
    final int limit;

    public MemoryAsyncIterator(Executor executor, NavigableMap<byte[], byte[]> map, int limit) {
        this.executor = executor;
        this.map = map;
        this.limit = limit;
        Iterator<Map.Entry<byte[], byte[]>> iterator = map.entrySet().iterator();
        synchronized (iterator) {
            this.internalIterator = iterator;
        }
    }

    @Override
    public CompletableFuture<Boolean> onHasNext() {
        return CompletableFuture.supplyAsync(() -> hasNext(), executor);
    }

    @Override
    public boolean hasNext() {
        synchronized (internalIterator) {
            if (limit != ROW_LIMIT_UNLIMITED && produced >= limit) {
                return false;
            }
            return internalIterator.hasNext();
        }
    }

    @Override
    public KeyValue next() {
        synchronized (internalIterator) {
            Map.Entry<byte[], byte[]> nextInternal = internalIterator.next();
            produced++;
            return new KeyValue(nextInternal.getKey(), nextInternal.getValue());
        }
    }

    @Override
    public void cancel() {
    }
}
