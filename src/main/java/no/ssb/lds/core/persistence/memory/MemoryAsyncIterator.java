package no.ssb.lds.core.persistence.memory;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class MemoryAsyncIterator implements AsyncIterator<KeyValue> {

    final Executor executor;
    final NavigableMap<byte[], byte[]> map;
    final Iterator<Map.Entry<byte[], byte[]>> internalIterator;

    public MemoryAsyncIterator(Executor executor, NavigableMap<byte[], byte[]> map) {
        this.executor = executor;
        this.map = map;
        Iterator<Map.Entry<byte[], byte[]>> iterator = map.entrySet().iterator();
        synchronized (iterator) {
            this.internalIterator = iterator;
        }
    }

    @Override
    public CompletableFuture<Boolean> onHasNext() {
        return CompletableFuture.supplyAsync(() -> {
            synchronized (internalIterator) {
                return internalIterator.hasNext();
            }
        }, executor);
    }

    @Override
    public boolean hasNext() {
        synchronized (internalIterator) {
            return internalIterator.hasNext();
        }
    }

    @Override
    public KeyValue next() {
        synchronized (internalIterator) {
            Map.Entry<byte[], byte[]> nextInternal = internalIterator.next();
            return new KeyValue(nextInternal.getKey(), nextInternal.getValue());
        }
    }

    @Override
    public void cancel() {
    }
}
