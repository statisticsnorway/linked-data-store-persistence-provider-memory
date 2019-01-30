package no.ssb.lds.core.persistence.memory;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;

import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

public class MemoryAsyncIterable implements AsyncIterable<KeyValue> {

    private final Executor executor;
    final NavigableMap<byte[], byte[]> map;
    private final int limit;

    public MemoryAsyncIterable(Executor executor, NavigableMap<byte[], byte[]> map, int limit) {
        this.executor = executor;
        this.map = map;
        this.limit = limit;
    }

    @Override
    public AsyncIterator<KeyValue> iterator() {
        return new MemoryAsyncIterator(executor, map, limit);
    }

    @Override
    public CompletableFuture<List<KeyValue>> asList() {
        return CompletableFuture.supplyAsync(() -> map.entrySet().stream().map(e -> new KeyValue(e.getKey(), e.getValue())).collect(Collectors.toList()), executor);
    }
}
