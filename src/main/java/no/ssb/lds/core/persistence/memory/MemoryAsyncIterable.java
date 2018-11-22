package no.ssb.lds.core.persistence.memory;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;

import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class MemoryAsyncIterable implements AsyncIterable<KeyValue> {

    final NavigableMap<byte[], byte[]> map;

    public MemoryAsyncIterable(NavigableMap<byte[], byte[]> map) {
        // this.map = new TreeMap<>((o1, o2) -> Arrays.compareUnsigned(o1, o2));
        // this.map.putAll(map);
        this.map = map;
    }

    @Override
    public AsyncIterator<KeyValue> iterator() {
        return new MemoryAsyncIterator(map);
    }

    @Override
    public CompletableFuture<List<KeyValue>> asList() {
        return CompletableFuture.completedFuture(map.entrySet().stream().map(e -> new KeyValue(e.getKey(), e.getValue())).collect(Collectors.toList()));
    }
}
