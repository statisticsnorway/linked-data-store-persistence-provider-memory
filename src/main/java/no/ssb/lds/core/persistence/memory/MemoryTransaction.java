package no.ssb.lds.core.persistence.memory;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.async.AsyncIterable;
import no.ssb.lds.api.persistence.TransactionStatistics;
import no.ssb.lds.core.persistence.foundationdb.OrderedKeyValueTransaction;

import java.util.Arrays;
import java.util.Collections;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;

class MemoryTransaction implements OrderedKeyValueTransaction {

    final ConcurrentMap<byte[], ConcurrentNavigableMap<byte[], byte[]>> mapByPrefix;
    final int prefixLength;
    final Lock lock;
    final TransactionStatistics statistics = new TransactionStatistics();
    final Function<KeySelector, byte[]> selectKey;

    public MemoryTransaction(ConcurrentMap<byte[], ConcurrentNavigableMap<byte[], byte[]>> mapByPrefix, int prefixLength, Lock lock) throws InterruptedException {
        this.mapByPrefix = mapByPrefix;
        this.prefixLength = prefixLength;
        this.lock = lock;
        lock.lockInterruptibly();
        selectKey = ks -> {
            NavigableMap<byte[], byte[]> map = getMapByPrefix(ks.getKey());
            String ksToString = ks.toString();
            if (ks.getOffset() == 0) {
                // less
                if (ksToString.contains("true")) {
                    // less than or equal
                    return map.floorKey(ks.getKey());
                }
                // less than
                return map.lowerKey(ks.getKey());
            }
            // greater
            if (ksToString.contains("true")) {
                // greater than or equal
                return map.ceilingKey(ks.getKey());
            }
            // greater than
            return map.higherKey(ks.getKey());
        };
    }

    ConcurrentNavigableMap<byte[], byte[]> getMapByPrefix(byte[] key) {
        byte[] prefix = new byte[prefixLength];
        System.arraycopy(key, 0, prefix, 0, prefix.length);
        return mapByPrefix.computeIfAbsent(prefix, k -> new ConcurrentSkipListMap<>((o1, o2) -> Arrays.compareUnsigned(o1, o2)));
    }

    @Override
    public CompletableFuture<TransactionStatistics> commit() {
        try {
            return CompletableFuture.completedFuture(statistics);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public CompletableFuture<TransactionStatistics> cancel() {
        try {
            throw new UnsupportedOperationException();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void clearRange(Range range, String index) {
        NavigableMap<byte[], byte[]> subMap = getMapByPrefix(range.begin).subMap(range.begin, true, range.end, false);
        subMap.clear();
    }

    @Override
    public void set(byte[] key, byte[] value, String index) {
        getMapByPrefix(key).put(key, value);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(Range range, String index) {
        return new MemoryAsyncIterable(getMapByPrefix(range.begin).subMap(range.begin, true, range.end, false));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, String index) {
        return getRange(begin, end, -1, StreamingMode.ITERATOR, index);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, int limit, StreamingMode streamingMode, String index) {
        byte[] fromInclusive = selectKey.apply(begin);
        byte[] toExclusive = selectKey.apply(end);
        if (fromInclusive != null) {
            if (toExclusive != null) {
                int compareUnsigned = Arrays.compareUnsigned(fromInclusive, toExclusive);
                if (compareUnsigned >= 0) {
                    // negative or empty range
                    return new MemoryAsyncIterable(Collections.emptyNavigableMap());
                }
                // positive range
                return new MemoryAsyncIterable(getMapByPrefix(begin.getKey()).subMap(fromInclusive, true, toExclusive, false));
            }
            // toExclusive is null
            if (end.getOffset() == 0) {
                // end less
                return new MemoryAsyncIterable(Collections.emptyNavigableMap());
            }
            // end greater
            return new MemoryAsyncIterable(getMapByPrefix(begin.getKey()).tailMap(fromInclusive, true));
        } else {
            // fromInclusive == null
            if (toExclusive != null) {
                if (begin.getOffset() == 0) {
                    // begin less
                    getMapByPrefix(begin.getKey()).headMap(toExclusive, false);
                }
                // begin greater
                return new MemoryAsyncIterable(Collections.emptyNavigableMap());
            }
            // toExclusive == null
            if (begin.getOffset() == 0 && end.getOffset() == 1) {
                // begin less and end greater
                return new MemoryAsyncIterable(getMapByPrefix(begin.getKey()));
            }
            // begin greater or end less
            return new MemoryAsyncIterable(Collections.emptyNavigableMap());
        }
    }

    @Override
    public void clear(byte[] key, String index) {
        getMapByPrefix(key).remove(key);
    }
}
