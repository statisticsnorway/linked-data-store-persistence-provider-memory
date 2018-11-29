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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;

class MemoryTransaction implements OrderedKeyValueTransaction {

    static class TransactionLogElement {
        final String index;
        final Map<byte[], byte[]> map;

        TransactionLogElement(String index, Map<byte[], byte[]> map) {
            this.index = index;
            this.map = new LinkedHashMap<>(map);
        }
    }

    final ConcurrentMap<String, ConcurrentNavigableMap<byte[], byte[]>> mapByIndex;
    final Lock lock;
    final AtomicBoolean locked = new AtomicBoolean(true);
    final TransactionStatistics statistics = new TransactionStatistics();
    final List<TransactionLogElement> transactionLog = new CopyOnWriteArrayList<>();

    public MemoryTransaction(ConcurrentMap<String, ConcurrentNavigableMap<byte[], byte[]>> mapByIndex, Lock lock) throws InterruptedException {
        this.mapByIndex = mapByIndex;
        this.lock = lock;
        lock.lockInterruptibly();
    }

    ConcurrentNavigableMap<byte[], byte[]> getMapByIndex(String index) {
        if (!locked.get()) {
            throw new IllegalStateException("Attempting to access database from within a closed transaction.");
        }
        return mapByIndex.computeIfAbsent(index, k -> new ConcurrentSkipListMap<>((o1, o2) -> Arrays.compareUnsigned(o1, o2)));
    }

    Function<KeySelector, byte[]> keySelectorFunction(String index) {
        return ks -> {
            NavigableMap<byte[], byte[]> map = getMapByIndex(index);
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

    @Override
    public CompletableFuture<TransactionStatistics> commit() {
        try {
            return CompletableFuture.completedFuture(statistics);
        } finally {
            if (locked.compareAndSet(true, false)) {
                lock.unlock();
            }
        }
    }

    @Override
    public CompletableFuture<TransactionStatistics> cancel() {
        try {
            for (int i = transactionLog.size() - 1; i >= 0; i--) {
                TransactionLogElement element = transactionLog.get(i);
                ConcurrentNavigableMap<byte[], byte[]> mapByPrefix = getMapByIndex(element.index);
                for (Map.Entry<byte[], byte[]> entry : element.map.entrySet()) {
                    if (entry.getValue() == null) {
                        mapByPrefix.remove(entry.getKey());
                    } else {
                        mapByPrefix.put(entry.getKey(), entry.getValue());
                    }
                }
            }
            transactionLog.clear();
            return CompletableFuture.completedFuture(statistics);
        } finally {
            if (locked.compareAndSet(true, false)) {
                lock.unlock();
            }
        }
    }

    @Override
    public void clearRange(Range range, String index) {
        NavigableMap<byte[], byte[]> subMap = getMapByIndex(index).subMap(range.begin, true, range.end, false);
        transactionLog.add(new TransactionLogElement(index, subMap));
        subMap.clear();
    }

    @Override
    public void clear(byte[] key, String index) {
        byte[] previousValue = getMapByIndex(index).remove(key);
        LinkedHashMap<byte[], byte[]> map = new LinkedHashMap<>();
        map.put(key, previousValue);
        transactionLog.add(new TransactionLogElement(index, map));
    }

    @Override
    public void set(byte[] key, byte[] value, String index) {
        byte[] previousValue = getMapByIndex(index).put(key, value);
        LinkedHashMap<byte[], byte[]> map = new LinkedHashMap<>();
        map.put(key, previousValue);
        transactionLog.add(new TransactionLogElement(index, map));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(Range range, String index) {
        return new MemoryAsyncIterable(getMapByIndex(index).subMap(range.begin, true, range.end, false));
    }

    @Override
    public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, String index) {
        return getRange(begin, end, -1, StreamingMode.ITERATOR, index);
    }

    @Override
    public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, int limit, StreamingMode streamingMode, String index) {
        byte[] fromInclusive = keySelectorFunction(index).apply(begin);
        byte[] toExclusive = keySelectorFunction(index).apply(end);
        if (fromInclusive != null) {
            if (toExclusive != null) {
                int compareUnsigned = Arrays.compareUnsigned(fromInclusive, toExclusive);
                if (compareUnsigned >= 0) {
                    // negative or empty range
                    return new MemoryAsyncIterable(Collections.emptyNavigableMap());
                }
                // positive range
                return new MemoryAsyncIterable(getMapByIndex(index).subMap(fromInclusive, true, toExclusive, false));
            }
            // toExclusive is null
            if (end.getOffset() == 0) {
                // end less
                return new MemoryAsyncIterable(Collections.emptyNavigableMap());
            }
            // end greater
            return new MemoryAsyncIterable(getMapByIndex(index).tailMap(fromInclusive, true));
        } else {
            // fromInclusive == null
            if (toExclusive != null) {
                if (begin.getOffset() == 0) {
                    // begin less
                    getMapByIndex(index).headMap(toExclusive, false);
                }
                // begin greater
                return new MemoryAsyncIterable(Collections.emptyNavigableMap());
            }
            // toExclusive == null
            if (begin.getOffset() == 0 && end.getOffset() == 1) {
                // begin less and end greater
                return new MemoryAsyncIterable(getMapByIndex(index));
            }
            // begin greater or end less
            return new MemoryAsyncIterable(Collections.emptyNavigableMap());
        }
    }
}
