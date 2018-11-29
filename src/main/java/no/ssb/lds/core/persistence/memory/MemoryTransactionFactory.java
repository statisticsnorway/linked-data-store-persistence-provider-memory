package no.ssb.lds.core.persistence.memory;

import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.TransactionFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

public class MemoryTransactionFactory implements TransactionFactory {

    final int prefixLength;
    final ConcurrentMap<String, ConcurrentNavigableMap<byte[], byte[]>> indexByPrefix = new ConcurrentSkipListMap<>();
    final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public MemoryTransactionFactory(int prefixLength) {
        this.prefixLength = prefixLength;
    }

    @Override
    public <T> CompletableFuture<T> runAsyncInIsolatedTransaction(Function<? super Transaction, ? extends CompletableFuture<T>> retryable) {
        ForkJoinTask<? extends CompletableFuture<T>> task = ForkJoinPool.commonPool().submit(() -> {
            try (Transaction tx = createTransaction(false)) {
                return retryable.apply(tx);
            }
        });
        return task.join();
    }

    @Override
    public Transaction createTransaction(boolean readOnly) throws PersistenceException {
        try {
            return new MemoryTransaction(indexByPrefix, prefixLength, readOnly ? readWriteLock.readLock() : readWriteLock.writeLock());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
    }
}
