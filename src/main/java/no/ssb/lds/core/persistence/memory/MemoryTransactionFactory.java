package no.ssb.lds.core.persistence.memory;

import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.TransactionFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

public class MemoryTransactionFactory implements TransactionFactory {

    final ExecutorService executor = Executors.newFixedThreadPool(8, r -> {
        Thread thread = new Thread(r);
        thread.setName("Memory-Pool-" + thread.getName());
        return thread;
    });
    final ConcurrentMap<String, ConcurrentNavigableMap<byte[], byte[]>> indexByIndex = new ConcurrentSkipListMap<>();
    final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final boolean cancelTxOnClose;

    public MemoryTransactionFactory(boolean cancelTxOnClose) {
        this.cancelTxOnClose = cancelTxOnClose;
    }

    @Override
    public <T> CompletableFuture<T> runAsyncInIsolatedTransaction(Function<? super Transaction, ? extends T> retryable, boolean readOnly) {
        return CompletableFuture.supplyAsync(() -> {
            try (MemoryTransaction tx = createTransaction(readOnly)) {
                return retryable.apply(tx);
            }
        }, executor);
    }

    @Override
    public MemoryTransaction createTransaction(boolean readOnly) throws PersistenceException {
        try {
            return new MemoryTransaction(executor, indexByIndex, readOnly ? readWriteLock.readLock() : readWriteLock.writeLock(), cancelTxOnClose);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        shutdownAndAwaitTermination(executor);
    }

    void shutdownAndAwaitTermination(ExecutorService pool) {
        pool.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
                pool.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!pool.awaitTermination(10, TimeUnit.SECONDS))
                    System.err.println("Pool did not terminate");
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            pool.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }
}
