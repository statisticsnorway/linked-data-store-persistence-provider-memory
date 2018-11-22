package no.ssb.lds.core.persistence.memory;

import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.core.persistence.foundationdb.FoundationDBDirectory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class MemoryDirectory implements FoundationDBDirectory {

    final int prefixLength;

    final byte[] nextPrefix;
    final ConcurrentMap<Tuple, Subspace> subspaceByTuple = new ConcurrentHashMap<>();

    MemoryDirectory(int prefixLength) {
        this.prefixLength = prefixLength;
        this.nextPrefix = new byte[prefixLength];
    }

    @Override
    public CompletableFuture<Subspace> createOrOpen(Tuple key) throws PersistenceException {
        Subspace subspace = subspaceByTuple.computeIfAbsent(key, k -> new Subspace(allocateUnusedShortPrefix()));
        return CompletableFuture.completedFuture(subspace);
    }

    byte[] allocateUnusedShortPrefix() {
        synchronized (nextPrefix) {
            // copy next prefix
            byte[] prefix = new byte[prefixLength];
            System.arraycopy(nextPrefix, 0, prefix, 0, prefix.length);
            // increment prefix
            for (int i = prefixLength - 1; i >= 0; i--)
                if (++nextPrefix[i] != 0) {
                    // rolled around least-significant byte, increment the more significant byte as well
                    break;
                }
            return prefix;
        }
    }
}
