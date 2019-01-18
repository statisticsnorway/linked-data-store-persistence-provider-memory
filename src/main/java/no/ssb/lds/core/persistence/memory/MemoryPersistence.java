package no.ssb.lds.core.persistence.memory;

import no.ssb.lds.api.persistence.TransactionFactory;
import no.ssb.lds.core.persistence.foundationdb.FoundationDBDirectory;
import no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence;

public class MemoryPersistence extends FoundationDBPersistence {

    MemoryPersistence(TransactionFactory transactionFactory, FoundationDBDirectory foundationDBDirectory) {
        super(transactionFactory, foundationDBDirectory);
    }
}