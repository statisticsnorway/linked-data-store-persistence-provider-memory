package no.ssb.lds.core.persistence.memory;

import no.ssb.lds.api.persistence.PersistenceException;

class InMemoryPersistenceTransaction implements AutoCloseable {

    InMemoryPersistenceTransaction() {
    }

    void beginTransaction() throws PersistenceException {
        // do nothing
    }

    void endTransaction() throws PersistenceException {
        // do nothing
    }

    Void getClient() {
        return null;
    }

    @Override
    public void close() {
        endTransaction();
    }
}
