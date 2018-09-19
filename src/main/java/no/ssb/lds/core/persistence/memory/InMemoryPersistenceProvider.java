package no.ssb.lds.core.persistence.memory;

class InMemoryPersistenceProvider {

    private final InMemoryPersistenceDataSource driver;

    private final int waitMinMs;
    private final int waitMaxMs;

    InMemoryPersistenceProvider(int waitMinMs, int waitMaxMs) {
        this.waitMinMs = waitMinMs;
        this.waitMaxMs = waitMaxMs;
        driver = new InMemoryPersistenceDataSource();
    }

    InMemoryPersistenceDataSource driver() {
        return driver;
    }

    InMemoryPersistenceTransaction newTransaction() {
        return new InMemoryPersistenceTransaction();
    }

    InMemoryPersistenceReader newReader() {
        return new InMemoryPersistenceReader(driver);
    }

    InMemoryPersistenceWriter newWriter() {
        return new InMemoryPersistenceWriter(driver, waitMinMs, waitMaxMs);
    }
}
