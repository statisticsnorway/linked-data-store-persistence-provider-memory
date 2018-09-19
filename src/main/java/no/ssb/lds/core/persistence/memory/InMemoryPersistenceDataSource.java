package no.ssb.lds.core.persistence.memory;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class InMemoryPersistenceDataSource implements Closeable {

    private final Map<String, String> dataMap;

    InMemoryPersistenceDataSource() {
        dataMap = new ConcurrentHashMap<>();
    }

    Map<String, String> map() {
        return dataMap;
    }

    @Override
    public void close() {
        dataMap.clear();
    }
}
