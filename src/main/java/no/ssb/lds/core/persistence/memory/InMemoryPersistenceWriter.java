package no.ssb.lds.core.persistence.memory;

import org.json.JSONObject;

import java.util.Random;

class InMemoryPersistenceWriter {

    private final InMemoryPersistenceDataSource datasource;

    private final Random random = new Random(System.currentTimeMillis());
    private final int waitMinMs;
    private final int waitMaxMs;

    InMemoryPersistenceWriter(InMemoryPersistenceDataSource datasource, int waitMinMs, int waitMaxMs) {
        this.datasource = datasource;
        this.waitMinMs = waitMinMs;
        this.waitMaxMs = waitMaxMs;
    }

    boolean createOrOverwriteEntity(String namespace, String entity, String id, JSONObject jsonObject) {
        emulateSlowPersistence();
        String key = namespace + entity + id;
        String previousValue = datasource.map().put(key, jsonObject.toString());
        return previousValue == null;
    }

    boolean deleteEntity(String namespace, String entity, String id) {
        String removedValue = datasource.map().remove(namespace + entity + id);
        return removedValue != null;
    }

    private void emulateSlowPersistence() {
        if (waitMaxMs > 0) {
            try {
                Thread.sleep(waitMinMs + random.nextInt(waitMaxMs - waitMinMs));
            } catch (InterruptedException e) {
            }
        }
    }
}
