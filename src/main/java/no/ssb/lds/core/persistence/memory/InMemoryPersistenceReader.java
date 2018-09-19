package no.ssb.lds.core.persistence.memory;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Map;

class InMemoryPersistenceReader {

    private final InMemoryPersistenceDataSource datasource;

    InMemoryPersistenceReader(InMemoryPersistenceDataSource datasource) {
        this.datasource = datasource;
    }

    JSONObject getEntity(String namespace, String entity, String id) {
        String json = datasource.map().get(namespace + entity + id);
        return (json == null ? null : new JSONObject(json));
    }

    JSONArray getEntities(String namespace, String entity) {
        JSONArray result = new JSONArray();
        String key = namespace + entity;
        int n = 0;
        for (Map.Entry<String, String> entry : datasource.map().entrySet()) {
            if (n == 250) break;
            if (entry.getKey().startsWith(key)) {
                result.put(new JSONObject(entry.getValue()));
                n++;
            }
        }
        return result;
    }
}
