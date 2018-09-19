package no.ssb.lds.core.persistence.memory;

import no.ssb.lds.api.persistence.OutgoingLink;
import no.ssb.lds.api.persistence.Persistence;
import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.PersistenceException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;

class InMemoryPersistence implements Persistence {

    private final InMemoryPersistenceProvider provider;

    InMemoryPersistence(InMemoryPersistenceProvider provider) {
        this.provider = provider;
    }

    @Override
    public boolean createOrOverwrite(String namespace, String entity, String id, JSONObject jsonObject, Set<OutgoingLink> links) throws PersistenceException {
        try (InMemoryPersistenceTransaction transaction = provider.newTransaction()) {
            InMemoryPersistenceWriter writer = provider.newWriter();
            boolean created = writer.createOrOverwriteEntity(namespace, entity, id, jsonObject);
            return created;
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public JSONObject read(String namespace, String entity, String id) throws PersistenceException {
        try (InMemoryPersistenceTransaction transaction = provider.newTransaction()) {
            InMemoryPersistenceReader reader = provider.newReader();
            return reader.getEntity(namespace, entity, id);
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public boolean delete(String namespace, String entity, String id, PersistenceDeletePolicy policy) throws PersistenceException {
        try (InMemoryPersistenceTransaction transaction = provider.newTransaction()) {
            InMemoryPersistenceWriter writer = provider.newWriter();
            return writer.deleteEntity(namespace, entity, id);
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public JSONArray findAll(String namespace, String entity) throws PersistenceException {
        try (InMemoryPersistenceTransaction transaction = provider.newTransaction()) {
            InMemoryPersistenceReader reader = provider.newReader();
            return reader.getEntities(namespace, entity);
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public void close() throws PersistenceException {
        if (provider.driver() instanceof Closeable) {
            try {
                ((Closeable) provider.driver()).close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }
}
