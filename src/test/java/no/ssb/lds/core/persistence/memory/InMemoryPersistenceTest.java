package no.ssb.lds.core.persistence.memory;

import no.ssb.lds.api.persistence.Persistence;
import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import org.json.JSONObject;
import org.testng.annotations.Test;

import java.util.Collections;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class InMemoryPersistenceTest {

    @Test
    public void testSimpleCreateOrOverwrite() {
        Persistence persistence = new InMemoryPersistence(new InMemoryPersistenceProvider(0, 0));
        JSONObject doc = new JSONObject("{\"foo\": \"bar\"}");
        persistence.createOrOverwrite("data", "abc", "1", doc, Collections.emptySet());
    }

    @Test
    public void testCreateOrOverwrite() {
        Persistence persistence = new InMemoryPersistence(new InMemoryPersistenceProvider(0, 0));
        for (Integer n = 0; n < 50; n++) {
            JSONObject doc = new JSONObject("{\"foo\": \"bar\"}");
            persistence.createOrOverwrite("data", "abc", n.toString(), doc, Collections.emptySet());
        }
        for (Integer n = 0; n < 300; n++) {
            JSONObject doc = new JSONObject("{\"foo\": \"bar\"}");
            persistence.createOrOverwrite("data", "def", n.toString(), doc, Collections.emptySet());
        }
        for (Integer n = 0; n < 50; n++) {
            JSONObject doc = new JSONObject("{\"foo\": \"bar\"}");
            persistence.createOrOverwrite("data", "ghi", n.toString(), doc, Collections.emptySet());
        }
    }

    @Test
    public void testRead() {
        Persistence persistence = new InMemoryPersistence(new InMemoryPersistenceProvider(0, 0));
        JSONObject doc = new JSONObject("{\"foo\":\"bar\"}");
        persistence.createOrOverwrite("data", "ghi", "0", doc, Collections.emptySet());
        assertEquals(persistence.read("data", "ghi", "0").toString(), "{\"foo\":\"bar\"}");
    }

    @Test
    public void testDelete() {
        Persistence persistence = new InMemoryPersistence(new InMemoryPersistenceProvider(0, 0));
        JSONObject doc = new JSONObject("{\"foo\":\"bar\"}");
        persistence.createOrOverwrite("data", "ghi", "0", doc, Collections.emptySet());
        assertTrue(persistence.delete("data", "ghi", "0", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS));
        assertNull(persistence.read("data", "ghi", "0"));
    }

    @Test
    public void testFindAll() {
        Persistence persistence = new InMemoryPersistence(new InMemoryPersistenceProvider(0, 0));
        for (Integer n = 0; n < 300; n++) {
            JSONObject doc = new JSONObject("{\"foo\": \"bar\"}");
            persistence.createOrOverwrite("data", "def", n.toString(), doc, Collections.emptySet());
        }
        assertEquals(persistence.findAll("data", "def").length(), 250);
    }

}
