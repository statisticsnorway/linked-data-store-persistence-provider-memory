import no.ssb.lds.api.persistence.PersistenceInitializer;
import no.ssb.lds.core.persistence.memory.InMemoryInitializer;

module no.ssb.lds.persistence.memory {
    requires no.ssb.lds.persistence.api;
    requires org.json;

    provides PersistenceInitializer with InMemoryInitializer;
}
