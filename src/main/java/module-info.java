import no.ssb.lds.api.persistence.PersistenceInitializer;
import no.ssb.lds.core.persistence.memory.MemoryInitializer;

module no.ssb.lds.persistence.memory {
    requires no.ssb.lds.persistence.api;
    requires no.ssb.lds.persistence.foundationdb;
    requires fdb.java;

    provides PersistenceInitializer with MemoryInitializer;
}
