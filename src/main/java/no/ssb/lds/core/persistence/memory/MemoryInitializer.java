package no.ssb.lds.core.persistence.memory;

import no.ssb.lds.api.persistence.PersistenceInitializer;
import no.ssb.lds.api.persistence.ProviderName;
import no.ssb.lds.api.persistence.TransactionFactory;
import no.ssb.lds.api.persistence.flattened.DefaultFlattenedPersistence;
import no.ssb.lds.api.persistence.json.BufferedJsonPersistence;
import no.ssb.lds.api.persistence.json.JsonPersistence;
import no.ssb.lds.core.persistence.foundationdb.FoundationDBDirectory;

import java.util.Map;
import java.util.Set;

import static java.util.Optional.ofNullable;

@ProviderName("mem")
public class MemoryInitializer implements PersistenceInitializer {

    @Override
    public String persistenceProviderId() {
        return "mem";
    }

    @Override
    public Set<String> configurationKeys() {
        return Set.of(
                "persistence.mem.wait.min",
                "persistence.mem.wait.max",
                "persistence.fragment.capacity"
        );
    }

    @Override
    public JsonPersistence initialize(String defaultNamespace, Map<String, String> configuration, Set<String> managedDomains) {
        int waitMinMs = Integer.parseInt(configuration.get("persistence.mem.wait.min"));
        int waitMaxMs = Integer.parseInt(configuration.get("persistence.mem.wait.max"));
        int fragmentCapacityBytes = Integer.parseInt(ofNullable(configuration.get("persistence.fragment.capacity")).orElse("8192"));
        TransactionFactory transactionFactory = new MemoryTransactionFactory();
        FoundationDBDirectory foundationDbDirectory = new MemoryDirectory(2);
        return new BufferedJsonPersistence(new DefaultFlattenedPersistence(new MemoryPersistence(transactionFactory, foundationDbDirectory), fragmentCapacityBytes), fragmentCapacityBytes);
    }
}
