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

    MemoryPersistence persistence;
    JsonPersistence jsonPersistence;

    @Override
    public String persistenceProviderId() {
        return "mem";
    }

    @Override
    public Set<String> configurationKeys() {
        return Set.of(
                "persistence.fragment.capacity"
        );
    }

    public MemoryPersistence getPersistence() {
        return persistence;
    }

    public JsonPersistence getJsonPersistence() {
        return jsonPersistence;
    }

    @Override
    public JsonPersistence initialize(String defaultNamespace, Map<String, String> configuration, Set<String> managedDomains) {
        int fragmentCapacityBytes = Integer.parseInt(ofNullable(configuration.get("persistence.fragment.capacity")).orElse("8192"));
        boolean cancelTxOnClose = Boolean.parseBoolean(ofNullable(configuration.get("persistence.mem.transaction.cancel-on-close")).orElse("false"));
        TransactionFactory transactionFactory = new MemoryTransactionFactory(cancelTxOnClose);
        FoundationDBDirectory foundationDbDirectory = new MemoryDirectory(2);
        persistence = new MemoryPersistence(transactionFactory, foundationDbDirectory);
        jsonPersistence = new BufferedJsonPersistence(new DefaultFlattenedPersistence(persistence, fragmentCapacityBytes), fragmentCapacityBytes);
        return jsonPersistence;
    }
}
