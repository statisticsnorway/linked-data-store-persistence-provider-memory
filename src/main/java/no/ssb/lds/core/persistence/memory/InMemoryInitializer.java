package no.ssb.lds.core.persistence.memory;

import no.ssb.lds.api.persistence.Persistence;
import no.ssb.lds.api.persistence.PersistenceInitializer;
import no.ssb.lds.api.persistence.ProviderName;

import java.util.Map;
import java.util.Set;

@ProviderName("mem")
public class InMemoryInitializer implements PersistenceInitializer {

    @Override
    public String persistenceProviderId() {
        return "mem";
    }

    @Override
    public Set<String> configurationKeys() {
        return Set.of(
                "persistence.mem.wait.min",
                "persistence.mem.wait.max"
        );
    }

    @Override
    public Persistence initialize(String defaultNamespace, Map<String, String> configuration, Set<String> managedDomains) {
        int waitMinMs = Integer.parseInt(configuration.get("persistence.mem.wait.min"));
        int waitMaxMs = Integer.parseInt(configuration.get("persistence.mem.wait.max"));
        return new InMemoryPersistence(new InMemoryPersistenceProvider(waitMinMs, waitMaxMs));
    }
}
