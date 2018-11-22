package no.ssb.lds.core.persistence.memory;

import com.apple.foundationdb.tuple.Tuple;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.LinkedHashSet;
import java.util.Set;

public class MemoryDirectoryTest {

    @Test
    public void thatPrefixAllocatorAllocatesUniquePrefixesEveryTime() {
        Set<Tuple> set = new LinkedHashSet<>();
        int prefixLength = 2;
        long possibilities = Math.round(Math.pow(256, prefixLength));
        MemoryDirectory directory = new MemoryDirectory(prefixLength);
        for (int i = 0; i < possibilities; i++) {
            byte[] prefix = directory.allocateUnusedShortPrefix();
            Tuple tuple = Tuple.from(prefix);
            Assert.assertFalse(set.contains(tuple));
            set.add(tuple);
        }
    }
}
