package no.ssb.lds.core.persistence.memory;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class MemoryTransactionTest {

    @Test
    public void thatTransactionRollsBackOnCancel() {
        MemoryTransactionFactory factory = new MemoryTransactionFactory();
        try (MemoryTransaction tx = factory.createTransaction(false)) {
            tx.set(new byte[]{1, 2, 2}, new byte[]{1, 2, 2}, "i1");
            tx.set(new byte[]{1, 2, 3}, new byte[]{1, 2, 3}, "i1");
            tx.set(new byte[]{1, 2, 3, 0}, new byte[]{1, 2, 3, 0}, "i1");
            tx.set(new byte[]{1, 2, 3, 1}, new byte[]{1, 2, 3, 1}, "i1");
            tx.set(new byte[]{1, 2, 3, 2}, new byte[]{1, 2, 3, 2}, "i1");
            tx.set(new byte[]{1, 2, 3, 3}, new byte[]{1, 2, 3, 3}, "i1");
            tx.set(new byte[]{1, 2, 3, 4}, new byte[]{1, 2, 3, 4}, "i1");
            tx.set(new byte[]{1, 2, 3, 5}, new byte[]{1, 2, 3, 5}, "i1");
            tx.set(new byte[]{1, 2, 3, 5, 1}, new byte[]{1, 2, 3, 5, 1}, "i1");
            tx.set(new byte[]{1, 2, 3, 6}, new byte[]{1, 2, 3, 6}, "i1");
            tx.set(new byte[]{1, 2, 3, 7}, new byte[]{1, 2, 3, 7}, "i1");
            tx.set(new byte[]{1, 2, 3, 8}, new byte[]{1, 2, 3, 8}, "i1");
            tx.set(new byte[]{1, 2, 3, 9}, new byte[]{1, 2, 3, 9}, "i1");
            tx.set(new byte[]{1, 2, 4}, new byte[]{1, 2, 4}, "i1");
            tx.set(new byte[]{1, 2, 5}, new byte[]{1, 2, 5}, "i1");
        }

        // capture initial state
        List<KeyValue> expected = copyDatabase(factory);

        // change data within a transaction and assert that change is visible within transaction context, then roll-back
        try (MemoryTransaction tx = factory.createTransaction(false)) {

            // clear elements in a specific range
            List<KeyValue> beforeClearRange = getRange(tx,
                    firstGreaterOrEqual(new byte[]{1, 2, 3, 4, 5}),
                    KeySelector.lastLessOrEqual(new byte[]{1, 2, 3, 6}), "i1");
            Assert.assertEquals(beforeClearRange.size(), 2); // confirm that range contains 2 elements
            tx.clearRange(Range.startsWith(new byte[]{1, 2, 3, 5}), "i1");
            List<KeyValue> afterClearRange = getRange(tx,
                    firstGreaterOrEqual(new byte[]{1, 2, 3, 4, 5}),
                    KeySelector.lastLessOrEqual(new byte[]{1, 2, 3, 6}), "i1");
            Assert.assertEquals(afterClearRange.size(), 0); // confirm that range was cleared

            // replace an existing value with a new value
            List<KeyValue> rangeResultBeforeNewValue = getRange(tx, Range.startsWith(new byte[]{1, 2, 3, 7}), "i1");
            tx.set(new byte[]{1, 2, 3, 7}, new byte[]{93, 12, 51, 67, 125, -42}, "i1");
            List<KeyValue> rangeResultAfterNewValue = getRange(tx, Range.startsWith(new byte[]{1, 2, 3, 7}), "i1");
            Assert.assertNotEquals(rangeResultAfterNewValue, rangeResultBeforeNewValue); // confirm that value has changed

            List<KeyValue> withAllChanges = copyDatabase(factory);
            Assert.assertNotEquals(withAllChanges, expected); // confirm that database state is not the initial state

            tx.cancel();

            try {
                getRange(tx, Range.startsWith(new byte[]{1, 2, 3, 7}), "i1");
                Assert.fail("Accessing database from within a cancelled transaction is expected to be illegal.");
            } catch (IllegalStateException e) {
                // expected
            }
        }

        // check that cancel works
        List<KeyValue> actual = copyDatabase(factory);
        Assert.assertEquals(actual, expected); // confirm that database state is identical with the initial state
    }

    private List<KeyValue> getRange(MemoryTransaction tx, Range range, String index) {
        List<KeyValue> rangeResult = new ArrayList<>();
        AsyncIterable<KeyValue> iterable = tx.getRange(range, index);
        AsyncIterator<KeyValue> iterator = iterable.iterator();
        while (iterator.hasNext()) {
            KeyValue kv = iterator.next();
            rangeResult.add(kv);
        }
        return rangeResult;
    }

    private List<KeyValue> getRange(MemoryTransaction tx, KeySelector fromInclusive, KeySelector toExclusive, String index) {
        List<KeyValue> rangeResult = new ArrayList<>();
        AsyncIterable<KeyValue> asyncIterable = tx.getRange(fromInclusive, toExclusive, index);
        AsyncIterator<KeyValue> iterator = asyncIterable.iterator();
        while (iterator.hasNext()) {
            KeyValue kv = iterator.next();
            rangeResult.add(kv);
        }
        return rangeResult;
    }

    private List<KeyValue> copyDatabase(MemoryTransactionFactory factory) {
        List<KeyValue> copyOfState = new ArrayList<>();
        // capture entire database state
        try (MemoryTransaction tx = factory.createTransaction(true)) {
            AsyncIterable<KeyValue> asyncIterable = tx.getRange(firstGreaterOrEqual(new byte[]{1, 2, 3}), KeySelector.lastLessOrEqual(new byte[]{1, 2, 4}), "i1");
            AsyncIterator<KeyValue> iterator = asyncIterable.iterator();
            while (iterator.hasNext()) {
                KeyValue kv = iterator.next();
                copyOfState.add(kv);
            }
        }
        return copyOfState;
    }

    private KeySelector firstGreaterOrEqual(byte[] key) {
        return new KeySelector(key, true, 1);
    }
}
