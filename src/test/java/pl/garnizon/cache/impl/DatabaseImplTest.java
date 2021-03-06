package pl.garnizon.cache.impl;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import pl.garnizon.cache.api.Database;
import pl.garnizon.cache.api.Transaction;
import pl.garnizon.cache.snapshot.api.AbstractSnapshot;
import pl.garnizon.cache.snapshot.api.Snapshot;

import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DatabaseImplTest {

    @Mock
    Snapshot snapshot;

    @Mock
    Supplier<Snapshot<String>> snapshotSupplier;

    @Spy
    Deque<Snapshot<String>> snapshots = new LinkedList<>();

    @Test
    public void baseSnapshotCreationTest() {
        new DatabaseImpl(snapshotSupplier, snapshots);

        verify(snapshotSupplier).get();
    }

    @Test
    public void databaseOperationsTest() {
        final Database<String> database = new DatabaseImpl<>(() -> snapshot, snapshots);

        database.delete("Name")
                .set("Name", "Abc")
                .get("Name");

        verify(snapshot).set(anyString(), anyString());
        verify(snapshot).get(anyString());
        verify(snapshot).delete(anyString());
    }

    @Test
    public void beginTransactionTest() {
        final Transaction database = new DatabaseImpl(snapshotSupplier, snapshots);

        database.begin();

        verify(snapshotSupplier, times(2)).get();
        assertThat(snapshots.size()).isEqualTo(1);
    }

    @Test
    public void rollbackTransactionTest() {
        final Transaction database = new DatabaseImpl<>(snapshotSupplier, snapshots);

        database.begin()
                .begin()
                .rollback();

        verify(snapshotSupplier, times(3)).get();
        assertThat(snapshots.size()).isEqualTo(1);
    }

    @Test
    public void commitTransactionTest() {
        when(snapshotSupplier.get()).thenReturn(snapshot);
        doNothing().when(snapshot).merge(any());

        final Transaction database = new DatabaseImpl(snapshotSupplier, snapshots);

        database.begin()
                .begin()
                .commit();

        verify(snapshotSupplier, times(3)).get();
        assertThat(snapshots.size()).isEqualTo(0);
    }

    @Test
    public void countKeysWithValueTestAfterCommit() {
        final DatabaseImpl<String> database = new DatabaseImpl(SnapshotImplTest::create, new LinkedList<>());

        final String name = "name";
        final String name2 = "name2";
        final String name3 = "name3";
        final String value = "value";

        assertThat(database.set(name, value)
                .begin()
                .set(name3, value)
                .delete(name)
                .delete(name)
                .begin()
                .set(name, value)
                .set(name, value)
                .begin()
                .set(name2, value)
                .delete(name3)
                .count(value)
        ).isEqualTo(2);

        assertThat(database.commit().count(value)).isEqualTo(2);
        assertThat(database.get(name)).isEqualTo(value);
        assertThat(database.get(name2)).isEqualTo(value);
    }

    @Test
    public void countKeysWithValueTestAfterRollback() {
        final DatabaseImpl<String> database = new DatabaseImpl(SnapshotImplTest::create, new LinkedList<>());

        final String name = "name";
        final String name2 = "name2";
        final String value = "value";

        assertThat(database.set(name, value)
                .begin()
                .delete(name)
                .delete(name)
                .begin()
                .set(name, value)
                .set(name, value)
                .begin()
                .set(name2, value)
                .rollback()
                .count(value)
        ).isEqualTo(1);

        assertThat(database.commit().count(value)).isEqualTo(1);
        assertThat(database.get(name)).isEqualTo(value);
        assertThat(database.get(name2)).isEqualTo(SnapshotImplTest.TOMBSTONE);
    }

    private static class SnapshotImplTest extends AbstractSnapshot<String> {

        public static final String TOMBSTONE = "NULL";

        public static SnapshotImplTest create() {
            return new SnapshotImplTest(new HashMap<>(), TreeMultimap.create());
        }

        public SnapshotImplTest(Map<String, String> memtable, Multimap<String, String> invertedIndex) {
            super(memtable, invertedIndex);
        }

        @Override
        public String getTombstone() {
            return TOMBSTONE;
        }
    }

}
