package pl.garnizon.memdb;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Deque;
import java.util.LinkedList;
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
    Supplier<Snapshot> snapshotSupplier;

    @Spy
    Deque<Snapshot> snapshots = new LinkedList<>();

    @BeforeAll
    static public void beforeAll() {
    }

    @Test
    public void baseSnapshotCreationTest() {
        new DatabaseImpl(snapshotSupplier, snapshots);

        verify(snapshotSupplier).get();
    }

    @Test
    public void databaseOperationsTest() {
        final Database database = new DatabaseImpl(() -> snapshot, snapshots);

        database.delete("Name");
        database.set("Name", "Abc");
        database.get("Name");

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
        final Transaction database = new DatabaseImpl(snapshotSupplier, snapshots);

        database.begin();
        database.begin();
        database.rollback();

        verify(snapshotSupplier, times(3)).get();
        assertThat(snapshots.size()).isEqualTo(1);
    }

    @Test
    public void commitTransactionTest() {
        when(snapshotSupplier.get()).thenReturn(snapshot);
        doNothing().when(snapshot).merge(any());

        final Transaction database = new DatabaseImpl(snapshotSupplier, snapshots);

        database.begin();
        database.begin();
        database.commit();

        verify(snapshotSupplier, times(3)).get();
        assertThat(snapshots.size()).isEqualTo(0);
    }

    @Test
    public void countKeysWithValueTestAfterCommit() {
        final DatabaseImpl database = DatabaseImpl.create();

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
                .count(value)
        ).isEqualTo(2);

        assertThat(database.commit().count(value)).isEqualTo(2);
        assertThat(database.get(name)).isEqualTo(value);
        assertThat(database.get(name2)).isEqualTo(value);
    }

    @Test
    public void countKeysWithValueTestAfterRollback() {
        final DatabaseImpl database = DatabaseImpl.create();

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
        assertThat(database.get(name2)).isEqualTo(SnapshotImpl.TOMBSTONE);
    }

}
