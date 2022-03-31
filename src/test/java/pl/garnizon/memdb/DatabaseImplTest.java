package pl.garnizon.memdb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Deque;
import java.util.LinkedList;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DatabaseImplTest {

    public static final String TOMBSTONE = "NULL";

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
        final DatabaseImpl<String> database = new DatabaseImpl(this::getSnapshotWithMockedTombstone, new LinkedList<>());

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
        final DatabaseImpl<String> database = new DatabaseImpl(this::getSnapshotWithMockedTombstone, new LinkedList<>());

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
        assertThat(database.get(name2)).isEqualTo(TOMBSTONE);
    }

    private Snapshot<String> getSnapshotWithMockedTombstone() {
        final SnapshotImpl snapshotMock = Mockito.spy(SnapshotImpl.create());
        lenient().when(snapshotMock.getTombstone()).thenReturn(TOMBSTONE);
        return snapshotMock;
    }

}
