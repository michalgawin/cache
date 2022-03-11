package pl.garnizon.memdb;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.logging.Logger;

public class DatabaseImpl implements TransactionalDatabase {

    public static final String NO_TRANSACTION = "NO TRANSACTION";

    private final Supplier<Snapshot> snapshotSupplier;
    private final Snapshot snapshot;
    private final Deque<Snapshot> snapshots;
    private Snapshot currentSnapshot;

    public static DatabaseImpl create() {
        return new DatabaseImpl(SnapshotImpl::create, new LinkedList<>());
    }

    DatabaseImpl(Supplier<Snapshot> snapshotSupplier, Deque<Snapshot> snapshots) {
        this.snapshotSupplier = snapshotSupplier;
        this.snapshots = snapshots;

        this.snapshot = createSnapshot();
        this.currentSnapshot = this.snapshot;
    }

    @Override
    public DatabaseImpl begin() {
        Snapshot t = createSnapshot();
        this.snapshots.add(t);
        this.currentSnapshot = t;

        return this;
    }

    @Override
    public DatabaseImpl rollback() {
        if (isOpen()) {
            this.snapshots.pollLast();
            this.currentSnapshot = this.snapshots.peekLast();
            if (this.currentSnapshot == null) {
                this.currentSnapshot = this.snapshot;
            }
        }

        return this;
    }

    @Override
    public DatabaseImpl commit() {
        if (isOpen()) {
            Snapshot transaction;
            while (Objects.nonNull(transaction = this.snapshots.poll())) {
                this.snapshot.merge(transaction);
            }
            this.currentSnapshot = snapshot;
        }

        return this;
    }

    private boolean isOpen() {
        if (this.snapshots.isEmpty()) {
            Logger.getGlobal().info(NO_TRANSACTION);
            return false;
        }
        return true;
    }

    @Override
    public DatabaseImpl set(String key, String value) {
        this.currentSnapshot.set(key, value);

        return this;
    }

    @Override
    public String get(String key) {
        return this.currentSnapshot.get(key);
    }

    @Override
    public DatabaseImpl delete(String key) {
        this.currentSnapshot.delete(key);

        return this;
    }

    @Override
    public long count(String value) {
        long count = this.snapshot.count(value);
        count += this.snapshots.stream()
                .mapToLong(s -> s.count(value))
                .sum();
        Logger.getGlobal().info(String.valueOf(count));

        return count;
    }

    private Snapshot createSnapshot() {
        return this.snapshotSupplier.get();
    }

}
