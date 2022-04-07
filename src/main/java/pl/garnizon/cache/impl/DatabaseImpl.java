package pl.garnizon.cache.impl;

import pl.garnizon.cache.api.TransactionalDatabase;
import pl.garnizon.cache.snapshot.api.Snapshot;
import pl.garnizon.cache.snapshot.impl.SnapshotImpl;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.logging.Logger;

public class DatabaseImpl<T> implements TransactionalDatabase<T> {

    public static final String NO_TRANSACTION = "NO TRANSACTION";

    private final Supplier<Snapshot<T>> snapshotSupplier;
    private final Snapshot<T> snapshot;
    private final Deque<Snapshot<T>> snapshots;
    private Snapshot<T> currentSnapshot;

    public static <T extends Comparable<T>> DatabaseImpl<T> create() {
        return new DatabaseImpl<>(SnapshotImpl::create, new LinkedList<>());
    }

    DatabaseImpl(Supplier<Snapshot<T>> snapshotSupplier, Deque<Snapshot<T>> snapshots) {
        this.snapshotSupplier = snapshotSupplier;
        this.snapshots = snapshots;

        this.snapshot = createSnapshot();
        this.currentSnapshot = this.snapshot;
    }

    @Override
    public DatabaseImpl<T> begin() {
        Snapshot t = createSnapshot();
        this.snapshots.add(t);
        this.currentSnapshot = t;

        return this;
    }

    @Override
    public DatabaseImpl<T> rollback() {
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
    public DatabaseImpl<T> commit() {
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
    public DatabaseImpl<T> set(String key, T value) {
        this.currentSnapshot.set(key, value);

        return this;
    }

    @Override
    public T get(String key) {
        return this.currentSnapshot.get(key);
    }

    @Override
    public DatabaseImpl<T> delete(String key) {
        this.currentSnapshot.delete(key);

        return this;
    }

    @Override
    public long count(T value) {
        long count = this.snapshot.count(value);
        count += this.snapshots.stream()
                .mapToLong(s -> s.count(value))
                .sum();
        Logger.getGlobal().info(String.valueOf(count));

        return count;
    }

    private Snapshot<T> createSnapshot() {
        return this.snapshotSupplier.get();
    }

}
