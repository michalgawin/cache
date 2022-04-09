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
    private final Snapshot<T> base;
    private final Deque<Snapshot<T>> snapshots;
    private Snapshot<T> currentSnapshot;

    public static <T extends Comparable<T>> DatabaseImpl<T> create() {
        return new DatabaseImpl<>(SnapshotImpl::create, new LinkedList<>());
    }

    DatabaseImpl(Supplier<Snapshot<T>> snapshotSupplier, Deque<Snapshot<T>> snapshots) {
        this.snapshotSupplier = snapshotSupplier;
        this.snapshots = snapshots;

        this.base = createSnapshot();
        this.currentSnapshot = this.base;
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
                this.currentSnapshot = this.base;
            }
        }

        return this;
    }

    @Override
    public DatabaseImpl<T> commit() {
        if (isOpen()) {
            Snapshot transaction;
            while (Objects.nonNull(transaction = this.snapshots.poll())) {
                this.base.merge(transaction);
            }
            this.currentSnapshot = base;
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
        T t = this.currentSnapshot.get(key);
        return currentSnapshot.isTombstone(t) ? this.base.get(key) : t;
    }

    @Override
    public DatabaseImpl<T> delete(String key) {
        this.currentSnapshot.delete(key);

        return this;
    }

    @Override
    public long count(T value) {
        long count = this.base.count(value);
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
