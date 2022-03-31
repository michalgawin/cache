package pl.garnizon.memdb;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Logger;

public class SnapshotImpl<T> implements Snapshot<T> {

    public static final Object TOMBSTONE = "NULL";

    public static <T> T getTombstone() {
        return (T) TOMBSTONE;
    }

    private final Map<String, T> memtable;
    private final Multimap<T, String> invertedIndex;

    public static SnapshotImpl create() {
        return new SnapshotImpl(new HashMap<>(), TreeMultimap.create());
    }

    SnapshotImpl(Map<String, T> memtable, Multimap<T, String> invertedIndex) {
        this.memtable = memtable;
        this.invertedIndex = invertedIndex;
    }

    @Override
    public SnapshotImpl set(String key, T value) {
        return updateRevertedIndex(key, memtable.put(key, value), value);
    }

    private SnapshotImpl updateRevertedIndex(String key, T oldValue, T value) {
        if (Objects.nonNull(oldValue)) {
            deleteFromInvertedIndex(key, oldValue, value);
        }
        invertedIndex.put(value, key);

        return this;
    }

    @Override
    public T get(String key) {
        final T value = memtable.getOrDefault(key, getTombstone());
        Logger.getGlobal().info(value.toString());
        return value;
    }

    @Override
    public SnapshotImpl delete(String key) {
        return deleteFromInvertedIndex(key, memtable.put(key, getTombstone()), getTombstone());
    }

    private SnapshotImpl deleteFromInvertedIndex(String key, T oldValue, T value) {
        if (Objects.nonNull(oldValue)) {
            invertedIndex.get(oldValue).removeIf(k -> k.equals(key));
        }
        invertedIndex.put(value, key);

        return this;
    }

    @Override
    public long count(T value) {
        final Collection<String> keys = invertedIndex.get(value);
        long count = keys.stream()
                .mapToInt(key -> memtable.get(key).equals(TOMBSTONE) ? -1 : 1)
                .sum();

        final Collection<String> tombstones = invertedIndex.get(getTombstone());
        final long tombstonesFound = tombstones.stream()
                .filter(key -> tombstones.contains(key))
                .count();

        count -= tombstonesFound;

        Logger.getGlobal().info(String.valueOf(count));
        return count;
    }

    @Override
    public void merge(Snapshot transaction) {
        if (Objects.nonNull(transaction)) {
            merge(transaction.getSnapshot());
        }
    }

    private void merge(Map<String, T> memtable) {
        if (Objects.nonNull(memtable)) {
            for (Map.Entry<String, T> entry : memtable.entrySet()) {
                set(entry.getKey(), entry.getValue());
            }
            invertedIndex.get(getTombstone()).clear();
        }
    }

    @Override
    public Map<String, T> getSnapshot() {
        return memtable;
    }

}
