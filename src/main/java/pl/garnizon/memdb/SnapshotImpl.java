package pl.garnizon.memdb;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Logger;

public class SnapshotImpl implements Snapshot {

    public static final String TOMBSTONE = "NULL";

    private final Map<String, String> memtable;
    private final Multimap<String, String> invertedIndex;

    public static SnapshotImpl create() {
        return new SnapshotImpl(new HashMap<>(), TreeMultimap.create());
    }

    SnapshotImpl(Map<String, String> memtable, Multimap<String, String> invertedIndex) {
        this.memtable = memtable;
        this.invertedIndex = invertedIndex;
    }

    @Override
    public SnapshotImpl set(String key, String value) {
        return updateRevertedIndex(key, memtable.put(key, value), value);
    }

    private SnapshotImpl updateRevertedIndex(String key, String oldValue, String value) {
        if (Objects.nonNull(oldValue)) {
            deleteFromInvertedIndex(key, oldValue, value);
        }
        invertedIndex.put(value, key);

        return this;
    }

    @Override
    public String get(String key) {
        final String value = memtable.getOrDefault(key, TOMBSTONE);
        Logger.getGlobal().info(value);
        return value;
    }

    @Override
    public SnapshotImpl delete(String key) {
        return deleteFromInvertedIndex(key, memtable.put(key, TOMBSTONE), TOMBSTONE);
    }

    private SnapshotImpl deleteFromInvertedIndex(String key, String oldValue, String value) {
        if (Objects.nonNull(oldValue)) {
            invertedIndex.get(oldValue).removeIf(k -> k.equals(key));
        }
        invertedIndex.put(value, key);

        return this;
    }

    @Override
    public long count(String value) {
        final Collection<String> keys = invertedIndex.get(value);
        long count = keys.stream()
                .mapToInt(key -> memtable.get(key).equals(TOMBSTONE) ? -1 : 1)
                .sum();

        final Collection<String> tombstones = invertedIndex.get(TOMBSTONE);
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

    private void merge(Map<String, String> memtable) {
        if (Objects.nonNull(memtable)) {
            for (Map.Entry<String, String> entry : memtable.entrySet()) {
                set(entry.getKey(), entry.getValue());
            }
            invertedIndex.get(TOMBSTONE).clear();
        }
    }

    @Override
    public Map<String, String> getSnapshot() {
        return memtable;
    }

}
